# app/utils/caching.py
"""
Enhanced caching strategies with memory-mapped file handling, cache invalidation,
warming strategies, and distributed caching support.
"""
import mmap
import csv
import os
import yaml
import pandas as pd
import hashlib
import threading
import time
import pickle
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union, Callable
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import CacheError, ConfigError

logger = get_logger('app.caching')

# Global cache dictionaries for different cache types
_in_memory_cache = {}
_cache_metadata = {}  # Stores cache metadata like TTL, last_updated, etc.
_cache_locks = {}  # Threading locks for cache safety
_cache_warming_status = {}

# Cache statistics
_cache_stats = {
    'hits': 0,
    'misses': 0,
    'evictions': 0,
    'warnings': 0,
    'errors': 0
}

def _load_config():
    """Loads caching configuration from config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    if not os.path.exists(config_path):
        raise ConfigError("config.yaml not found for caching configuration.")
    with open(config_path, 'rt') as f:
        config = yaml.safe_load(f)
    if 'caching' not in config or 'file_paths' not in config:
        raise ConfigError("Caching or file_paths section missing in config.yaml.")
    return config

CONFIG = _load_config()

class CacheEntry:
    """Represents a single cache entry with metadata."""
    def __init__(self, value: Any, ttl_seconds: Optional[int] = None, 
                 last_modified: Optional[datetime] = None):
        self.value = value
        self.created_at = datetime.now()
        self.last_accessed = self.created_at
        self.access_count = 0
        self.ttl_seconds = ttl_seconds
        self.expires_at = None
        self.last_modified = last_modified or self.created_at
        
        if ttl_seconds:
            self.expires_at = self.created_at + timedelta(seconds=ttl_seconds)
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at
    
    def touch(self):
        """Update last accessed time and increment access count."""
        self.last_accessed = datetime.now()
        self.access_count += 1
    
    def is_stale(self, file_path: str) -> bool:
        """Check if cache is stale compared to source file."""
        if not os.path.exists(file_path):
            return True
        
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        return file_mtime > self.last_modified

class DistributedCacheInterface:
    """Interface for distributed caching backends (Redis, Memcached, etc.)"""
    
    def __init__(self):
        self.enabled = CONFIG.get('caching', {}).get('distributed_cache', {}).get('enabled', False)
        self.backend = CONFIG.get('caching', {}).get('distributed_cache', {}).get('backend', 'redis')
        self.connection = None
        
        if self.enabled:
            self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize connection to distributed cache backend."""
        try:
            if self.backend == 'redis':
                import redis
                redis_config = CONFIG.get('caching', {}).get('distributed_cache', {}).get('redis', {})
                self.connection = redis.Redis(
                    host=redis_config.get('host', 'localhost'),
                    port=redis_config.get('port', 6379),
                    db=redis_config.get('db', 0),
                    password=redis_config.get('password'),
                    decode_responses=True
                )
                # Test connection
                self.connection.ping()
                logger.info("Redis distributed cache connection established.")
                
            elif self.backend == 'memcached':
                import pymemcache.client.base as memcache
                memcached_config = CONFIG.get('caching', {}).get('distributed_cache', {}).get('memcached', {})
                self.connection = memcache.Client(
                    (memcached_config.get('host', 'localhost'), memcached_config.get('port', 11211))
                )
                logger.info("Memcached distributed cache connection established.")
                
        except Exception as e:
            logger.warning(f"Failed to initialize distributed cache ({self.backend}): {e}")
            self.enabled = False
            self.connection = None
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from distributed cache."""
        if not self.enabled or not self.connection:
            return None
            
        try:
            if self.backend == 'redis':
                value = self.connection.get(key)
                if value:
                    return pickle.loads(value.encode('latin1'))
            elif self.backend == 'memcached':
                value = self.connection.get(key)
                if value:
                    return pickle.loads(value)
        except Exception as e:
            logger.warning(f"Error getting key '{key}' from distributed cache: {e}")
            _cache_stats['errors'] += 1
        
        return None
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """Set value in distributed cache."""
        if not self.enabled or not self.connection:
            return False
            
        try:
            serialized_value = pickle.dumps(value)
            
            if self.backend == 'redis':
                if ttl_seconds:
                    self.connection.setex(key, ttl_seconds, serialized_value.decode('latin1'))
                else:
                    self.connection.set(key, serialized_value.decode('latin1'))
                return True
                
            elif self.backend == 'memcached':
                expire = ttl_seconds if ttl_seconds else 0
                self.connection.set(key, serialized_value, expire=expire)
                return True
                
        except Exception as e:
            logger.warning(f"Error setting key '{key}' in distributed cache: {e}")
            _cache_stats['errors'] += 1
        
        return False
    
    def delete(self, key: str) -> bool:
        """Delete key from distributed cache."""
        if not self.enabled or not self.connection:
            return False
            
        try:
            if self.backend == 'redis':
                return bool(self.connection.delete(key))
            elif self.backend == 'memcached':
                return bool(self.connection.delete(key))
        except Exception as e:
            logger.warning(f"Error deleting key '{key}' from distributed cache: {e}")
            _cache_stats['errors'] += 1
        
        return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate keys matching a pattern (Redis only)."""
        if not self.enabled or not self.connection or self.backend != 'redis':
            return 0
            
        try:
            keys = self.connection.keys(pattern)
            if keys:
                return self.connection.delete(*keys)
        except Exception as e:
            logger.warning(f"Error invalidating pattern '{pattern}' from distributed cache: {e}")
            _cache_stats['errors'] += 1
        
        return 0

# Initialize distributed cache
distributed_cache = DistributedCacheInterface()

class OptimizedMemoryMappedFile:
    """Optimized memory-mapped file handler with better performance and safety."""
    
    def __init__(self, file_path: str, mode: str = 'r'):
        self.file_path = Path(file_path)
        self.mode = mode
        self.file_obj = None
        self.mmap_obj = None
        self.file_size = 0
        self.last_modified = None
        
    def __enter__(self):
        """Context manager entry."""
        try:
            if not self.file_path.exists():
                raise FileNotFoundError(f"File not found: {self.file_path}")
            
            # Get file stats
            stat = self.file_path.stat()
            self.file_size = stat.st_size
            self.last_modified = datetime.fromtimestamp(stat.st_mtime)
            
            # Open file and create memory map
            if self.mode == 'r':
                self.file_obj = open(self.file_path, 'rb')
                if self.file_size > 0:  # Only create mmap for non-empty files
                    self.mmap_obj = mmap.mmap(self.file_obj.fileno(), 0, access=mmap.ACCESS_READ)
            elif self.mode == 'w':
                self.file_obj = open(self.file_path, 'r+b')
                if self.file_size > 0:
                    self.mmap_obj = mmap.mmap(self.file_obj.fileno(), 0)
            
            return self
            
        except Exception as e:
            self._cleanup()
            raise CacheError(f"Failed to initialize memory-mapped file {self.file_path}: {e}")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self._cleanup()
    
    def _cleanup(self):
        """Clean up file handles and memory maps."""
        if self.mmap_obj:
            try:
                self.mmap_obj.close()
            except Exception as e:
                logger.warning(f"Error closing memory map: {e}")
            finally:
                self.mmap_obj = None
        
        if self.file_obj:
            try:
                self.file_obj.close()
            except Exception as e:
                logger.warning(f"Error closing file: {e}")
            finally:
                self.file_obj = None
    
    def read_chunk(self, start: int, size: int) -> bytes:
        """Read a chunk of data from the memory-mapped file."""
        if not self.mmap_obj:
            raise CacheError("Memory map not initialized")
        
        if start + size > self.file_size:
            size = self.file_size - start
        
        return self.mmap_obj[start:start + size]
    
    def search_pattern(self, pattern: bytes, start: int = 0) -> int:
        """Search for a pattern in the memory-mapped file."""
        if not self.mmap_obj:
            raise CacheError("Memory map not initialized")
        
        return self.mmap_obj.find(pattern, start)
    
    def get_lines(self, max_lines: Optional[int] = None) -> List[str]:
        """Get lines from the memory-mapped file with optional limit."""
        if not self.mmap_obj:
            raise CacheError("Memory map not initialized")
        
        lines = []
        line_count = 0
        current_pos = 0
        
        while current_pos < self.file_size and (max_lines is None or line_count < max_lines):
            # Find next newline
            newline_pos = self.mmap_obj.find(b'\n', current_pos)
            if newline_pos == -1:
                # Last line without newline
                if current_pos < self.file_size:
                    line = self.mmap_obj[current_pos:].decode('utf-8', errors='replace')
                    lines.append(line.rstrip('\r'))
                break
            
            line = self.mmap_obj[current_pos:newline_pos].decode('utf-8', errors='replace')
            lines.append(line.rstrip('\r'))
            current_pos = newline_pos + 1
            line_count += 1
        
        return lines

class EnhancedInMemoryCache:
    """Enhanced in-memory cache with TTL, LRU eviction, and performance monitoring."""
    
    def __init__(self, max_size: int = 10000, default_ttl: int = 3600):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.cache: Dict[str, CacheEntry] = {}
        self.access_order: List[str] = []  # For LRU tracking
        self.lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get an item from cache with LRU update."""
        with self.lock:
            if key not in self.cache:
                _cache_stats['misses'] += 1
                return None
            
            entry = self.cache[key]
            
            if entry.is_expired():
                del self.cache[key]
                if key in self.access_order:
                    self.access_order.remove(key)
                _cache_stats['evictions'] += 1
                _cache_stats['misses'] += 1
                return None
            
            # Update LRU order
            if key in self.access_order:
                self.access_order.remove(key)
            self.access_order.append(key)
            
            entry.touch()
            _cache_stats['hits'] += 1
            return entry.value
    
    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None):
        """Set an item in cache with TTL and LRU eviction."""
        with self.lock:
            ttl = ttl_seconds or self.default_ttl
            entry = CacheEntry(value, ttl_seconds=ttl)
            
            # If key exists, remove from old position
            if key in self.cache:
                if key in self.access_order:
                    self.access_order.remove(key)
            
            # Add to cache and access order
            self.cache[key] = entry
            self.access_order.append(key)
            
            # Evict if over max size (LRU)
            while len(self.cache) > self.max_size:
                if self.access_order:
                    oldest_key = self.access_order.pop(0)
                    if oldest_key in self.cache:
                        del self.cache[oldest_key]
                        _cache_stats['evictions'] += 1
                else:
                    break
    
    def delete(self, key: str) -> bool:
        """Delete an item from cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                if key in self.access_order:
                    self.access_order.remove(key)
                return True
            return False
    
    def clear(self):
        """Clear all items from cache."""
        with self.lock:
            evicted_count = len(self.cache)
            self.cache.clear()
            self.access_order.clear()
            _cache_stats['evictions'] += evicted_count
    
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count of removed items."""
        with self.lock:
            expired_keys = []
            for key, entry in self.cache.items():
                if entry.is_expired():
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self.cache[key]
                if key in self.access_order:
                    self.access_order.remove(key)
            
            _cache_stats['evictions'] += len(expired_keys)
            return len(expired_keys)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'access_order_length': len(self.access_order)
            }
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """Estimate memory usage of cache."""
        import sys
        with self.lock:
            total_size = 0
            for key, entry in self.cache.items():
                total_size += sys.getsizeof(key)
                total_size += sys.getsizeof(entry.value)
                total_size += sys.getsizeof(entry)
            
            return {
                'estimated_bytes': total_size,
                'estimated_mb': round(total_size / (1024 * 1024), 2),
                'entries': len(self.cache)
            }

# Global enhanced cache instance
enhanced_cache = EnhancedInMemoryCache()

def get_from_in_memory_cache(key: str) -> Optional[Any]:
    """Gets an item from the enhanced in-memory cache."""
    return enhanced_cache.get(key)

def set_in_memory_cache(key: str, value: Any, ttl: Optional[int] = None):
    """Sets an item in the enhanced in-memory cache with optional TTL."""
    enhanced_cache.set(key, value, ttl_seconds=ttl)
    logger.debug(f"Set cache for key '{key}' with TTL {ttl}")

def clear_in_memory_cache(key: Optional[str] = None):
    """Clears a specific key or the entire in-memory cache."""
    if key:
        if enhanced_cache.delete(key):
            logger.info(f"Cleared cache for key '{key}'.")
    else:
        enhanced_cache.clear()
        logger.info("Cleared all in-memory cache.")

def invalidate_cache_pattern(pattern: str):
    """Invalidate cache entries matching a pattern."""
    keys_to_remove = []
    for key in enhanced_cache.cache.keys():
        if pattern in key:  # Simple pattern matching
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        enhanced_cache.delete(key)
    
    # Also invalidate in distributed cache
    distributed_cache.invalidate_pattern(pattern)
    
    logger.info(f"Invalidated {len(keys_to_remove)} cache entries matching pattern '{pattern}'")

class EnhancedRVUCache:
    """
    Enhanced RVU cache with optimized memory-mapped file handling,
    cache warming, invalidation strategies, and performance monitoring.
    """
    
    def __init__(self):
        self.cache_type = CONFIG.get('caching', {}).get('rvu_cache_type', 'in_memory').lower()
        self.rvu_data_path = CONFIG.get('file_paths', {}).get('rvu_data_csv')
        self.enable_warming = CONFIG.get('caching', {}).get('enable_cache_warming', True)
        self.warming_batch_size = CONFIG.get('caching', {}).get('warming_batch_size', 1000)
        self.cache_ttl = CONFIG.get('caching', {}).get('rvu_cache_ttl_seconds', 3600)  # 1 hour default
        
        self._rvu_data = None
        self._cache_key = "rvu_data"
        self._warming_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="RVUCacheWarming")
        self._warming_in_progress = False
        self._last_file_check = None
        self._file_check_interval = timedelta(minutes=5)  # Check file changes every 5 minutes
        
        # Initialize lock for thread safety
        if self._cache_key not in _cache_locks:
            _cache_locks[self._cache_key] = threading.RLock()
        
        if not self.rvu_data_path:
            msg = "RVU data CSV path (file_paths.rvu_data_csv) not configured in config.yaml."
            logger.error(msg)
            raise ConfigError(msg)

        if not os.path.exists(self.rvu_data_path):
            msg = f"RVU data file not found at path: {self.rvu_data_path}"
            logger.error(msg)
            raise FileNotFoundError(msg)
        
        self._load_rvu_data()
        
        if self.enable_warming:
            self._start_background_warming()

    def _get_file_hash(self, file_path: str) -> str:
        """Generate hash of file for change detection."""
        try:
            stat = os.stat(file_path)
            # Use file size and mtime for quick change detection
            content = f"{stat.st_size}_{stat.st_mtime}".encode()
            return hashlib.md5(content).hexdigest()
        except Exception as e:
            logger.warning(f"Error generating file hash for {file_path}: {e}")
            return str(time.time())

    def _should_check_file_changes(self) -> bool:
        """Determine if we should check for file changes."""
        if self._last_file_check is None:
            return True
        return datetime.now() - self._last_file_check > self._file_check_interval

    def _load_rvu_data(self):
        """Loads RVU data based on the configured cache type with optimized strategies."""
        logger.info(f"Loading RVU data using cache type: {self.cache_type} from {self.rvu_data_path}")
        
        with _cache_locks[self._cache_key]:
            try:
                # Check distributed cache first
                cache_entry = None
                file_hash = self._get_file_hash(self.rvu_data_path)
                cache_key = f"rvu_data_{file_hash}"
                
                # Try to get from distributed cache
                distributed_data = distributed_cache.get(cache_key)
                if distributed_data:
                    logger.info("RVU data loaded from distributed cache.")
                    self._rvu_data = distributed_data
                    _cache_stats['hits'] += 1
                    return
                
                # Check local cache
                if cache_key in _in_memory_cache:
                    cache_entry = _in_memory_cache[cache_key]
                    if not cache_entry.is_expired() and not cache_entry.is_stale(self.rvu_data_path):
                        logger.info("RVU data loaded from local cache.")
                        cache_entry.touch()
                        self._rvu_data = cache_entry.value
                        _cache_stats['hits'] += 1
                        return
                    else:
                        # Remove stale/expired entry
                        del _in_memory_cache[cache_key]
                        _cache_stats['evictions'] += 1

                _cache_stats['misses'] += 1
                
                # Load from file
                start_time = time.perf_counter()
                
                if self.cache_type == 'memory_mapped':
                    self._rvu_data = self._load_with_memory_mapping()
                elif self.cache_type == 'in_memory':
                    self._rvu_data = self._load_with_pandas()
                else:
                    raise ConfigError(f"Unsupported RVU cache type: {self.cache_type}")
                
                load_time = time.perf_counter() - start_time
                
                if 'cpt_code' not in self._rvu_data.columns:
                    raise CacheError("RVU data CSV must contain a 'cpt_code' column for lookup.")
                
                # Set index for fast lookups
                if not isinstance(self._rvu_data.index, pd.Index) or self._rvu_data.index.name != 'cpt_code':
                    self._rvu_data.set_index('cpt_code', inplace=True)
                
                logger.info(f"RVU data loaded from file in {load_time:.3f}s with {len(self._rvu_data)} entries.")
                
                # Cache the loaded data
                cache_entry = CacheEntry(
                    value=self._rvu_data.copy(),  # Store a copy to avoid modifications
                    ttl_seconds=self.cache_ttl,
                    last_modified=datetime.fromtimestamp(os.path.getmtime(self.rvu_data_path))
                )
                
                _in_memory_cache[cache_key] = cache_entry
                
                # Store in distributed cache
                distributed_cache.set(cache_key, self._rvu_data, ttl_seconds=self.cache_ttl)
                
                # Clean up old cache entries
                self._cleanup_old_cache_entries()
                
            except FileNotFoundError:
                logger.error(f"RVU data file not found: {self.rvu_data_path}")
                raise
            except pd.errors.EmptyDataError:
                logger.error(f"RVU data file is empty: {self.rvu_data_path}")
                raise CacheError(f"RVU data file is empty: {self.rvu_data_path}")
            except Exception as e:
                logger.error(f"Error loading RVU data from {self.rvu_data_path}: {e}", exc_info=True)
                raise CacheError(f"Failed to load RVU data: {e}")

    def _load_with_pandas(self) -> pd.DataFrame:
        """Load RVU data using pandas with optimizations."""
        # Use optimal data types to reduce memory usage
        dtype_map = {
            'cpt_code': 'category',  # CPT codes are categorical
            'description': 'string'
        }
        
        # Try to infer numeric columns
        try:
            sample_df = pd.read_csv(self.rvu_data_path, nrows=100)
            for col in sample_df.columns:
                if col not in dtype_map:
                    if pd.api.types.is_numeric_dtype(sample_df[col]):
                        # Use smallest numeric type that fits the data
                        if sample_df[col].dtype == 'int64':
                            max_val = sample_df[col].max()
                            if max_val < 32767:
                                dtype_map[col] = 'int16'
                            elif max_val < 2147483647:
                                dtype_map[col] = 'int32'
                            else:
                                dtype_map[col] = 'int64'
                        elif sample_df[col].dtype == 'float64':
                            dtype_map[col] = 'float32'  # RVU values don't need double precision
        except Exception as e:
            logger.warning(f"Could not optimize data types: {e}")
            dtype_map = None
        
        return pd.read_csv(self.rvu_data_path, dtype=dtype_map, engine='c')

    def _load_with_memory_mapping(self) -> pd.DataFrame:
        """Load RVU data using memory mapping for large files."""
        with OptimizedMemoryMappedFile(self.rvu_data_path, 'r') as mmf:
            # For CSV files, we still need to parse the content
            # Memory mapping is most beneficial for direct data access patterns
            # For CSV parsing, we'll use pandas with memory_map parameter
            try:
                # Use pandas with memory mapping
                df = pd.read_csv(self.rvu_data_path, memory_map=True, engine='c')
                logger.info(f"Loaded {len(df)} RVU records using memory mapping")
                return df
            except Exception as e:
                logger.warning(f"Memory mapping failed, falling back to regular loading: {e}")
                return self._load_with_pandas()

    def _cleanup_old_cache_entries(self):
        """Remove expired or old cache entries to prevent memory leaks."""
        current_time = datetime.now()
        keys_to_remove = []
        
        for key, entry in _in_memory_cache.items():
            if key.startswith("rvu_data_") and (entry.is_expired() or 
                current_time - entry.created_at > timedelta(hours=24)):
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del _in_memory_cache[key]
            _cache_stats['evictions'] += 1
        
        if keys_to_remove:
            logger.info(f"Cleaned up {len(keys_to_remove)} old RVU cache entries")

    def _start_background_warming(self):
        """Start background cache warming process."""
        def warm_cache():
            """Background task to warm cache with frequently accessed data."""
            try:
                if self._warming_in_progress:
                    return
                
                self._warming_in_progress = True
                _cache_warming_status['rvu_cache'] = 'warming'
                
                logger.info("Starting RVU cache warming...")
                
                # Simulate warming most common CPT codes
                # In production, this could be based on historical access patterns
                common_cpt_codes = self._get_common_cpt_codes()
                
                warmed_count = 0
                for cpt_code in common_cpt_codes[:self.warming_batch_size]:
                    try:
                        self.get_rvu_details(cpt_code)
                        warmed_count += 1
                    except Exception as e:
                        logger.warning(f"Error warming cache for CPT {cpt_code}: {e}")
                
                _cache_warming_status['rvu_cache'] = 'completed'
                logger.info(f"RVU cache warming completed. Warmed {warmed_count} entries.")
                
            except Exception as e:
                _cache_warming_status['rvu_cache'] = 'failed'
                logger.error(f"RVU cache warming failed: {e}", exc_info=True)
            finally:
                self._warming_in_progress = False
        
        # Start warming in background
        self._warming_executor.submit(warm_cache)

    def _get_common_cpt_codes(self) -> List[str]:
        """Get list of commonly used CPT codes for cache warming."""
        if self._rvu_data is None:
            return []
        
        # Return all CPT codes, most common ones first based on RVU value
        # In production, this could be based on actual usage statistics
        try:
            if 'rvu_value' in self._rvu_data.columns:
                return self._rvu_data.nlargest(self.warming_batch_size, 'rvu_value').index.tolist()
            else:
                return self._rvu_data.index.tolist()[:self.warming_batch_size]
        except Exception as e:
            logger.warning(f"Error getting common CPT codes: {e}")
            return self._rvu_data.index.tolist()[:self.warming_batch_size] if self._rvu_data is not None else []

    def get_rvu_details(self, cpt_code: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves RVU details for a given CPT code with caching and performance monitoring.
        """
        if self._rvu_data is None:
            logger.warning("RVU data not loaded. Cannot retrieve details.")
            _cache_stats['warnings'] += 1
            return None
        
        # Check for file changes periodically
        if self._should_check_file_changes():
            self._last_file_check = datetime.now()
            if self._file_changed():
                logger.info("RVU file changed, refreshing cache...")
                self.refresh_cache()
        
        try:
            start_time = time.perf_counter()
            
            # Try distributed cache first for this specific CPT
            cache_key = f"rvu_detail_{cpt_code}"
            cached_result = distributed_cache.get(cache_key)
            
            if cached_result:
                _cache_stats['hits'] += 1
                return cached_result
            
            # Look up in main data
            if cpt_code in self._rvu_data.index:
                rvu_details = self._rvu_data.loc[cpt_code].to_dict()
                
                # Cache the result
                distributed_cache.set(cache_key, rvu_details, ttl_seconds=self.cache_ttl)
                
                lookup_time = time.perf_counter() - start_time
                logger.debug(f"RVU details found for CPT {cpt_code} in {lookup_time:.4f}s: {rvu_details}")
                
                _cache_stats['hits'] += 1
                return rvu_details
            else:
                logger.debug(f"RVU details not found for CPT {cpt_code}.")
                _cache_stats['misses'] += 1
                return None
                
        except KeyError:
            logger.debug(f"RVU details (KeyError) not found for CPT {cpt_code}.")
            _cache_stats['misses'] += 1
            return None
        except Exception as e:
            logger.error(f"Error retrieving RVU for CPT {cpt_code}: {e}", exc_info=True)
            _cache_stats['errors'] += 1
            return None

    def _file_changed(self) -> bool:
        """Check if the source file has changed."""
        try:
            current_hash = self._get_file_hash(self.rvu_data_path)
            # Get the current cache key (assumes latest hash is in use)
            for key in _in_memory_cache.keys():
                if key.startswith("rvu_data_") and key.endswith(current_hash):
                    return False
            return True
        except Exception as e:
            logger.warning(f"Error checking file changes: {e}")
            return False

    def refresh_cache(self, force: bool = False):
        """Reloads the RVU data from the source file with cache invalidation."""
        logger.info("Refreshing RVU cache...")
        
        with _cache_locks[self._cache_key]:
            try:
                # Invalidate distributed cache
                pattern = "rvu_*"
                invalidated_count = distributed_cache.invalidate_pattern(pattern)
                logger.info(f"Invalidated {invalidated_count} distributed cache entries")
                
                # Clear local cache entries
                keys_to_remove = [k for k in _in_memory_cache.keys() if k.startswith("rvu_")]
                for key in keys_to_remove:
                    del _in_memory_cache[key]
                    _cache_stats['evictions'] += 1
                
                # Reload data
                self._load_rvu_data()
                
                # Restart warming if enabled
                if self.enable_warming:
                    self._start_background_warming()
                
                logger.info("RVU cache refreshed successfully.")
                
            except Exception as e:
                logger.error(f"Failed to refresh RVU cache: {e}", exc_info=True)
                raise CacheError(f"Failed to refresh RVU cache: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        total_requests = _cache_stats['hits'] + _cache_stats['misses']
        hit_rate = (_cache_stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'hit_rate_percent': round(hit_rate, 2),
            'total_requests': total_requests,
            'cache_stats': _cache_stats.copy(),
            'warming_status': _cache_warming_status.copy(),
            'cache_entries': len(_in_memory_cache),
            'distributed_cache_enabled': distributed_cache.enabled
        }

    def shutdown(self):
        """Shutdown cache and cleanup resources."""
        try:
            self._warming_executor.shutdown(wait=True)
            logger.info("RVU cache shutdown completed.")
        except Exception as e:
            logger.warning(f"Error during RVU cache shutdown: {e}")

# --- Cache Warming and Monitoring ---

def warm_startup_cache():
    """Warm critical caches on application startup."""
    cid = get_correlation_id()
    logger.info(f"[{cid}] Starting cache warming on application startup...")
    
    warming_tasks = []
    
    # Warm RVU cache
    if rvu_cache_instance and rvu_cache_instance.enable_warming:
        warming_tasks.append(("RVU Cache", lambda: rvu_cache_instance._start_background_warming()))
    
    for task_name, task_func in warming_tasks:
        try:
            task_func()
            logger.info(f"[{cid}] {task_name} warming initiated successfully")
        except Exception as e:
            logger.warning(f"[{cid}] Failed to warm {task_name}: {e}")

async def async_cache_warming():
    """Asynchronous cache warming for better performance."""
    logger.info("Starting asynchronous cache warming...")
    
    async def warm_rvu_cache():
        """Async RVU cache warming."""
        if rvu_cache_instance:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, rvu_cache_instance._start_background_warming)
    
    warming_tasks = [warm_rvu_cache()]
    
    await asyncio.gather(*warming_tasks, return_exceptions=True)
    logger.info("Asynchronous cache warming completed")

def start_cache_maintenance():
    """Start background cache maintenance tasks."""
    def maintenance_worker():
        """Background worker for cache maintenance."""
        while True:
            try:
                # Clean up expired entries
                expired_count = enhanced_cache.cleanup_expired()
                if expired_count > 0:
                    logger.debug(f"Cleaned up {expired_count} expired cache entries")
                
                # Log cache statistics periodically
                stats = get_cache_statistics()
                logger.debug(f"Cache statistics: {stats}")
                
                # Sleep for maintenance interval
                time.sleep(CONFIG.get('caching', {}).get('maintenance_interval_seconds', 300))  # 5 minutes default
                
            except Exception as e:
                logger.error(f"Error in cache maintenance worker: {e}", exc_info=True)
                time.sleep(60)  # Wait before retrying
    
    # Start maintenance thread
    maintenance_thread = threading.Thread(target=maintenance_worker, daemon=True, name="CacheMaintenance")
    maintenance_thread.start()
    logger.info("Cache maintenance worker started")

def get_cache_statistics() -> Dict[str, Any]:
    """Get comprehensive cache statistics."""
    stats = {
        'global_stats': _cache_stats.copy(),
        'enhanced_cache': enhanced_cache.get_stats(),
        'memory_usage': enhanced_cache.get_memory_usage(),
        'distributed_cache': {
            'enabled': distributed_cache.enabled,
            'backend': distributed_cache.backend if distributed_cache.enabled else None
        },
        'warming_status': _cache_warming_status.copy()
    }
    
    # Add RVU cache stats if available
    if rvu_cache_instance:
        stats['rvu_cache'] = rvu_cache_instance.get_cache_stats()
    
    # Calculate derived statistics
    total_requests = stats['global_stats']['hits'] + stats['global_stats']['misses']
    if total_requests > 0:
        stats['derived_stats'] = {
            'hit_rate_percent': round((stats['global_stats']['hits'] / total_requests) * 100, 2),
            'miss_rate_percent': round((stats['global_stats']['misses'] / total_requests) * 100, 2),
            'eviction_rate_percent': round((stats['global_stats']['evictions'] / total_requests) * 100, 2)
        }
    else:
        stats['derived_stats'] = {
            'hit_rate_percent': 0.0,
            'miss_rate_percent': 0.0,
            'eviction_rate_percent': 0.0
        }
    
    return stats

def perform_cache_health_check() -> Dict[str, Any]:
    """Perform comprehensive cache health check."""
    health_status = {
        'overall_status': 'healthy',
        'checks': {},
        'warnings': [],
        'errors': [],
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        # Check enhanced cache
        test_key = f"health_check_{int(time.time())}"
        test_value = {"health_check": True, "timestamp": time.time()}
        
        # Test write
        enhanced_cache.set(test_key, test_value, ttl_seconds=60)
        
        # Test read
        retrieved_value = enhanced_cache.get(test_key)
        
        if retrieved_value == test_value:
            health_status['checks']['enhanced_cache'] = 'pass'
        else:
            health_status['checks']['enhanced_cache'] = 'fail'
            health_status['errors'].append("Enhanced cache read/write test failed")
        
        # Test delete
        if enhanced_cache.delete(test_key):
            health_status['checks']['enhanced_cache_delete'] = 'pass'
        else:
            health_status['checks']['enhanced_cache_delete'] = 'fail'
            health_status['warnings'].append("Enhanced cache delete test failed")
        
        # Check distributed cache
        if distributed_cache.enabled:
            success = distributed_cache.set(test_key, test_value, ttl_seconds=60)
            if success:
                retrieved = distributed_cache.get(test_key)
                if retrieved and retrieved.get('health_check'):
                    health_status['checks']['distributed_cache'] = 'pass'
                else:
                    health_status['checks']['distributed_cache'] = 'fail'
                    health_status['errors'].append("Distributed cache read test failed")
                distributed_cache.delete(test_key)
            else:
                health_status['checks']['distributed_cache'] = 'fail'
                health_status['errors'].append("Distributed cache write test failed")
        else:
            health_status['checks']['distributed_cache'] = 'disabled'
        
        # Check RVU cache
        if rvu_cache_instance:
            try:
                test_cpt = "99213"  # Common CPT code
                result = rvu_cache_instance.get_rvu_details(test_cpt)
                health_status['checks']['rvu_cache'] = 'pass'
            except Exception as e:
                health_status['checks']['rvu_cache'] = 'fail'
                health_status['errors'].append(f"RVU cache test failed: {e}")
        else:
            health_status['checks']['rvu_cache'] = 'not_initialized'
            health_status['warnings'].append("RVU cache not initialized")
        
        # Check cache statistics for anomalies
        stats = get_cache_statistics()
        
        # Memory usage check
        memory_mb = stats['memory_usage']['estimated_mb']
        if memory_mb > 500:  # 500MB threshold
            health_status['warnings'].append(f"High memory usage: {memory_mb} MB")
        
        # Hit rate check
        hit_rate = stats['derived_stats']['hit_rate_percent']
        if hit_rate < 50 and stats['global_stats']['hits'] + stats['global_stats']['misses'] > 100:
            health_status['warnings'].append(f"Low cache hit rate: {hit_rate}%")
        
        # Set overall status
        if health_status['errors']:
            health_status['overall_status'] = 'unhealthy'
        elif health_status['warnings']:
            health_status['overall_status'] = 'degraded'
        
    except Exception as e:
        health_status['overall_status'] = 'unhealthy'
        health_status['errors'].append(f"Health check failed: {e}")
    
    return health_status

# Initialize RVU cache instance
try:
    rvu_cache_instance = EnhancedRVUCache()
    logger.info("Enhanced RVU cache initialized successfully")
except (ConfigError, FileNotFoundError, CacheError) as e:
    logger.critical(f"Failed to initialize Enhanced RVU Cache: {e}. RVU lookups will fail.", exc_info=True)
    rvu_cache_instance = None

# Start cache maintenance on module import
start_cache_maintenance()

def shutdown_caches():
    """Shutdown all caches gracefully."""
    logger.info("Shutting down caches...")
    
    if rvu_cache_instance:
        rvu_cache_instance.shutdown()
    
    enhanced_cache.clear()
    
    logger.info("Cache shutdown completed")

if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    import csv
    
    setup_logging()
    set_correlation_id("ENHANCED_CACHE_TEST")
    
    logger.info("--- Enhanced Caching System Test ---")
    
    # Test distributed cache if enabled
    if distributed_cache.enabled:
        logger.info(f"Testing distributed cache ({distributed_cache.backend})...")
        test_success = distributed_cache.set("test_key", {"test": "value"}, ttl_seconds=60)
        if test_success:
            result = distributed_cache.get("test_key")
            logger.info(f"Distributed cache test result: {result}")
            distributed_cache.delete("test_key")
        else:
            logger.warning("Distributed cache test failed")
    
    # Test enhanced in-memory cache
    logger.info("Testing enhanced in-memory cache...")
    set_in_memory_cache("test_key", {"enhanced": "cache"}, ttl=60)
    result = get_from_in_memory_cache("test_key")
    logger.info(f"Enhanced cache test result: {result}")
    
    # Create dummy RVU data if needed
    if rvu_cache_instance:
        dummy_rvu_path = CONFIG.get('file_paths', {}).get('rvu_data_csv', 'data/rvu_data/rvu_table.csv')
        os.makedirs(os.path.dirname(dummy_rvu_path), exist_ok=True)
        
        if not os.path.exists(dummy_rvu_path):
            with open(dummy_rvu_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['cpt_code', 'rvu_value', 'description'])
                writer.writerow(['99213', '1.50', 'Office visit est pt level 3'])
                writer.writerow(['99214', '2.45', 'Office visit est pt level 4'])
                writer.writerow(['G0008', '0.25', 'Admin of flu shot'])
                writer.writerow(['99211', '0.18', 'Office visit est pt level 1'])
                writer.writerow(['99212', '0.93', 'Office visit est pt level 2'])
            
            logger.info(f"Dummy RVU data created at {dummy_rvu_path}")
            
            # Re-initialize RVU cache
            try:
                rvu_cache_instance.refresh_cache()
            except Exception as e:
                logger.error(f"Could not refresh RVU cache: {e}")
        
        # Test RVU cache
        logger.info("Testing Enhanced RVU Cache...")
        test_cpts = ['99213', '99214', 'G0008', 'INVALID_CPT']
        
        for cpt in test_cpts:
            details = rvu_cache_instance.get_rvu_details(cpt)
            logger.info(f"RVU details for {cpt}: {details}")
        
        # Test cache warming
        logger.info("Testing cache warming...")
        warm_startup_cache()
        
        # Display cache statistics
        stats = rvu_cache_instance.get_cache_stats()
        logger.info(f"RVU Cache statistics: {stats}")
    
    # Test cache health check
    logger.info("Performing cache health check...")
    health = perform_cache_health_check()
    logger.info(f"Cache health status: {health}")
    
    # Test cache statistics
    overall_stats = get_cache_statistics()
    logger.info(f"Overall cache statistics: {overall_stats}")
    
    # Test cache invalidation
    logger.info("Testing cache invalidation...")
    invalidate_cache_pattern("test_")
    
    logger.info("Enhanced caching system test completed successfully!")