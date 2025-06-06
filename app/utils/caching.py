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
from app.database.connection_manager import get_sqlserver_session
from app.database.models.sqlserver_models import RvuData

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

    def shutdown(self):
        """Closes the connection to the distributed cache."""
        if self.enabled and self.connection:
            try:
                if self.backend == 'redis':
                    self.connection.close()
                elif self.backend == 'memcached':
                    self.connection.close()
                logger.info(f"Distributed cache connection ({self.backend}) closed.")
            except Exception as e:
                logger.warning(f"Error closing distributed cache connection: {e}")


# Initialize distributed cache
distributed_cache = DistributedCacheInterface()

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

class RvuDatabaseCache:
    """
    Handles caching of RVU data fetched from the SQL Server database.
    It uses a generic in-memory cache to store results of DB queries.
    """
    def __init__(self):
        self.cache_ttl = CONFIG.get('caching', {}).get('rvu_cache_ttl_seconds', 3600)
        logger.info(f"RvuDatabaseCache initialized with TTL: {self.cache_ttl} seconds.")
        self.cache_type = CONFIG.get('caching', {}).get('rvu_cache_type', 'in_memory') # For health check metadata

    def get_rvu_details(self, cpt_code: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves RVU details for a given CPT code, using an in-memory cache
        to avoid redundant database queries.
        """
        cache_key = f"rvu_db_{cpt_code}"
        
        # 1. Check in-memory cache first
        cached_details = get_from_in_memory_cache(cache_key)
        if cached_details:
            logger.debug(f"RVU cache HIT for CPT {cpt_code}")
            return cached_details

        logger.debug(f"RVU cache MISS for CPT {cpt_code}. Querying database.")
        _cache_stats['misses'] += 1

        # 2. On miss, query the database
        sql_session = None
        try:
            sql_session = get_sqlserver_session()
            rvu_record = sql_session.query(RvuData).filter(RvuData.CptCode == cpt_code).first()

            if not rvu_record:
                # Cache the fact that it's not found to prevent re-querying for the same invalid code
                set_in_memory_cache(cache_key, {"found": False}, ttl=self.cache_ttl)
                return None

            # 3. If found, format and store in cache
            rvu_details = {
                "cpt_code": rvu_record.CptCode,
                "description": rvu_record.Description,
                "rvu_value": rvu_record.RvuValue, # Will be a Decimal object
                "found": True
            }
            set_in_memory_cache(cache_key, rvu_details, ttl=self.cache_ttl)
            return rvu_details

        except Exception as e:
            logger.error(f"Error querying RVU data for CPT {cpt_code}: {e}", exc_info=True)
            _cache_stats['errors'] += 1
            return None # Return None on error
        finally:
            if sql_session:
                sql_session.close()

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
    # if rvu_cache_instance:
    #     stats['rvu_cache'] = rvu_cache_instance.get_cache_stats()
    
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

# Initialize RVU cache instance
try:
    rvu_cache_instance = RvuDatabaseCache()
    logger.info("Database-backed RVU cache initialized successfully")
except Exception as e:
    logger.critical(f"Failed to initialize RvuDatabaseCache: {e}. RVU lookups will fail.", exc_info=True)
    rvu_cache_instance = None

def shutdown_caches():
    """Function to gracefully shut down all cache-related resources."""
    logger.info("Shutting down cache systems...")
    distributed_cache.shutdown()
    enhanced_cache.clear()
    logger.info("All cache systems have been shut down.")
