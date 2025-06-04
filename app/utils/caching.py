# app/utils/caching.py
"""
Implements caching strategies, including memory-mapped file caching for RVU data.
"""
import mmap
import csv
import os
import yaml
import pandas as pd
from app.utils.logging_config import get_logger
from app.utils.error_handler import CacheError, ConfigError

logger = get_logger('app.caching')

# Global cache dictionary for in-memory caching (if used)
_in_memory_cache = {}

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

class RVUCache:
    """
    Handles caching of RVU (Relative Value Unit) data.
    Supports memory-mapped file caching or simple in-memory dictionary caching.
    """
    def __init__(self):
        self.cache_type = CONFIG.get('caching', {}).get('rvu_cache_type', 'in_memory').lower()
        self.rvu_data_path = CONFIG.get('file_paths', {}).get('rvu_data_csv')
        self._rvu_data = None  # Holds the mmap object or pandas DataFrame

        if not self.rvu_data_path:
            msg = "RVU data CSV path (file_paths.rvu_data_csv) not configured in config.yaml."
            logger.error(msg)
            raise ConfigError(msg)

        if not os.path.exists(self.rvu_data_path):
            msg = f"RVU data file not found at path: {self.rvu_data_path}"
            logger.error(msg)
            raise FileNotFoundError(msg)
        
        self._load_rvu_data()

    def _load_rvu_data(self):
        """Loads RVU data based on the configured cache type."""
        logger.info(f"Loading RVU data using cache type: {self.cache_type} from {self.rvu_data_path}")
        try:
            if self.cache_type == 'memory_mapped':
                # For memory-mapped files, we typically read it line by line or use a structure.
                # A simple approach for CSVs with mmap is to read it into a more accessible structure like a dict
                # or use pandas with memory_map=True if the library supports it well for this use case.
                # Let's use pandas for easier lookup, assuming the CSV is not excessively large for memory
                # after parsing, even if the file itself is memory-mapped during read.
                # Pandas can utilize memory mapping when reading CSVs with certain engines.
                # For true mmap line-by-line processing, the access pattern would be different.
                # Given the need for quick lookups (e.g., by CPT code), a DataFrame is practical.
                self._rvu_data = pd.read_csv(self.rvu_data_path, memory_map=True)
                # Assuming CPT code is the primary key for lookup
                if 'cpt_code' not in self._rvu_data.columns: # Adjust column name as per your CSV
                    raise CacheError("RVU data CSV must contain a 'cpt_code' column for lookup.")
                self._rvu_data.set_index('cpt_code', inplace=True)
                logger.info(f"RVU data loaded into pandas DataFrame from {self.rvu_data_path} with {len(self._rvu_data)} entries (memory_map=True).")

            elif self.cache_type == 'in_memory':
                self._rvu_data = pd.read_csv(self.rvu_data_path)
                if 'cpt_code' not in self._rvu_data.columns:
                    raise CacheError("RVU data CSV must contain a 'cpt_code' column for lookup.")
                self._rvu_data.set_index('cpt_code', inplace=True)
                logger.info(f"RVU data loaded into in-memory pandas DataFrame from {self.rvu_data_path} with {len(self._rvu_data)} entries.")
            else:
                raise ConfigError(f"Unsupported RVU cache type: {self.cache_type}")
        except FileNotFoundError:
            logger.error(f"RVU data file not found: {self.rvu_data_path}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"RVU data file is empty: {self.rvu_data_path}")
            raise CacheError(f"RVU data file is empty: {self.rvu_data_path}")
        except Exception as e:
            logger.error(f"Error loading RVU data from {self.rvu_data_path}: {e}", exc_info=True)
            raise CacheError(f"Failed to load RVU data: {e}")

    def get_rvu_details(self, cpt_code: str) -> dict:
        """
        Retrieves RVU details for a given CPT code.
        Returns a dictionary of the row if found, else None.
        Assumes 'rvu_value' is a column in your CSV. Adjust as needed.
        """
        if self._rvu_data is None:
            logger.warning("RVU data not loaded. Cannot retrieve details.")
            return None
        
        try:
            if cpt_code in self._rvu_data.index:
                # .loc returns a Series, convert to dict
                rvu_details = self._rvu_data.loc[cpt_code].to_dict()
                logger.debug(f"RVU details found for CPT {cpt_code}: {rvu_details}")
                return rvu_details
            else:
                logger.debug(f"RVU details not found for CPT {cpt_code}.")
                return None
        except KeyError: # Should be caught by `in self._rvu_data.index` but as a safeguard
            logger.debug(f"RVU details (KeyError) not found for CPT {cpt_code}.")
            return None
        except Exception as e:
            logger.error(f"Error retrieving RVU for CPT {cpt_code}: {e}", exc_info=True)
            # Depending on policy, might raise CacheError or return None
            return None 

    def refresh_cache(self):
        """Reloads the RVU data from the source file."""
        logger.info("Refreshing RVU cache...")
        try:
            self._load_rvu_data()
            logger.info("RVU cache refreshed successfully.")
        except Exception as e:
            logger.error(f"Failed to refresh RVU cache: {e}", exc_info=True)
            # Decide on error handling: keep stale cache or raise?
            # For now, log and potentially keep stale data.
            # raise CacheError(f"Failed to refresh RVU cache: {e}")


# --- Generic In-Memory Cache (Example, can be expanded) ---

def get_from_in_memory_cache(key: str):
    """Gets an item from the generic in-memory cache."""
    return _in_memory_cache.get(key)

def set_in_memory_cache(key: str, value, ttl: int = None):
    """
    Sets an item in the generic in-memory cache with an optional TTL (not implemented here).
    For TTL, a more sophisticated cache library (e.g., cachetools) would be better.
    """
    _in_memory_cache[key] = value
    logger.debug(f"Set cache for key '{key}'.")

def clear_in_memory_cache(key: str = None):
    """Clears a specific key or the entire in-memory cache."""
    if key:
        if key in _in_memory_cache:
            del _in_memory_cache[key]
            logger.info(f"Cleared cache for key '{key}'.")
    else:
        _in_memory_cache.clear()
        logger.info("Cleared all in-memory cache.")


# Initialize RVU cache instance (singleton-like pattern for easy access)
# This will load data when the module is first imported.
try:
    rvu_cache_instance = RVUCache()
except (ConfigError, FileNotFoundError, CacheError) as e:
    logger.critical(f"Failed to initialize RVUCache: {e}. RVU lookups will fail.", exc_info=True)
    rvu_cache_instance = None # Ensure it's defined but indicates failure


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    setup_logging()
    set_correlation_id("CACHE_TEST")

    if rvu_cache_instance:
        # Create a dummy rvu_table.csv for testing
        dummy_rvu_path = CONFIG.get('file_paths', {}).get('rvu_data_csv', 'data/rvu_data/rvu_table.csv')
        os.makedirs(os.path.dirname(dummy_rvu_path), exist_ok=True)
        with open(dummy_rvu_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['cpt_code', 'rvu_value', 'description'])
            writer.writerow(['99213', '1.5', 'Office visit est pt level 3'])
            writer.writerow(['99214', '2.5', 'Office visit est pt level 4'])
            writer.writerow(['G0008', '0.5', 'Admin of flu shot'])
        
        logger.info(f"Dummy RVU data written to {dummy_rvu_path}")
        
        # Re-initialize if needed due to file creation after initial load attempt
        try:
            if rvu_cache_instance._rvu_data is None: # If initial load failed due to missing file
                 rvu_cache_instance = RVUCache()
        except Exception as e:
            logger.error(f"Could not re-initialize RVU cache for test: {e}")
            rvu_cache_instance = None


        if rvu_cache_instance and rvu_cache_instance._rvu_data is not None:
            logger.info("--- Testing RVU Cache ---")
            cpt1_details = rvu_cache_instance.get_rvu_details('99213')
            logger.info(f"Details for 99213: {cpt1_details}")
            if cpt1_details:
                logger.info(f"RVU value for 99213: {cpt1_details.get('rvu_value')}")

            cpt2_details = rvu_cache_instance.get_rvu_details('G0008')
            logger.info(f"Details for G0008: {cpt2_details}")

            cpt_nonexistent = rvu_cache_instance.get_rvu_details('XXXXX')
            logger.info(f"Details for XXXXX: {cpt_nonexistent}")

            logger.info("Attempting to refresh RVU cache...")
            rvu_cache_instance.refresh_cache()
            cpt1_details_after_refresh = rvu_cache_instance.get_rvu_details('99213')
            logger.info(f"Details for 99213 after refresh: {cpt1_details_after_refresh}")
        else:
            logger.error("RVU Cache instance or its data is None, cannot run tests.")
            
        # Clean up dummy file
        # os.remove(dummy_rvu_path)
        # logger.info(f"Dummy RVU data file {dummy_rvu_path} removed.")

    else:
        logger.error("RVU Cache instance is None. Cannot run tests.")

    logger.info("--- Testing Generic In-Memory Cache ---")
    set_in_memory_cache("my_key", {"data": "some value"})
    cached_val = get_from_in_memory_cache("my_key")
    logger.info(f"Cached value for 'my_key': {cached_val}")
    clear_in_memory_cache("my_key")
    cached_val_after_clear = get_from_in_memory_cache("my_key")
    logger.info(f"Cached value for 'my_key' after clear: {cached_val_after_clear}")