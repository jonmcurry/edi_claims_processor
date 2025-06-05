# app/utils/platform_config.py
"""
Cross-platform configuration and event loop setup.
Handles Windows/Linux differences for optimal performance.
"""
import sys
import asyncio
import os
from app.utils.logging_config import get_logger

logger = get_logger('app.platform_config')

def setup_optimal_event_loop():
    """
    Configures the optimal event loop for the current platform.
    - Linux/macOS: Uses uvloop if available
    - Windows: Uses winloop if available, falls back to ProactorEventLoop
    """
    if sys.platform == "win32":
        # Windows-specific event loop setup
        logger.info("Setting up event loop for Windows platform")
        
        # Try winloop first (high-performance Windows event loop)
        try:
            import winloop
            asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
            logger.info("Using winloop event loop policy for Windows")
            return "winloop"
        except ImportError:
            logger.info("winloop not available, using Windows ProactorEventLoop")
            # Use ProactorEventLoop for better I/O performance on Windows
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            return "proactor"
    
    else:
        # Unix-like systems (Linux, macOS)
        logger.info(f"Setting up event loop for {sys.platform} platform")
        
        # Try uvloop first (high-performance Unix event loop)
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("Using uvloop event loop policy for Unix")
            return "uvloop"
        except ImportError:
            logger.info("uvloop not available, using default asyncio event loop")
            return "default"

def get_optimal_worker_count():
    """
    Returns the optimal number of workers based on platform and CPU count.
    """
    cpu_count = os.cpu_count() or 1
    
    if sys.platform == "win32":
        # Windows: More conservative with process workers due to overhead
        # Use ThreadPoolExecutor for I/O bound tasks
        return min(cpu_count, 8)
    else:
        # Unix: Can handle more process workers efficiently
        return min(cpu_count * 2, 16)

def get_platform_specific_config():
    """
    Returns platform-specific configuration adjustments.
    """
    config = {
        "event_loop": setup_optimal_event_loop(),
        "max_workers": get_optimal_worker_count(),
        "platform": sys.platform,
        "supports_fork": hasattr(os, 'fork'),
        "preferred_executor": "thread" if sys.platform == "win32" else "process"
    }
    
    # Platform-specific database settings
    if sys.platform == "win32":
        config.update({
            "db_pool_pre_ping": True,  # More important on Windows
            "db_pool_recycle": 1800,   # More frequent recycling
            "file_separator": "\\",
            "use_multiprocessing": False  # Prefer threading on Windows
        })
    else:
        config.update({
            "db_pool_pre_ping": True,
            "db_pool_recycle": 3600,   # Less frequent recycling on Unix
            "file_separator": "/",
            "use_multiprocessing": True   # Can use multiprocessing efficiently
        })
    
    logger.info(f"Platform configuration: {config}")
    return config

# Global platform configuration
PLATFORM_CONFIG = get_platform_specific_config()

def configure_multiprocessing():
    """
    Configures multiprocessing for cross-platform compatibility.
    """
    if sys.platform == "win32":
        import multiprocessing
        # Required for Windows when using multiprocessing
        multiprocessing.freeze_support()
        
        # Set start method to 'spawn' for Windows compatibility
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            # Already set
            pass
    
    logger.info("Multiprocessing configured for platform compatibility")

# Updated app/api/main.py startup section
"""
Add this to your app/api/main.py before creating the FastAPI app:

from app.utils.platform_config import setup_optimal_event_loop, PLATFORM_CONFIG

# Setup optimal event loop for the platform
event_loop_type = setup_optimal_event_loop()
logger.info(f"Configured {event_loop_type} event loop for platform {PLATFORM_CONFIG['platform']}")
"""

# Updated app/main.py startup section
"""
Add this to your app/main.py at the beginning of main():

from app.utils.platform_config import configure_multiprocessing, PLATFORM_CONFIG

def main():
    # Configure platform-specific settings
    configure_multiprocessing()
    
    overall_cid = set_correlation_id("EDI_PROC_APP_RUN") 
    logger.info(f"[{overall_cid}] EDI Claims Processor starting on {PLATFORM_CONFIG['platform']}")
    logger.info(f"Platform config: {PLATFORM_CONFIG}")
    
    # ... rest of your main() function
"""