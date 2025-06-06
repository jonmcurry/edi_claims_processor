# app/utils/platform_config.py
"""
Platform-specific configuration and optimizations for Windows, Linux, and macOS.
"""
import os
import sys
import multiprocessing
import asyncio
from typing import Dict, Any

def configure_multiprocessing():
    """Configure multiprocessing for the current platform."""
    if sys.platform == "win32":
        # Windows-specific multiprocessing setup
        multiprocessing.freeze_support()
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass  # Already set
        
        # Configure event loop policy for Windows
        try:
            import winloop
            asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
        except ImportError:
            # Fall back to ProactorEventLoop for better Windows support
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    else:
        # Unix-like systems (Linux, macOS)
        try:
            # Try to use fork if available (more efficient)
            multiprocessing.set_start_method('fork', force=True)
        except (RuntimeError, ValueError):
            try:
                # Fall back to spawn
                multiprocessing.set_start_method('spawn', force=True)
            except RuntimeError:
                pass  # Already set
        
        # Try to use uvloop for better performance on Unix
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ImportError:
            pass  # Use default event loop

# Platform configuration
PLATFORM_CONFIG: Dict[str, Any] = {
    "platform": sys.platform,
    "max_workers": min(os.cpu_count() or 4, 16 if sys.platform != "win32" else 8),
    "preferred_executor": "process" if sys.platform != "win32" else "thread",
    "use_multiprocessing": sys.platform != "win32",  # More conservative on Windows
    "supports_fork": hasattr(os, 'fork') and sys.platform != "win32",
    "supports_uvloop": sys.platform != "win32",
    "default_connection_timeout": 30 if sys.platform == "win32" else 15,
    "default_pool_size": 8 if sys.platform == "win32" else 15,
}

# Mark that platform config is available
HAS_PLATFORM_CONFIG = True