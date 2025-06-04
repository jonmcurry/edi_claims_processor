# app/utils/logging_config.py
"""
Configuration for application-wide logging.
Reads settings from config.yaml.
Includes correlation ID handling.
"""
import logging
import logging.config
import yaml
import os
import uuid
from contextvars import ContextVar

# Context variable for correlation ID
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")

class CorrelationIdFilter(logging.Filter):
    """
    A logging filter to add the correlation ID to log records.
    """
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

def setup_logging():
    """
    Sets up logging configuration based on settings in config.yaml.
    If config.yaml is not found or is invalid, sets up a basic default configuration.
    """
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    default_level = logging.INFO
    
    if os.path.exists(config_path):
        try:
            with open(config_path, 'rt') as f:
                config_data = yaml.safe_load(f.read())
            
            log_config = {
                'version': 1,
                'disable_existing_loggers': False,
                'formatters': {
                    'standard': {
                        'format': config_data.get('logging', {}).get('log_format', 
                                                                    '%(asctime)s - %(name)s - %(levelname)s - %(correlation_id)s - %(message)s')
                    },
                },
                'filters': {
                    'correlation_id': {
                        '()': CorrelationIdFilter,
                    }
                },
                'handlers': {
                    'console': {
                        'class': 'logging.StreamHandler',
                        'formatter': 'standard',
                        'level': config_data.get('logging', {}).get('level', 'INFO').upper(),
                        'filters': ['correlation_id'],
                        'stream': 'ext://sys.stdout',  # Default to stdout
                    },
                    'app_file_handler': {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'formatter': 'standard',
                        'filename': config_data.get('logging', {}).get('app_log_file', 'logs/app.log'),
                        'maxBytes': config_data.get('logging', {}).get('max_bytes', 10485760),
                        'backupCount': config_data.get('logging', {}).get('backup_count', 5),
                        'encoding': 'utf8',
                        'level': config_data.get('logging', {}).get('level', 'INFO').upper(),
                        'filters': ['correlation_id'],
                    },
                    'audit_file_handler': {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'formatter': 'standard',
                        'filename': config_data.get('logging', {}).get('audit_log_file', 'logs/audit.log'),
                        'maxBytes': config_data.get('logging', {}).get('max_bytes', 10485760),
                        'backupCount': config_data.get('logging', {}).get('backup_count', 5),
                        'encoding': 'utf8',
                        'level': 'INFO', # Audit logs should generally capture INFO and above
                        'filters': ['correlation_id'],
                    }
                },
                'loggers': {
                    'app': { # Main application logger
                        'handlers': ['console', 'app_file_handler'],
                        'level': config_data.get('logging', {}).get('level', 'INFO').upper(),
                        'propagate': False
                    },
                    'audit': { # Logger for audit trails
                        'handlers': ['audit_file_handler'], # Potentially also console if needed
                        'level': 'INFO',
                        'propagate': False
                    },
                    'sqlalchemy.engine': { # Example: SQLA logs, can be noisy
                        'handlers': ['app_file_handler'], # Or a separate db_log_handler
                        'level': 'WARNING', # Adjust as needed
                        'propagate': False
                    }
                },
                'root': { # Catch-all for other libraries
                    'handlers': ['console', 'app_file_handler'],
                    'level': 'WARNING', # Be less verbose for external libs by default
                }
            }
            
            # Ensure log directory exists
            log_dir = config_data.get('logging', {}).get('log_dir', 'logs')
            if not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            logging.config.dictConfig(log_config)
            logging.getLogger('app').info("Logging configured successfully from config.yaml.")

        except Exception as e:
            logging.basicConfig(level=default_level, format='%(asctime)s - %(name)s - %(levelname)s - %(correlation_id)s - %(message)s')
            logging.exception(f"Error configuring logging from YAML, using basicConfig: {e}")
            # Add correlation ID filter to basic config handlers if possible
            for handler in logging.root.handlers:
                handler.addFilter(CorrelationIdFilter())
    else:
        logging.basicConfig(level=default_level, format='%(asctime)s - %(name)s - %(levelname)s - %(correlation_id)s - %(message)s')
        logging.warning("config.yaml not found. Using basic logging configuration.")
        for handler in logging.root.handlers:
            handler.addFilter(CorrelationIdFilter())


def get_logger(name: str):
    """
    Returns a logger instance with the given name.
    Ensures correlation ID is available.
    """
    return logging.getLogger(name)

def set_correlation_id(cid: str = None):
    """
    Sets the correlation ID for the current context.
    If no cid is provided, a new UUID is generated.
    """
    if cid is None:
        cid = str(uuid.uuid4())
    correlation_id_var.set(cid)
    return cid

def get_correlation_id() -> str:
    """
    Gets the current correlation ID.
    """
    return correlation_id_var.get()

# Example usage (typically called once at application startup in main.py)
# if __name__ == '__main__':
#     setup_logging()
#     set_correlation_id() # Set initial correlation ID for the main thread/process
#
#     app_logger = get_logger('app.example_module')
#     audit_logger = get_logger('audit.security')
#
#     app_logger.info("This is an app info message.")
#     app_logger.warning("This is an app warning message.")
#     audit_logger.info("User 'admin' logged in.")
#
#     # In a new request or task, you might reset the correlation ID
#     new_request_id = set_correlation_id()
#     app_logger.info(f"Processing new request with ID: {new_request_id}")
