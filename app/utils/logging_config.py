# app/utils/logging_config.py
"""
Enhanced configuration for application-wide logging with structured logging,
distributed tracing, log aggregation, and performance monitoring.
Reads settings from config.yaml.
"""
import logging
import logging.config
import json
import time
import yaml
import os
import uuid
import threading
import re # Added for environment variable expansion
from contextvars import ContextVar
from datetime import datetime
from typing import Dict, Any, Optional
from pythonjsonlogger import jsonlogger

# Context variables for correlation and tracing
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")
trace_id_var: ContextVar[str] = ContextVar("trace_id", default="-")
span_id_var: ContextVar[str] = ContextVar("span_id", default="-")
user_id_var: ContextVar[str] = ContextVar("user_id", default="system")

# Thread-local storage for performance metrics
thread_local = threading.local()

class StructuredFormatter(jsonlogger.JsonFormatter):
    """
    Custom JSON formatter for structured logging with correlation IDs,
    tracing information, and performance metrics.
    """
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        
        # Add correlation and tracing information
        log_record['correlation_id'] = correlation_id_var.get()
        log_record['trace_id'] = trace_id_var.get()
        log_record['span_id'] = span_id_var.get()
        log_record['user_id'] = user_id_var.get()
        
        # Add timestamp in ISO format
        log_record['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        
        # Add service metadata
        log_record['service'] = 'edi-claims-processor'
        log_record['version'] = getattr(record, 'version', '1.0.0')
        log_record['environment'] = os.getenv('ENVIRONMENT', 'development')
        
        # Add thread and process information
        log_record['thread_id'] = record.thread
        log_record['thread_name'] = record.threadName
        log_record['process_id'] = record.process
        
        # Add performance metrics if available
        if hasattr(thread_local, 'performance_metrics'):
            log_record['performance'] = thread_local.performance_metrics
        
        # Add custom fields from extra
        if hasattr(record, 'claim_id'):
            log_record['claim_id'] = record.claim_id
        if hasattr(record, 'batch_id'):
            log_record['batch_id'] = record.batch_id
        if hasattr(record, 'facility_id'):
            log_record['facility_id'] = record.facility_id
        if hasattr(record, 'error_details'):
            log_record['error_details'] = record.error_details
        if hasattr(record, 'security_details'):
            log_record['security_details'] = record.security_details
        if hasattr(record, 'performance_metrics'):
            log_record['performance_metrics'] = record.performance_metrics


class CorrelationIdFilter(logging.Filter):
    """
    A logging filter to add correlation ID and tracing information to log records.
    """
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        record.trace_id = trace_id_var.get()
        record.span_id = span_id_var.get()
        record.user_id = user_id_var.get()
        return True


class PerformanceFilter(logging.Filter):
    """
    A logging filter to add performance metrics to log records.
    """
    def filter(self, record):
        if hasattr(thread_local, 'performance_metrics'):
            record.performance = thread_local.performance_metrics
        return True


class AuditFormatter(logging.Formatter):
    """
    Specialized formatter for audit logs with enhanced security context.
    """
    def format(self, record):
        # Create audit-specific structure
        audit_record = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'event_type': 'audit',
            'correlation_id': correlation_id_var.get(),
            'trace_id': trace_id_var.get(),
            'user_id': user_id_var.get(),
            'action': getattr(record, 'action', 'unknown'),
            'resource': getattr(record, 'resource', 'unknown'),
            'outcome': getattr(record, 'outcome', 'unknown'),
            'message': record.getMessage(),
            'level': record.levelname,
            'logger': record.name,
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add security context if available
        if hasattr(record, 'security_details'):
            audit_record['security_details'] = record.security_details
        
        # Add IP address and session info if available
        if hasattr(record, 'ip_address'):
            audit_record['ip_address'] = record.ip_address
        if hasattr(record, 'session_id'):
            audit_record['session_id'] = record.session_id
        
        return json.dumps(audit_record)


def setup_logging():
    """
    Sets up enhanced logging configuration with structured logging,
    distributed tracing, and performance monitoring.
    """
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    default_level = logging.INFO
    
    if os.path.exists(config_path):
        try:
            with open(config_path, 'rt') as f:
                config_data = yaml.safe_load(f.read())
            
            logging_config = config_data.get('logging', {})

            # Helper to expand environment variables like ${VAR:-default}
            def _expand_env_var(value: str) -> str:
                if not isinstance(value, str):
                    return value
                # This regex finds ${VAR:-default} or ${VAR}
                pattern = r'\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]+))?\}'
                def replace_match(match):
                    var_name, default_value = match.groups()
                    # os.environ.get returns None if not found, which is fine
                    return os.environ.get(var_name, default_value if default_value is not None else "")
                return re.sub(pattern, replace_match, value)

            # Process paths with environment variable expansion
            log_dir = _expand_env_var(logging_config.get('log_dir', 'logs'))
            app_log_filename = logging_config.get('app_log_file', 'app.log')
            audit_log_filename = logging_config.get('audit_log_file', 'audit.log')
            performance_log_filename = logging_config.get('performance_log_file', 'performance.log')
            error_log_filename = logging_config.get('error_log_file', 'error.log')
            
            # Ensure log directory exists
            os.makedirs(log_dir, exist_ok=True)
            
            # Determine if structured logging is enabled
            use_json_format = logging_config.get('structured', True)
            log_aggregation = logging_config.get('aggregation', {})
            
            # Base formatter configurations
            if use_json_format:
                standard_formatter = {
                    '()': StructuredFormatter,
                    'format': '%(timestamp)s %(level)s %(name)s %(message)s'
                }
                console_formatter = {
                    '()': StructuredFormatter,
                    'format': '%(timestamp)s %(level)s %(name)s %(message)s'
                } if logging_config.get('console_json', False) else {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(correlation_id)s - %(message)s'
                }
            else:
                standard_formatter = {
                    'format': logging_config.get('log_format', 
                                              '%(asctime)s - %(name)s - %(levelname)s - %(correlation_id)s - %(message)s')
                }
                console_formatter = standard_formatter
            
            log_config = {
                'version': 1,
                'disable_existing_loggers': False,
                'formatters': {
                    'standard': standard_formatter,
                    'console': console_formatter,
                    'audit': {
                        '()': AuditFormatter
                    },
                    'performance': {
                        '()': StructuredFormatter,
                        'format': '%(timestamp)s %(level)s %(name)s %(message)s'
                    }
                },
                'filters': {
                    'correlation_id': {
                        '()': CorrelationIdFilter,
                    },
                    'performance': {
                        '()': PerformanceFilter,
                    }
                },
                'handlers': {
                    'console': {
                        'class': 'logging.StreamHandler',
                        'formatter': 'console',
                        'level': logging_config.get('level', 'INFO').upper(),
                        'filters': ['correlation_id'],
                        'stream': 'ext://sys.stdout',
                    },
                    'app_file_handler': {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'formatter': 'standard',
                        'filename': os.path.join(log_dir, app_log_filename),
                        'maxBytes': logging_config.get('max_bytes', 10485760),
                        'backupCount': logging_config.get('backup_count', 5),
                        'encoding': 'utf8',
                        'level': logging_config.get('level', 'INFO').upper(),
                        'filters': ['correlation_id', 'performance'],
                    },
                    'audit_file_handler': {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'formatter': 'audit',
                        'filename': os.path.join(log_dir, audit_log_filename),
                        'maxBytes': logging_config.get('max_bytes', 10485760),
                        'backupCount': logging_config.get('backup_count', 5),
                        'encoding': 'utf8',
                        'level': 'INFO',
                        'filters': ['correlation_id'],
                    },
                    'performance_file_handler': {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'formatter': 'performance',
                        'filename': os.path.join(log_dir, performance_log_filename),
                        'maxBytes': logging_config.get('max_bytes', 10485760),
                        'backupCount': logging_config.get('backup_count', 5),
                        'encoding': 'utf8',
                        'level': 'INFO',
                        'filters': ['correlation_id', 'performance'],
                    },
                    'error_file_handler': {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'formatter': 'standard',
                        'filename': os.path.join(log_dir, error_log_filename),
                        'maxBytes': logging_config.get('max_bytes', 10485760),
                        'backupCount': logging_config.get('backup_count', 10),
                        'encoding': 'utf8',
                        'level': 'ERROR',
                        'filters': ['correlation_id'],
                    }
                },
                'loggers': {
                    'app': {
                        'handlers': ['console', 'app_file_handler', 'error_file_handler'],
                        'level': logging_config.get('level', 'INFO').upper(),
                        'propagate': False
                    },
                    'audit': {
                        'handlers': ['audit_file_handler'],
                        'level': 'INFO',
                        'propagate': False
                    },
                    'performance': {
                        'handlers': ['performance_file_handler'],
                        'level': 'INFO',
                        'propagate': False
                    },
                    'sqlalchemy.engine': {
                        'handlers': ['app_file_handler'],
                        'level': logging_config.get('sqlalchemy_level', 'WARNING').upper(),
                        'propagate': False
                    },
                    'uvicorn': {
                        'handlers': ['app_file_handler'],
                        'level': 'INFO',
                        'propagate': False
                    },
                    'fastapi': {
                        'handlers': ['app_file_handler'],
                        'level': 'INFO',
                        'propagate': False
                    }
                },
                'root': {
                    'handlers': ['console', 'app_file_handler'],
                    'level': 'WARNING',
                }
            }
            
            # Add log aggregation handlers if configured
            if log_aggregation.get('enabled', False):
                _add_aggregation_handlers(log_config, log_aggregation)
            
            logging.config.dictConfig(log_config)
            logging.getLogger('app').info(
                "Enhanced logging configured successfully from config.yaml",
                extra={'structured_logging': use_json_format, 'aggregation_enabled': log_aggregation.get('enabled', False)}
            )

        except Exception as e:
            _setup_fallback_logging(default_level)
            logging.exception(f"Error configuring enhanced logging from YAML, using fallback: {e}")
    else:
        _setup_fallback_logging(default_level)
        logging.warning("config.yaml not found. Using fallback logging configuration.")


def _add_aggregation_handlers(log_config: Dict[str, Any], aggregation_config: Dict[str, Any]):
    """
    Add log aggregation handlers (e.g., for ELK, Fluentd, etc.)
    """
    if aggregation_config.get('type') == 'syslog':
        log_config['handlers']['syslog'] = {
            'class': 'logging.handlers.SysLogHandler',
            'formatter': 'standard',
            'address': (
                aggregation_config.get('host', 'localhost'),
                aggregation_config.get('port', 514)
            ),
            'facility': aggregation_config.get('facility', 'user'),
            'level': 'INFO',
            'filters': ['correlation_id', 'performance']
        }
        # Add to app logger
        log_config['loggers']['app']['handlers'].append('syslog')
    
    elif aggregation_config.get('type') == 'http':
        # Custom HTTP handler for log aggregation services
        log_config['handlers']['http_aggregator'] = {
            'class': 'logging.handlers.HTTPHandler',
            'formatter': 'standard',
            'host': aggregation_config.get('host', 'localhost'),
            'url': aggregation_config.get('endpoint', '/logs'),
            'method': aggregation_config.get('method', 'POST'),
            'level': 'INFO',
            'filters': ['correlation_id', 'performance']
        }
        log_config['loggers']['app']['handlers'].append('http_aggregator')


def _setup_fallback_logging(level):
    """
    Setup basic fallback logging configuration.
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(correlation_id)s - %(message)s'
    )
    for handler in logging.root.handlers:
        handler.addFilter(CorrelationIdFilter())


def get_logger(name: str):
    """
    Returns a logger instance with the given name.
    Ensures correlation ID and tracing information is available.
    """
    return logging.getLogger(name)


def set_correlation_id(cid: str = None) -> str:
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


def set_trace_id(trace_id: str = None) -> str:
    """
    Sets the trace ID for distributed tracing.
    """
    if trace_id is None:
        trace_id = str(uuid.uuid4())
    trace_id_var.set(trace_id)
    return trace_id


def get_trace_id() -> str:
    """
    Gets the current trace ID.
    """
    return trace_id_var.get()


def set_span_id(span_id: str = None) -> str:
    """
    Sets the span ID for distributed tracing.
    """
    if span_id is None:
        span_id = str(uuid.uuid4())
    span_id_var.set(span_id)
    return span_id


def get_span_id() -> str:
    """
    Gets the current span ID.
    """
    return span_id_var.get()


def set_user_id(user_id: str):
    """
    Sets the user ID for the current context.
    """
    user_id_var.set(user_id)


def get_user_id() -> str:
    """
    Gets the current user ID.
    """
    return user_id_var.get()


class PerformanceLogger:
    """
    Context manager for performance logging with automatic timing.
    """
    def __init__(self, operation_name: str, logger: logging.Logger = None, 
                 threshold_ms: float = None, **kwargs):
        self.operation_name = operation_name
        self.logger = logger or get_logger('performance')
        self.threshold_ms = threshold_ms
        self.extra_data = kwargs
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        self.logger.debug(
            f"Starting operation: {self.operation_name}",
            extra={
                'operation': self.operation_name,
                'event': 'start',
                'performance_metrics': {
                    'operation_name': self.operation_name,
                    'start_time': self.start_time,
                    **self.extra_data
                }
            }
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        duration_ms = (self.end_time - self.start_time) * 1000
        
        performance_data = {
            'operation_name': self.operation_name,
            'duration_ms': round(duration_ms, 2),
            'start_time': self.start_time,
            'end_time': self.end_time,
            'success': exc_type is None,
            **self.extra_data
        }
        
        if exc_type is not None:
            performance_data['error_type'] = exc_type.__name__
            performance_data['error_message'] = str(exc_val)
        
        # Store in thread local for other loggers to access
        thread_local.performance_metrics = performance_data
        
        # Log based on threshold and success
        log_level = logging.INFO
        if self.threshold_ms and duration_ms > self.threshold_ms:
            log_level = logging.WARNING
        if exc_type is not None:
            log_level = logging.ERROR
        
        self.logger.log(
            log_level,
            f"Completed operation: {self.operation_name} in {duration_ms:.2f}ms",
            extra={
                'operation': self.operation_name,
                'event': 'complete',
                'performance_metrics': performance_data
            }
        )
        
        # Clear thread local performance metrics
        if hasattr(thread_local, 'performance_metrics'):
            delattr(thread_local, 'performance_metrics')


def log_performance(operation_name: str, threshold_ms: float = None, **kwargs):
    """
    Decorator for automatic performance logging.
    """
    def decorator(func):
        def wrapper(*args, **func_kwargs):
            logger = get_logger(f'performance.{func.__module__}.{func.__name__}')
            with PerformanceLogger(operation_name, logger, threshold_ms, **kwargs):
                return func(*args, **func_kwargs)
        return wrapper
    return decorator


def log_audit_event(action: str, resource: str, outcome: str = "success", 
                   user_id: str = None, **details):
    """
    Logs an audit event with structured information.
    """
    audit_logger = get_logger('audit')
    
    if user_id:
        set_user_id(user_id)
    
    audit_logger.info(
        f"Audit: {action} on {resource} - {outcome}",
        extra={
            'action': action,
            'resource': resource,
            'outcome': outcome,
            'security_details': details
        }
    )


class TraceContext:
    """
    Context manager for distributed tracing spans.
    """
    def __init__(self, span_name: str, trace_id: str = None, parent_span_id: str = None):
        self.span_name = span_name
        self.trace_id = trace_id or get_trace_id()
        self.parent_span_id = parent_span_id or get_span_id()
        self.span_id = None
        self.previous_span_id = None
    
    def __enter__(self):
        self.previous_span_id = get_span_id()
        self.span_id = set_span_id()
        set_trace_id(self.trace_id)
        
        logger = get_logger('app.trace')
        logger.debug(
            f"Starting span: {self.span_name}",
            extra={
                'span_name': self.span_name,
                'parent_span_id': self.parent_span_id,
                'trace_event': 'span_start'
            }
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        logger = get_logger('app.trace')
        logger.debug(
            f"Ending span: {self.span_name}",
            extra={
                'span_name': self.span_name,
                'success': exc_type is None,
                'trace_event': 'span_end'
            }
        )
        set_span_id(self.previous_span_id)


if __name__ == '__main__':
    # Example usage and testing
    setup_logging()
    set_correlation_id("LOGGING_TEST")
    set_trace_id("trace_12345")
    set_user_id("test_user")
    
    app_logger = get_logger('app.test')
    audit_logger = get_logger('audit.test')
    performance_logger = get_logger('performance.test')
    
    # Test basic logging
    app_logger.info("This is a test info message with structured logging")
    app_logger.warning("This is a warning with extra data", 
                      extra={'claim_id': 'C12345', 'facility_id': 'FAC001'})
    
    # Test audit logging
    log_audit_event("login", "system", "success", 
                   ip_address="192.168.1.1", session_id="sess_123")
    
    # Test performance logging
    with PerformanceLogger("database_query", threshold_ms=1000, 
                          query_type="select", table="claims"):
        time.sleep(0.1)  # Simulate work
    
    # Test performance decorator
    @log_performance("test_operation", threshold_ms=50)
    def test_function():
        time.sleep(0.02)
        return "completed"
    
    result = test_function()
    
    # Test tracing context
    with TraceContext("main_operation"):
        app_logger.info("Inside main operation span")
        with TraceContext("sub_operation"):
            app_logger.info("Inside sub operation span")
    
    # Test error logging
    try:
        raise ValueError("Test error for logging")
    except Exception as e:
        app_logger.error("Error occurred", exc_info=True, 
                        extra={'error_details': {'type': 'ValueError', 'context': 'test'}})
    
    print("Enhanced logging test completed. Check log files for structured output.")
