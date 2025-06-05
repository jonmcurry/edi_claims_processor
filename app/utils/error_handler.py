# app/utils/error_handler.py
"""
Defines custom application exceptions and standardized error handling utilities.
Includes enhanced categorization for UI filtering and recovery strategies.
Production features: metrics collection, circuit breaker pattern, automated recovery, ML analysis.
"""
import time
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
from dataclasses import dataclass, field
import json
import uuid

from app.utils.logging_config import get_logger, get_correlation_id

logger = get_logger('app.error_handler')

# --- Error Severity and Categories for ML Analysis ---
class ErrorSeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class ErrorCategory(Enum):
    CONFIGURATION = "Configuration"
    DATABASE = "Database"
    STAGING_DATABASE = "StagingDatabase"
    PRODUCTION_DATABASE = "ProductionDatabase"
    PARSING = "Parsing"
    VALIDATION = "Validation"
    MACHINE_LEARNING = "MachineLearning"
    API = "API"
    EXTERNAL_SERVICE = "ExternalService"
    CACHING = "Caching"
    NETWORK = "Network"
    AUTHENTICATION = "Authentication"
    AUTHORIZATION = "Authorization"
    BUSINESS_LOGIC = "BusinessLogic"
    INTEGRATION = "Integration"
    PERFORMANCE = "Performance"
    SECURITY = "Security"
    DATA_QUALITY = "DataQuality"

class ErrorImpact(Enum):
    USER_FACING = "UserFacing"
    BACKGROUND_PROCESS = "BackgroundProcess"
    SYSTEM_WIDE = "SystemWide"
    DATA_CORRUPTION = "DataCorruption"
    SECURITY_BREACH = "SecurityBreach"

# --- Error Metrics Collection ---
@dataclass
class ErrorMetric:
    error_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    error_code: str = ""
    category: ErrorCategory = ErrorCategory.CONFIGURATION
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    impact: ErrorImpact = ErrorImpact.BACKGROUND_PROCESS
    component: str = ""
    correlation_id: str = ""
    claim_id: Optional[str] = None
    user_id: Optional[str] = None
    error_message: str = ""
    stack_trace: Optional[str] = None
    resolution_time: Optional[float] = None
    retry_count: int = 0
    recovery_attempted: bool = False
    recovery_successful: bool = False
    additional_context: Dict[str, Any] = field(default_factory=dict)

class ErrorMetricsCollector:
    """Collects and analyzes error metrics for monitoring and ML analysis."""
    
    def __init__(self, max_metrics_memory: int = 10000):
        self._metrics: deque[ErrorMetric] = deque(maxlen=max_metrics_memory)
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._category_counts: Dict[ErrorCategory, int] = defaultdict(int)
        self._hourly_errors: Dict[str, int] = defaultdict(int)  # hour_key -> count
        self._component_errors: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()
        
    def record_error(self, metric: ErrorMetric):
        """Records an error metric for analysis."""
        with self._lock:
            self._metrics.append(metric)
            self._error_counts[metric.error_code] += 1
            self._category_counts[metric.category] += 1
            
            # Track hourly patterns
            hour_key = metric.timestamp.strftime("%Y-%m-%d-%H")
            self._hourly_errors[hour_key] += 1
            
            # Track component errors
            self._component_errors[metric.component] += 1
            
            # Log for audit trail
            logger.info(
                f"Error recorded: {metric.error_code} in {metric.component} "
                f"(Severity: {metric.severity.value}, Impact: {metric.impact.value})",
                extra={
                    'error_metric': {
                        'error_id': metric.error_id,
                        'error_code': metric.error_code,
                        'category': metric.category.value,
                        'severity': metric.severity.value,
                        'correlation_id': metric.correlation_id,
                        'component': metric.component
                    }
                }
            )
    
    def get_error_rate(self, time_window_minutes: int = 60, category: Optional[ErrorCategory] = None) -> float:
        """Calculate error rate for the specified time window."""
        with self._lock:
            cutoff_time = datetime.utcnow() - timedelta(minutes=time_window_minutes)
            
            if category:
                recent_errors = [m for m in self._metrics 
                               if m.timestamp >= cutoff_time and m.category == category]
            else:
                recent_errors = [m for m in self._metrics if m.timestamp >= cutoff_time]
            
            return len(recent_errors) / max(time_window_minutes, 1)  # errors per minute
    
    def get_top_errors(self, limit: int = 10) -> List[tuple[str, int]]:
        """Get the most frequent error codes."""
        with self._lock:
            return sorted(self._error_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    def get_error_patterns(self) -> Dict[str, Any]:
        """Get error patterns for ML analysis."""
        with self._lock:
            patterns = {
                'total_errors': len(self._metrics),
                'error_rate_1h': self.get_error_rate(60),
                'error_rate_24h': self.get_error_rate(24 * 60),
                'top_errors': self.get_top_errors(),
                'category_distribution': dict(self._category_counts),
                'component_distribution': dict(self._component_errors),
                'hourly_trends': dict(self._hourly_errors),
                'severity_distribution': self._get_severity_distribution(),
                'impact_distribution': self._get_impact_distribution(),
                'recovery_success_rate': self._get_recovery_success_rate()
            }
            return patterns
    
    def _get_severity_distribution(self) -> Dict[str, int]:
        """Get distribution of error severities."""
        severity_dist = defaultdict(int)
        for metric in self._metrics:
            severity_dist[metric.severity.value] += 1
        return dict(severity_dist)
    
    def _get_impact_distribution(self) -> Dict[str, int]:
        """Get distribution of error impacts."""
        impact_dist = defaultdict(int)
        for metric in self._metrics:
            impact_dist[metric.impact.value] += 1
        return dict(impact_dist)
    
    def _get_recovery_success_rate(self) -> float:
        """Calculate recovery success rate."""
        recovery_attempts = [m for m in self._metrics if m.recovery_attempted]
        if not recovery_attempts:
            return 0.0
        successful_recoveries = [m for m in recovery_attempts if m.recovery_successful]
        return len(successful_recoveries) / len(recovery_attempts)

# Global metrics collector instance
metrics_collector = ErrorMetricsCollector()

# --- Circuit Breaker Pattern ---
@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 60.0  # seconds
    success_threshold: int = 3  # consecutive successes needed to close circuit

class CircuitBreakerState(Enum):
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Failing, requests rejected
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered

class CircuitBreaker:
    """Implements circuit breaker pattern to prevent cascade failures."""
    
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.lock = threading.RLock()
        
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        with self.lock:
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitBreakerState.HALF_OPEN
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                self._on_failure()
                raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.config.recovery_timeout
    
    def _on_success(self):
        """Handle successful operation."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                logger.info(f"Circuit breaker {self.name} closed after successful recovery")
        else:
            self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.config.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
        
        # Reset success count if we were in half-open state
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count = 0
            self.state = CircuitBreakerState.OPEN

# Global circuit breakers for different components
circuit_breakers: Dict[str, CircuitBreaker] = {}

def get_circuit_breaker(component_name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
    """Get or create a circuit breaker for a component."""
    if component_name not in circuit_breakers:
        circuit_breakers[component_name] = CircuitBreaker(component_name, config)
    return circuit_breakers[component_name]

# --- Custom Exception Classes with Enhanced Features ---
class AppException(Exception):
    """Base class for custom application exceptions with enhanced features."""
    
    def __init__(self, 
                 message: str, 
                 error_code: str = "APP_ERROR", 
                 details: dict = None, 
                 category: ErrorCategory = ErrorCategory.CONFIGURATION,
                 severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                 impact: ErrorImpact = ErrorImpact.BACKGROUND_PROCESS,
                 component: str = "Unknown",
                 recoverable: bool = True,
                 auto_retry: bool = False,
                 max_retries: int = 3):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details if details is not None else {}
        self.category = category
        self.severity = severity
        self.impact = impact
        self.component = component
        self.recoverable = recoverable
        self.auto_retry = auto_retry
        self.max_retries = max_retries
        self.correlation_id = get_correlation_id()
        self.timestamp = datetime.utcnow()
        
        # Record metric
        self._record_metric()

    def _record_metric(self):
        """Record this exception as a metric."""
        metric = ErrorMetric(
            error_code=self.error_code,
            category=self.category,
            severity=self.severity,
            impact=self.impact,
            component=self.component,
            correlation_id=self.correlation_id,
            error_message=self.message,
            additional_context=self.details
        )
        metrics_collector.record_error(metric)

    def __str__(self):
        return f"{self.error_code} ({self.category.value}): {self.message}"

# Specific Exception Classes with Enhanced Categorization
class CircuitBreakerOpenError(AppException):
    """Raised when circuit breaker is open."""
    def __init__(self, message: str, component: str = "Unknown"):
        super().__init__(
            message=message,
            error_code="CIRCUIT_BREAKER_OPEN",
            category=ErrorCategory.INTEGRATION,
            severity=ErrorSeverity.HIGH,
            impact=ErrorImpact.SYSTEM_WIDE,
            component=component,
            recoverable=True,
            auto_retry=True
        )

class ConfigError(AppException):
    """For errors related to application configuration."""
    def __init__(self, message: str, details: dict = None, component: str = "Configuration"):
        super().__init__(
            message=message,
            error_code="CONFIG_ERROR",
            details=details,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            impact=ErrorImpact.SYSTEM_WIDE,
            component=component,
            recoverable=False
        )

class DatabaseError(AppException):
    """For errors related to database operations."""
    def __init__(self, message: str, original_exception: Exception = None, details: dict = None, component: str = "Database"):
        super().__init__(
            message=message,
            error_code="DB_ERROR",
            details=details,
            category=ErrorCategory.DATABASE,
            severity=ErrorSeverity.HIGH,
            impact=ErrorImpact.BACKGROUND_PROCESS,
            component=component,
            recoverable=True,
            auto_retry=True,
            max_retries=3
        )
        self.original_exception = original_exception

class StagingDBError(DatabaseError):
    """For errors specific to the PostgreSQL staging database."""
    def __init__(self, message: str, original_exception: Exception = None, details: dict = None):
        super().__init__(
            message=message,
            original_exception=original_exception,
            details=details,
            component="StagingDatabase"
        )
        self.error_code = "STAGING_DB_ERROR"
        self.category = ErrorCategory.STAGING_DATABASE

class ProductionDBError(DatabaseError):
    """For errors specific to the SQL Server production database."""
    def __init__(self, message: str, original_exception: Exception = None, details: dict = None):
        super().__init__(
            message=message,
            original_exception=original_exception,
            details=details,
            component="ProductionDatabase"
        )
        self.error_code = "PROD_DB_ERROR"
        self.category = ErrorCategory.PRODUCTION_DATABASE

class EDIParserError(AppException):
    """For errors encountered during EDI file parsing."""
    def __init__(self, message: str, claim_id: str = None, segment: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="EDI_PARSE_ERROR",
            details=details,
            category=ErrorCategory.PARSING,
            severity=ErrorSeverity.MEDIUM,
            impact=ErrorImpact.BACKGROUND_PROCESS,
            component="EDIParser",
            recoverable=True,
            auto_retry=True
        )
        self.claim_id = claim_id
        self.segment = segment
        if self.claim_id: 
            self.details['claim_id'] = self.claim_id
        if self.segment: 
            self.details['segment'] = self.segment

class ValidationError(AppException):
    """For errors during data validation (rules engine)."""
    def __init__(self, message: str, claim_id: str = None, field: str = None, rule_id: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            details=details,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            impact=ErrorImpact.DATA_QUALITY,
            component="ValidationEngine",
            recoverable=True
        )
        self.claim_id = claim_id
        self.field = field
        self.rule_id = rule_id
        if self.claim_id: 
            self.details['claim_id'] = self.claim_id
        if self.field: 
            self.details['field'] = self.field
        if self.rule_id: 
            self.details['rule_id'] = self.rule_id

class MLModelError(AppException):
    """For errors related to machine learning model operations."""
    def __init__(self, message: str, model_name: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="ML_MODEL_ERROR",
            details=details,
            category=ErrorCategory.MACHINE_LEARNING,
            severity=ErrorSeverity.HIGH,
            impact=ErrorImpact.BACKGROUND_PROCESS,
            component="MLPredictor",
            recoverable=True,
            auto_retry=True
        )
        self.model_name = model_name
        if self.model_name: 
            self.details['model_name'] = self.model_name

class APIError(AppException):
    """For errors related to API operations."""
    def __init__(self, message: str, status_code: int = 500, details: dict = None, component: str = "API"):
        super().__init__(
            message=message,
            error_code="API_ERROR",
            details=details,
            category=ErrorCategory.API,
            severity=ErrorSeverity.MEDIUM,
            impact=ErrorImpact.USER_FACING,
            component=component,
            recoverable=True
        )
        self.status_code = status_code

class ExternalServiceError(APIError):
    """For errors when interacting with external services."""
    def __init__(self, message: str, service_name: str, status_code: int = 503, details: dict = None):
        super().__init__(
            message=message,
            status_code=status_code,
            details=details,
            component=f"ExternalService.{service_name}"
        )
        self.error_code = "EXT_SERVICE_ERROR"
        self.category = ErrorCategory.EXTERNAL_SERVICE
        self.impact = ErrorImpact.SYSTEM_WIDE
        self.service_name = service_name
        self.details['service_name'] = self.service_name

class CacheError(AppException):
    """For errors related to caching operations."""
    def __init__(self, message: str, cache_key: str = None, details: dict = None):
        super().__init__(
            message=message,
            error_code="CACHE_ERROR",
            details=details,
            category=ErrorCategory.CACHING,
            severity=ErrorSeverity.LOW,
            impact=ErrorImpact.PERFORMANCE,
            component="CacheManager",
            recoverable=True,
            auto_retry=True
        )
        self.cache_key = cache_key
        if self.cache_key: 
            self.details['cache_key'] = self.cache_key

class SecurityError(AppException):
    """For security-related errors."""
    def __init__(self, message: str, details: dict = None, component: str = "Security"):
        super().__init__(
            message=message,
            error_code="SECURITY_ERROR",
            details=details,
            category=ErrorCategory.SECURITY,
            severity=ErrorSeverity.CRITICAL,
            impact=ErrorImpact.SECURITY_BREACH,
            component=component,
            recoverable=False
        )

# --- Enhanced Error Handling Utilities ---
def log_app_exception(exc: AppException, level: str = "error", claim_id: str = None):
    """Logs an AppException with structured details and metrics."""
    log_method = getattr(logger, level.lower(), logger.error)
    
    log_message = (f"AppException: Code={exc.error_code}, Category={exc.category.value}, "
                  f"Severity={exc.severity.value}, Component={exc.component}, Message='{exc.message}'")
    
    if claim_id:
        log_message += f", ClaimID='{claim_id}'"
    
    log_details = exc.details.copy()
    log_details.update({
        'error_code': exc.error_code,
        'category': exc.category.value,
        'severity': exc.severity.value,
        'impact': exc.impact.value,
        'component': exc.component,
        'correlation_id': exc.correlation_id,
        'recoverable': exc.recoverable,
        'auto_retry': exc.auto_retry
    })
    
    if hasattr(exc, 'original_exception') and exc.original_exception:
        log_details['original_exception'] = str(exc.original_exception)

    log_method(log_message, extra={'error_details': log_details}, 
              exc_info=exc.severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL])

# --- Automated Recovery Strategies ---
class RecoveryStrategy:
    """Base class for recovery strategies."""
    
    def can_recover(self, exception: AppException) -> bool:
        """Check if this strategy can handle the exception."""
        return exception.recoverable
    
    def attempt_recovery(self, exception: AppException, context: Dict[str, Any] = None) -> bool:
        """Attempt to recover from the exception. Returns True if successful."""
        raise NotImplementedError

class DatabaseReconnectStrategy(RecoveryStrategy):
    """Recovery strategy for database connection issues."""
    
    def can_recover(self, exception: AppException) -> bool:
        return (isinstance(exception, DatabaseError) and 
                'connection' in exception.message.lower())
    
    def attempt_recovery(self, exception: AppException, context: Dict[str, Any] = None) -> bool:
        """Attempt to reconnect to database."""
        try:
            # Import here to avoid circular dependency
            from app.database.connection_manager import init_database_connections
            
            logger.info(f"Attempting database reconnection for {exception.component}")
            init_database_connections()
            logger.info("Database reconnection successful")
            return True
        except Exception as e:
            logger.error(f"Database reconnection failed: {e}")
            return False

class CacheRefreshStrategy(RecoveryStrategy):
    """Recovery strategy for cache-related issues."""
    
    def can_recover(self, exception: AppException) -> bool:
        return isinstance(exception, CacheError)
    
    def attempt_recovery(self, exception: AppException, context: Dict[str, Any] = None) -> bool:
        """Attempt to refresh cache."""
        try:
            # Import here to avoid circular dependency
            from app.utils.caching import rvu_cache_instance
            
            if rvu_cache_instance and hasattr(rvu_cache_instance, 'refresh_cache'):
                logger.info("Attempting cache refresh")
                rvu_cache_instance.refresh_cache()
                logger.info("Cache refresh successful")
                return True
            return False
        except Exception as e:
            logger.error(f"Cache refresh failed: {e}")
            return False

class MLModelReloadStrategy(RecoveryStrategy):
    """Recovery strategy for ML model issues."""
    
    def can_recover(self, exception: AppException) -> bool:
        return isinstance(exception, MLModelError)
    
    def attempt_recovery(self, exception: AppException, context: Dict[str, Any] = None) -> bool:
        """Attempt to reload ML model."""
        try:
            logger.info("Attempting ML model reload")
            # Import here to avoid circular dependency
            from app.processing.ml_predictor import MLPredictor
            
            # Reset the singleton model to force reload
            MLPredictor._model = None
            MLPredictor()  # This will trigger reload
            logger.info("ML model reload successful")
            return True
        except Exception as e:
            logger.error(f"ML model reload failed: {e}")
            return False

# Registry of recovery strategies
recovery_strategies: List[RecoveryStrategy] = [
    DatabaseReconnectStrategy(),
    CacheRefreshStrategy(),
    MLModelReloadStrategy()
]

def attempt_automated_recovery(exception: AppException, context: Dict[str, Any] = None) -> bool:
    """Attempt automated recovery using registered strategies."""
    if not exception.recoverable:
        return False
    
    for strategy in recovery_strategies:
        if strategy.can_recover(exception):
            logger.info(f"Attempting recovery with {strategy.__class__.__name__}")
            
            # Update metric with recovery attempt
            metric = ErrorMetric(
                error_code=exception.error_code,
                category=exception.category,
                severity=exception.severity,
                impact=exception.impact,
                component=exception.component,
                correlation_id=exception.correlation_id,
                error_message=exception.message,
                recovery_attempted=True
            )
            
            success = strategy.attempt_recovery(exception, context)
            metric.recovery_successful = success
            metrics_collector.record_error(metric)
            
            if success:
                logger.info(f"Recovery successful with {strategy.__class__.__name__}")
                return True
            else:
                logger.warning(f"Recovery failed with {strategy.__class__.__name__}")
    
    return False

def handle_exception(e: Exception, 
                    context: str = "General", 
                    claim_id: str = None, 
                    re_raise_as: AppException = None,
                    attempt_recovery: bool = True,
                    component: str = "Unknown"):
    """Enhanced exception handler with recovery and circuit breaker support."""
    
    if isinstance(e, AppException):
        log_app_exception(e, claim_id=claim_id or getattr(e, 'claim_id', None))
        
        # Attempt automated recovery if enabled
        if attempt_recovery and e.auto_retry:
            recovery_successful = attempt_automated_recovery(e, {'context': context, 'claim_id': claim_id})
            if recovery_successful:
                logger.info(f"Recovery successful for {e.error_code}, continuing execution")
                return  # Don't re-raise if recovery was successful
        
        if re_raise_as:
            raise re_raise_as from e
        raise e
    else:
        # For unexpected/generic exceptions
        err_message = f"Unexpected error in {context}: {str(e)}"
        logger.error(err_message, exc_info=True, extra={'claim_id': claim_id, 'component': component})
        
        if re_raise_as:
            if issubclass(re_raise_as, DatabaseError):
                wrapped_exception = re_raise_as(message=err_message, original_exception=e)
            else:
                wrapped_exception = re_raise_as(message=err_message)
            
            # Attempt recovery for wrapped exceptions
            if attempt_recovery and wrapped_exception.auto_retry:
                recovery_successful = attempt_automated_recovery(wrapped_exception, {'context': context, 'claim_id': claim_id})
                if recovery_successful:
                    return
            
            raise wrapped_exception from e
        else:
            wrapped_exception = AppException(
                message=err_message, 
                error_code="UNEXPECTED_ERROR", 
                category=ErrorCategory.BUSINESS_LOGIC,
                component=component
            )
            raise wrapped_exception from e

# --- Retry Mechanism with Circuit Breaker ---
def retry_with_circuit_breaker(component_name: str, 
                              max_retries: int = 3, 
                              delay: float = 1.0,
                              backoff_multiplier: float = 2.0,
                              circuit_config: Optional[CircuitBreakerConfig] = None):
    """Decorator for retry with circuit breaker pattern."""
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            circuit_breaker = get_circuit_breaker(component_name, circuit_config)
            
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return circuit_breaker.call(func, *args, **kwargs)
                except CircuitBreakerOpenError:
                    logger.warning(f"Circuit breaker {component_name} is open, not retrying")
                    raise
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        sleep_time = delay * (backoff_multiplier ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed for {component_name}, retrying in {sleep_time}s: {e}")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {component_name}")
            
            raise last_exception
        return wrapper
    return decorator

# --- Error Analysis for ML ---
def get_error_analysis_data(time_window_hours: int = 24) -> Dict[str, Any]:
    """Get comprehensive error analysis data for ML processing."""
    patterns = metrics_collector.get_error_patterns()
    
    # Add additional analysis
    cutoff_time = datetime.utcnow() - timedelta(hours=time_window_hours)
    recent_metrics = [m for m in metrics_collector._metrics if m.timestamp >= cutoff_time]
    
    analysis = {
        'time_window_hours': time_window_hours,
        'patterns': patterns,
        'detailed_analysis': {
            'error_clusters': _cluster_errors_by_similarity(recent_metrics),
            'temporal_patterns': _analyze_temporal_patterns(recent_metrics),
            'correlation_analysis': _analyze_error_correlations(recent_metrics),
            'recovery_effectiveness': _analyze_recovery_effectiveness(recent_metrics),
            'circuit_breaker_status': _get_circuit_breaker_status()
        }
    }
    
    return analysis

def _cluster_errors_by_similarity(metrics: List[ErrorMetric]) -> Dict[str, Any]:
    """Cluster errors by similarity for pattern recognition."""
    # Group by category and component
    clusters = defaultdict(list)
    for metric in metrics:
        cluster_key = f"{metric.category.value}_{metric.component}"
        clusters[cluster_key].append({
            'error_code': metric.error_code,
            'message': metric.error_message,
            'timestamp': metric.timestamp.isoformat(),
            'severity': metric.severity.value
        })
    
    return dict(clusters)

def _analyze_temporal_patterns(metrics: List[ErrorMetric]) -> Dict[str, Any]:
    """Analyze temporal patterns in errors."""
    if not metrics:
        return {}
    
    # Group errors by hour
    hourly_distribution = defaultdict(int)
    daily_distribution = defaultdict(int)
    
    for metric in metrics:
        hour_key = metric.timestamp.hour
        day_key = metric.timestamp.strftime("%A")
        hourly_distribution[hour_key] += 1
        daily_distribution[day_key] += 1
    
    # Calculate peak error times
    peak_hour = max(hourly_distribution.items(), key=lambda x: x[1]) if hourly_distribution else (0, 0)
    peak_day = max(daily_distribution.items(), key=lambda x: x[1]) if daily_distribution else ("Unknown", 0)
    
    return {
        'hourly_distribution': dict(hourly_distribution),
        'daily_distribution': dict(daily_distribution),
        'peak_error_hour': peak_hour[0],
        'peak_error_day': peak_day[0],
        'total_errors_analyzed': len(metrics)
    }

def _analyze_error_correlations(metrics: List[ErrorMetric]) -> Dict[str, Any]:
    """Analyze correlations between different error types."""
    if len(metrics) < 2:
        return {}
    
    # Group errors by time windows (5-minute buckets)
    time_buckets = defaultdict(lambda: defaultdict(int))
    
    for metric in metrics:
        bucket_key = metric.timestamp.replace(minute=(metric.timestamp.minute // 5) * 5, second=0, microsecond=0)
        time_buckets[bucket_key][metric.error_code] += 1
    
    # Find error codes that frequently occur together
    correlations = defaultdict(lambda: defaultdict(int))
    
    for bucket_errors in time_buckets.values():
        error_codes = list(bucket_errors.keys())
        for i, error1 in enumerate(error_codes):
            for error2 in error_codes[i+1:]:
                correlations[error1][error2] += 1
                correlations[error2][error1] += 1
    
    # Convert to regular dict and include only significant correlations
    significant_correlations = {}
    for error1, related_errors in correlations.items():
        significant = {error2: count for error2, count in related_errors.items() if count >= 2}
        if significant:
            significant_correlations[error1] = significant
    
    return {
        'correlated_errors': significant_correlations,
        'time_bucket_analysis': {
            'total_buckets': len(time_buckets),
            'avg_errors_per_bucket': sum(sum(bucket.values()) for bucket in time_buckets.values()) / len(time_buckets) if time_buckets else 0
        }
    }

def _analyze_recovery_effectiveness(metrics: List[ErrorMetric]) -> Dict[str, Any]:
    """Analyze effectiveness of recovery strategies."""
    recovery_stats = defaultdict(lambda: {'attempted': 0, 'successful': 0})
    
    for metric in metrics:
        if metric.recovery_attempted:
            key = f"{metric.category.value}_{metric.error_code}"
            recovery_stats[key]['attempted'] += 1
            if metric.recovery_successful:
                recovery_stats[key]['successful'] += 1
    
    # Calculate success rates
    recovery_effectiveness = {}
    for key, stats in recovery_stats.items():
        success_rate = stats['successful'] / stats['attempted'] if stats['attempted'] > 0 else 0
        recovery_effectiveness[key] = {
            'attempts': stats['attempted'],
            'successes': stats['successful'],
            'success_rate': success_rate
        }
    
    overall_attempts = sum(stats['attempted'] for stats in recovery_stats.values())
    overall_successes = sum(stats['successful'] for stats in recovery_stats.values())
    overall_success_rate = overall_successes / overall_attempts if overall_attempts > 0 else 0
    
    return {
        'by_error_type': recovery_effectiveness,
        'overall_stats': {
            'total_attempts': overall_attempts,
            'total_successes': overall_successes,
            'overall_success_rate': overall_success_rate
        }
    }

def _get_circuit_breaker_status() -> Dict[str, Any]:
    """Get status of all circuit breakers."""
    status = {}
    for name, breaker in circuit_breakers.items():
        status[name] = {
            'state': breaker.state.value,
            'failure_count': breaker.failure_count,
            'success_count': breaker.success_count,
            'last_failure_time': breaker.last_failure_time
        }
    return status

# --- Health Check Functions ---
def get_system_health_metrics() -> Dict[str, Any]:
    """Get comprehensive system health metrics."""
    error_rate_1h = metrics_collector.get_error_rate(60)
    error_rate_24h = metrics_collector.get_error_rate(24 * 60)
    
    # Determine health status
    health_status = "HEALTHY"
    if error_rate_1h > 10:  # More than 10 errors per minute
        health_status = "CRITICAL"
    elif error_rate_1h > 5:  # More than 5 errors per minute
        health_status = "DEGRADED"
    elif error_rate_1h > 1:  # More than 1 error per minute
        health_status = "WARNING"
    
    # Check circuit breaker states
    open_circuit_breakers = [name for name, breaker in circuit_breakers.items() 
                           if breaker.state == CircuitBreakerState.OPEN]
    
    if open_circuit_breakers:
        health_status = "DEGRADED" if health_status == "HEALTHY" else health_status
    
    return {
        'overall_health': health_status,
        'error_rates': {
            'last_hour': error_rate_1h,
            'last_24_hours': error_rate_24h
        },
        'circuit_breakers': {
            'total': len(circuit_breakers),
            'open': len(open_circuit_breakers),
            'open_breakers': open_circuit_breakers
        },
        'top_errors': metrics_collector.get_top_errors(5),
        'recovery_success_rate': metrics_collector._get_recovery_success_rate(),
        'timestamp': datetime.utcnow().isoformat()
    }

# --- Export Functions for Monitoring Dashboard ---
def export_metrics_for_dashboard(format_type: str = "json") -> str:
    """Export metrics in format suitable for monitoring dashboard."""
    health_metrics = get_system_health_metrics()
    error_patterns = metrics_collector.get_error_patterns()
    
    dashboard_data = {
        'system_health': health_metrics,
        'error_patterns': error_patterns,
        'circuit_breaker_status': _get_circuit_breaker_status(),
        'export_timestamp': datetime.utcnow().isoformat(),
        'metrics_collection_period': '24_hours'
    }
    
    if format_type.lower() == "json":
        return json.dumps(dashboard_data, indent=2, default=str)
    else:
        # Could add other formats like CSV, XML, etc.
        return str(dashboard_data)

# --- Legacy Recovery Functions (Enhanced) ---
def attempt_recovery_db_connection(operation_callable, *args, **kwargs):
    """Enhanced DB recovery with circuit breaker pattern."""
    component_name = "database_operations"
    circuit_breaker = get_circuit_breaker(component_name)
    
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            return circuit_breaker.call(operation_callable, *args, **kwargs)
        except CircuitBreakerOpenError:
            logger.warning(f"Database circuit breaker is open, not retrying")
            raise
        except DatabaseError as db_err:
            logger.warning(f"DB operation failed (attempt {attempt + 1}/{max_retries}): {db_err}")
            
            if attempt == max_retries - 1:
                logger.error(f"DB operation failed after {max_retries} attempts")
                # Try automated recovery
                recovery_successful = attempt_automated_recovery(db_err)
                if not recovery_successful:
                    raise
                else:
                    # Retry once more after successful recovery
                    try:
                        return circuit_breaker.call(operation_callable, *args, **kwargs)
                    except Exception:
                        raise db_err
            
            time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Non-recoverable error during DB operation: {e}")
            raise

# --- Performance Monitoring ---
class PerformanceMetric:
    """Track performance metrics related to error handling."""
    
    def __init__(self):
        self.operation_times = defaultdict(list)
        self.lock = threading.RLock()
    
    def record_operation(self, operation_name: str, duration: float, success: bool):
        """Record operation performance."""
        with self.lock:
            self.operation_times[operation_name].append({
                'duration': duration,
                'success': success,
                'timestamp': datetime.utcnow()
            })
            
            # Keep only last 1000 records per operation
            if len(self.operation_times[operation_name]) > 1000:
                self.operation_times[operation_name] = self.operation_times[operation_name][-1000:]
    
    def get_performance_stats(self, operation_name: str = None) -> Dict[str, Any]:
        """Get performance statistics."""
        with self.lock:
            if operation_name:
                operations = {operation_name: self.operation_times.get(operation_name, [])}
            else:
                operations = self.operation_times
            
            stats = {}
            for op_name, records in operations.items():
                if not records:
                    continue
                
                durations = [r['duration'] for r in records]
                successes = [r['success'] for r in records]
                
                stats[op_name] = {
                    'total_operations': len(records),
                    'success_rate': sum(successes) / len(successes) if successes else 0,
                    'avg_duration': sum(durations) / len(durations) if durations else 0,
                    'min_duration': min(durations) if durations else 0,
                    'max_duration': max(durations) if durations else 0,
                    'last_24h_operations': len([r for r in records if r['timestamp'] > datetime.utcnow() - timedelta(hours=24)])
                }
            
            return stats

performance_monitor = PerformanceMetric()

if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    setup_logging()
    set_correlation_id("ERR_HANDLER_ENHANCED_TEST")

    # Test enhanced error handling features
    logger.info("Testing enhanced error handler features...")
    
    # Test metrics collection
    try:
        raise ValidationError("Test validation error", claim_id="TEST001", field="facility_id", rule_id="VAL_FAC_001")
    except ValidationError as ve:
        log_app_exception(ve)
        logger.info("Validation error recorded")
    
    # Test circuit breaker
    test_breaker = get_circuit_breaker("test_component")
    
    def failing_operation():
        raise DatabaseError("Simulated DB failure", component="TestDB")
    
    # Test circuit breaker opening
    for i in range(6):  # Should open after 5 failures
        try:
            test_breaker.call(failing_operation)
        except (DatabaseError, CircuitBreakerOpenError) as e:
            logger.info(f"Attempt {i+1}: {type(e).__name__}")
    
    # Test recovery strategies
    db_error = DatabaseError("Connection lost", component="TestDB")
    recovery_result = attempt_automated_recovery(db_error)
    logger.info(f"Recovery attempt result: {recovery_result}")
    
    # Test metrics analysis
    analysis_data = get_error_analysis_data(1)  # Last 1 hour
    logger.info(f"Error analysis patterns: {len(analysis_data['patterns'])}")
    
    # Test health metrics
    health = get_system_health_metrics()
    logger.info(f"System health: {health['overall_health']}")
    
    # Test performance monitoring
    performance_monitor.record_operation("test_operation", 0.5, True)
    performance_monitor.record_operation("test_operation", 1.2, False)
    perf_stats = performance_monitor.get_performance_stats()
    logger.info(f"Performance stats: {perf_stats}")
    
    # Export metrics for dashboard
    dashboard_export = export_metrics_for_dashboard()
    logger.info("Dashboard export generated successfully")
    
    logger.info("Enhanced error handler test completed successfully")