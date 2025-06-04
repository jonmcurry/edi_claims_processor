# app/utils/error_handler.py
"""
Defines custom application exceptions and standardized error handling utilities.
Includes enhanced categorization for UI filtering and recovery strategies.
"""
from app.utils.logging_config import get_logger

logger = get_logger('app.error_handler')

# --- Custom Exception Base Class ---
class AppException(Exception):
    """Base class for custom application exceptions."""
    def __init__(self, message: str, error_code: str = "APP_ERROR", details: dict = None, category: str = "General"):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details if details is not None else {}
        self.category = category # For UI filtering (e.g., "Validation", "Database", "Parsing", "ML")

    def __str__(self):
        return f"{self.error_code} ({self.category}): {self.message} {self.details if self.details else ''}"

# --- Specific Exception Classes ---

# Configuration Errors
class ConfigError(AppException):
    """For errors related to application configuration."""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, error_code="CONFIG_ERROR", details=details, category="Configuration")

# Database Errors
class DatabaseError(AppException):
    """For errors related to database operations."""
    def __init__(self, message: str, original_exception: Exception = None, details: dict = None):
        super().__init__(message, error_code="DB_ERROR", details=details, category="Database")
        self.original_exception = original_exception

class StagingDBError(DatabaseError):
    """For errors specific to the PostgreSQL staging database."""
    def __init__(self, message: str, original_exception: Exception = None, details: dict = None):
        super().__init__(message, original_exception=original_exception, details=details)
        self.error_code = "STAGING_DB_ERROR"
        self.category = "StagingDatabase"

class ProductionDBError(DatabaseError):
    """For errors specific to the SQL Server production database."""
    def __init__(self, message: str, original_exception: Exception = None, details: dict = None):
        super().__init__(message, original_exception=original_exception, details=details)
        self.error_code = "PROD_DB_ERROR"
        self.category = "ProductionDatabase"

# EDI Parsing Errors
class EDIParserError(AppException):
    """For errors encountered during EDI file parsing."""
    def __init__(self, message: str, claim_id: str = None, segment: str = None, details: dict = None):
        super().__init__(message, error_code="EDI_PARSE_ERROR", details=details, category="Parsing")
        self.claim_id = claim_id
        self.segment = segment
        if self.claim_id: self.details['claim_id'] = self.claim_id
        if self.segment: self.details['segment'] = self.segment


# Validation Errors
class ValidationError(AppException):
    """For errors during data validation (rules engine)."""
    def __init__(self, message: str, claim_id: str = None, field: str = None, rule_id: str = None, details: dict = None):
        super().__init__(message, error_code="VALIDATION_ERROR", details=details, category="Validation")
        self.claim_id = claim_id
        self.field = field
        self.rule_id = rule_id
        if self.claim_id: self.details['claim_id'] = self.claim_id
        if self.field: self.details['field'] = self.field
        if self.rule_id: self.details['rule_id'] = self.rule_id

# ML Model Errors
class MLModelError(AppException):
    """For errors related to the machine learning model (prediction, loading)."""
    def __init__(self, message: str, model_name: str = None, details: dict = None):
        super().__init__(message, error_code="ML_MODEL_ERROR", details=details, category="MachineLearning")
        self.model_name = model_name
        if self.model_name: self.details['model_name'] = self.model_name

# API Errors
class APIError(AppException):
    """For errors related to API operations (e.g., request validation, external service calls)."""
    def __init__(self, message: str, status_code: int = 500, details: dict = None):
        super().__init__(message, error_code="API_ERROR", details=details, category="API")
        self.status_code = status_code

class ExternalServiceError(APIError):
    """For errors when interacting with external services (e.g., claim repair AI)."""
    def __init__(self, message: str, service_name: str, status_code: int = 503, details: dict = None):
        super().__init__(message, status_code=status_code, details=details)
        self.error_code = "EXT_SERVICE_ERROR"
        self.category = "ExternalService"
        self.service_name = service_name
        self.details['service_name'] = self.service_name

# Caching Errors
class CacheError(AppException):
    """For errors related to caching operations."""
    def __init__(self, message: str, cache_key: str = None, details: dict = None):
        super().__init__(message, error_code="CACHE_ERROR", details=details, category="Caching")
        self.cache_key = cache_key
        if self.cache_key: self.details['cache_key'] = self.cache_key


# --- Error Handling Utilities ---

def log_app_exception(exc: AppException, level: str = "error", claim_id: str = None):
    """Logs an AppException with structured details."""
    log_method = getattr(logger, level.lower(), logger.error)
    
    log_message = f"AppException Caught: Code={exc.error_code}, Category={exc.category}, Message='{exc.message}'"
    if claim_id:
        log_message += f", ClaimID='{claim_id}'"
    
    log_details = exc.details.copy() # Make a copy to avoid modifying original
    if hasattr(exc, 'original_exception') and exc.original_exception:
        log_details['original_exception'] = str(exc.original_exception)
        # Consider logging traceback of original_exception if level is debug or error
        # logger.debug("Original exception traceback:", exc_info=exc.original_exception)

    log_method(log_message, extra={'error_details': log_details}, exc_info=isinstance(exc, DatabaseError))


def handle_exception(e: Exception, context: str = "General", claim_id: str = None, re_raise_as: AppException = None):
    """
    Handles a generic exception, logs it, and can optionally re-raise it as a specific AppException.
    """
    if isinstance(e, AppException):
        # If it's already an AppException, just log it appropriately
        log_app_exception(e, claim_id=claim_id or getattr(e, 'claim_id', None))
        if re_raise_as: # This path is less common, usually you'd raise specific AppExceptions directly
            raise re_raise_as from e
        raise e # Re-raise the original AppException
    else:
        # For unexpected/generic exceptions
        err_message = f"Unexpected error in {context}: {str(e)}"
        logger.error(err_message, exc_info=True, extra={'claim_id': claim_id} if claim_id else {})
        
        if re_raise_as:
            # Wrap the generic exception in the specified AppException type
            # This is useful to standardize errors from third-party libraries
            if issubclass(re_raise_as, DatabaseError):
                 raise re_raise_as(message=err_message, original_exception=e)
            else:
                 raise re_raise_as(message=err_message) from e
        else:
            # If no specific re-raise type, wrap in a generic AppException
            raise AppException(message=err_message, error_code="UNEXPECTED_ERROR", category=context) from e


# --- Recovery Strategies (Placeholders) ---
# These would be more complex and application-specific.

def attempt_recovery_db_connection(operation_callable, *args, **kwargs):
    """Placeholder for a simple retry mechanism for DB operations."""
    max_retries = 3
    retry_delay = 5 # seconds
    for attempt in range(max_retries):
        try:
            return operation_callable(*args, **kwargs)
        except DatabaseError as db_err: # Catch specific DB errors that might be recoverable
            logger.warning(f"DB operation failed (attempt {attempt + 1}/{max_retries}): {db_err}. Retrying in {retry_delay}s...")
            if attempt == max_retries - 1:
                logger.error(f"DB operation failed after {max_retries} attempts.")
                raise # Re-raise the last exception
            time.sleep(retry_delay)
        except Exception as e: # Non-recoverable or other errors
            logger.error(f"Non-recoverable error during DB operation: {e}")
            raise


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    setup_logging()
    set_correlation_id("ERR_HANDLER_TEST")

    # Example Usage
    try:
        # Simulate a config error
        # raise ConfigError("Missing 'database_url' in configuration.", details={"missing_key": "database_url"})
        
        # Simulate a validation error
        # raise ValidationError("Invalid Facility ID.", claim_id="C12345", field="facility_id", rule_id="FAC_VAL_001")

        # Simulate a generic error being handled
        def risky_operation():
            return 1 / 0 # Raises ZeroDivisionError
        
        # handle_exception(ZeroDivisionError("test division"), context="RiskyMath", claim_id="C789")
        handle_exception(ZeroDivisionError("test division"), context="RiskyMath", claim_id="C789", re_raise_as=AppException)

    except AppException as ae:
        log_app_exception(ae, level="critical")
        # For UI, you might return:
        # response_data = {"error": ae.message, "code": ae.error_code, "category": ae.category, "details": ae.details}
        # print(f"Caught AppException: {response_data}")
    except Exception as e:
        logger.critical(f"Caught unexpected exception at top level: {e}", exc_info=True)