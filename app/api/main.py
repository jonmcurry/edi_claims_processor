# app/api/main.py
"""
Main FastAPI application setup for the EDI Claims Processor API.
This API serves the Failed Claims UI and potentially other external interactions.
"""
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware # For Cross-Origin Resource Sharing

# Assuming endpoints are in app.api.endpoints and logging_config in app.utils
from app.api import endpoints as api_endpoints # Relative import
from app.utils.logging_config import setup_logging, get_logger, set_correlation_id, get_correlation_id
from app.utils.error_handler import APIError, AppException
from app.database.connection_manager import init_database_connections, dispose_engines

# Setup logging first
setup_logging() # Reads from config/config.yaml by default
logger = get_logger('app.api.main')


# --- FastAPI App Initialization ---
app = FastAPI(
    title="EDI Claims Processor API",
    description="API for managing and viewing EDI claims processing, focusing on failed claims and analytics.",
    version="1.0.0",
    # docs_url="/docs", # Default
    # redoc_url="/redoc" # Default
)

# --- Middleware ---
# CORS Middleware (adjust origins as needed for your UI)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Or specify your frontend URL, e.g., "http://localhost:3000"
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    """Adds a correlation ID to each request for logging and tracing."""
    # Try to get existing correlation ID from headers (e.g., X-Correlation-ID)
    # If multiple microservices, this header might be passed along.
    cid_header = request.headers.get("X-Correlation-ID")
    cid = set_correlation_id(cid_header) # set_correlation_id generates one if None
    
    logger.info(f"Incoming request: {request.method} {request.url.path} (CID: {cid})")
    
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid # Add CID to response headers
    logger.info(f"Outgoing response: {response.status_code} for {request.url.path} (CID: {cid})")
    return response


# --- Event Handlers (Startup and Shutdown) ---
@app.on_event("startup")
async def startup_event():
    """Application startup event: Initialize database connections."""
    set_correlation_id("API_STARTUP")
    logger.info("FastAPI application startup...")
    try:
        init_database_connections() # From connection_manager
        logger.info("Database connections initialized for API.")
    except Exception as e:
        logger.critical(f"Failed to initialize database connections during API startup: {e}", exc_info=True)
        # Depending on severity, might want to prevent app from starting or enter a degraded mode.
    logger.info("FastAPI application startup complete.")

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event: Dispose of database engine pools."""
    set_correlation_id("API_SHUTDOWN")
    logger.info("FastAPI application shutdown...")
    dispose_engines() # From connection_manager
    logger.info("Database engines disposed. FastAPI application shutdown complete.")


# --- Custom Exception Handlers ---
@app.exception_handler(AppException) # Handles our custom base app exception
async def app_exception_handler(request: Request, exc: AppException):
    logger.error(
        f"AppException caught by API: {exc.message} (Code: {exc.error_code}, Category: {exc.category}, CID: {get_correlation_id()})",
        exc_info=False, # exc_info=True if you want full traceback for AppExceptions too
        extra={"error_details": exc.details, "error_code": exc.error_code, "error_category": exc.category}
    )
    status_code = 500 # Default
    if isinstance(exc, APIError): # Our specific APIError might have a status_code
        status_code = exc.status_code
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": exc.error_code,
                "message": exc.message,
                "category": exc.category,
                "details": exc.details,
                "correlation_id": get_correlation_id()
            }
        },
    )

@app.exception_handler(RequestValidationError) # Handles Pydantic validation errors
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(
        f"Request validation error: {exc.errors()} (CID: {get_correlation_id()})",
        extra={"validation_errors": exc.errors()}
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": "REQUEST_VALIDATION_ERROR",
                "message": "Invalid request parameters.",
                "details": exc.errors(), # Pydantic's detailed errors
                "correlation_id": get_correlation_id()
            }
        },
    )

@app.exception_handler(Exception) # Generic fallback for unexpected errors
async def generic_exception_handler(request: Request, exc: Exception):
    cid = get_correlation_id()
    logger.critical(
        f"Unhandled generic exception caught by API: {str(exc)} (CID: {cid})",
        exc_info=True # Log full traceback for unexpected errors
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "code": "UNEXPECTED_SERVER_ERROR",
                "message": "An unexpected internal server error occurred.",
                "details": {"exception_type": type(exc).__name__, "message": str(exc)},
                "correlation_id": cid
            }
        },
    )

# --- Include API Routers ---
app.include_router(api_endpoints.router, prefix="/api/v1") # Prefix all routes from endpoints.py

# --- Root Endpoint (Optional) ---
@app.get("/", tags=["Root"])
async def read_root():
    """Root endpoint providing basic API information."""
    cid = set_correlation_id("API_ROOT_GET")
    logger.info(f"GET / called (CID: {cid})")
    return {
        "message": "Welcome to the EDI Claims Processor API!",
        "version": app.version,
        "docs": app.docs_url,
        "redoc": app.redoc_url,
        "correlation_id": cid
    }

# To run this FastAPI app (example, usually done via app/main.py or a run script):
# if __name__ == "__main__":
#     import uvicorn
#     # This __main__ block is more for testing the API file directly.
#     # The main application entry point (app/main.py) would handle starting this if needed as a service.
#     logger.info("Starting FastAPI server directly from api/main.py for testing...")
#     uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")