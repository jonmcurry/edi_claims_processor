# app/api/main.py
"""
Main FastAPI application setup for the EDI Claims Processor API.
This API serves the Failed Claims UI and potentially other external interactions.
Optimized version with rate limiting, async middleware, response caching, and monitoring.
"""
import time
import sys
import asyncio
from typing import Dict, Any
from datetime import datetime, timedelta

from fastapi import FastAPI, Request, Response, status, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

# Third-party middleware and dependencies
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    from slowapi.middleware import SlowAPIMiddleware
    RATE_LIMITING_AVAILABLE = True
except ImportError:
    RATE_LIMITING_AVAILABLE = False

import redis.asyncio as redis
from contextlib import asynccontextmanager
import json
import hashlib

# Internal imports
from app.api import endpoints as api_endpoints
from app.utils.logging_config import setup_logging, get_logger, set_correlation_id, get_correlation_id
from app.utils.error_handler import APIError, AppException
from app.database.connection_manager import init_database_connections, dispose_engines, CONFIG as APP_CONFIG

# Setup logging first
setup_logging()
logger = get_logger('app.api.main')

# --- Application Lifespan Management ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages application startup and shutdown events."""
    startup_cid = set_correlation_id("API_STARTUP")
    logger.info(f"[{startup_cid}] FastAPI application startup...")
    
    try:
        # Initialize database connections
        init_database_connections()
        logger.info("Database connections initialized for API.")
        
        # Initialize Redis cache if configured
        if app.state.cache_enabled:
            try:
                app.state.redis_client = redis.from_url(
                    app.state.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=30
                )
                # Test connection
                await app.state.redis_client.ping()
                logger.info(f"Redis cache connected: {app.state.redis_url}")
            except Exception as e:
                logger.warning(f"Redis cache connection failed: {e}. Disabling cache.")
                app.state.cache_enabled = False
                app.state.redis_client = None
        
        # Initialize metrics collection
        app.state.metrics = {
            "requests_total": 0,
            "requests_by_endpoint": {},
            "response_times": [],
            "errors_total": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "startup_time": datetime.utcnow()
        }
        
        logger.info("FastAPI application startup complete.")
        yield
        
    except Exception as e:
        logger.critical(f"Failed to initialize application during startup: {e}", exc_info=True)
        raise
    finally:
        # Shutdown
        shutdown_cid = set_correlation_id("API_SHUTDOWN")
        logger.info(f"[{shutdown_cid}] FastAPI application shutdown...")
        
        # Close Redis connection
        if hasattr(app.state, 'redis_client') and app.state.redis_client:
            try:
                await app.state.redis_client.close()
                logger.info("Redis connection closed.")
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")
        
        # Dispose database engines
        dispose_engines()
        logger.info("Database engines disposed.")
        
        # Log final metrics
        if hasattr(app.state, 'metrics'):
            uptime = datetime.utcnow() - app.state.metrics["startup_time"]
            logger.info(f"API session metrics - Uptime: {uptime}, "
                       f"Total requests: {app.state.metrics['requests_total']}, "
                       f"Total errors: {app.state.metrics['errors_total']}, "
                       f"Cache hit ratio: {app.state.metrics['cache_hits']}/{app.state.metrics['cache_hits'] + app.state.metrics['cache_misses']}")
        
        logger.info("FastAPI application shutdown complete.")

# --- Rate Limiter Setup ---
if RATE_LIMITING_AVAILABLE:
    limiter = Limiter(key_func=get_remote_address)
else:
    limiter = None
    logger.warning("slowapi not available. Rate limiting disabled. Install with: pip install slowapi")

# --- FastAPI App Initialization ---
app_config = APP_CONFIG.get('api', {})
cache_config = APP_CONFIG.get('caching', {})

app = FastAPI(
    title="EDI Claims Processor API",
    description="Optimized API for managing and viewing EDI claims processing, focusing on failed claims and analytics.",
    version="1.0.0",
    lifespan=lifespan,
    # Enable automatic API documentation
    docs_url="/docs",
    openapi_url="/openapi.json"
)

# --- Application State Configuration ---
app.state.cache_enabled = cache_config.get('api_cache_enabled', True)
app.state.redis_url = cache_config.get('redis_url', 'redis://localhost:6379/0')
app.state.cache_ttl = cache_config.get('api_cache_ttl_seconds', 300)  # 5 minutes default

# --- Middleware Setup ---

# Security Middleware - Trusted Hosts
trusted_hosts = app_config.get('trusted_hosts', ['localhost', '127.0.0.1', '*'])
if '*' not in trusted_hosts:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=trusted_hosts)

# Compression Middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=app_config.get('cors_origins', ["*"]),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Correlation-ID", "X-Cache-Status", "X-Response-Time"]
)

# Rate Limiting Middleware
if RATE_LIMITING_AVAILABLE:
    app.state.limiter = limiter
    app.add_middleware(SlowAPIMiddleware)

# --- Custom Async Middleware ---

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    """Adds correlation ID and tracks request metrics."""
    start_time = time.time()
    
    # Set correlation ID
    cid_header = request.headers.get("X-Correlation-ID")
    cid = set_correlation_id(cid_header)
    
    # Track request
    app.state.metrics["requests_total"] += 1
    endpoint = f"{request.method} {request.url.path}"
    app.state.metrics["requests_by_endpoint"][endpoint] = app.state.metrics["requests_by_endpoint"].get(endpoint, 0) + 1
    
    logger.info(f"[{cid}] Incoming request: {endpoint} from {request.client.host if request.client else 'unknown'}")
    
    try:
        response = await call_next(request)
        
        # Calculate response time
        process_time = time.time() - start_time
        app.state.metrics["response_times"].append(process_time)
        
        # Keep only last 1000 response times for memory management
        if len(app.state.metrics["response_times"]) > 1000:
            app.state.metrics["response_times"] = app.state.metrics["response_times"][-1000:]
        
        # Add headers
        response.headers["X-Correlation-ID"] = cid
        response.headers["X-Response-Time"] = f"{process_time:.4f}s"
        
        logger.info(f"[{cid}] Response: {response.status_code} for {endpoint} in {process_time:.4f}s")
        return response
        
    except Exception as e:
        app.state.metrics["errors_total"] += 1
        process_time = time.time() - start_time
        logger.error(f"[{cid}] Request failed: {endpoint} in {process_time:.4f}s - {str(e)}")
        raise

@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    """Adds security headers to responses."""
    response = await call_next(request)
    
    # Security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    
    # Don't cache sensitive endpoints
    if any(sensitive in request.url.path for sensitive in ['/failed-claims', '/analytics']):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    
    return response

# --- Caching Utilities ---

async def get_cache_key(request: Request) -> str:
    """Generate cache key for request."""
    # Include method, path, and sorted query parameters
    query_params = str(sorted(request.query_params.items()))
    cache_data = f"{request.method}:{request.url.path}:{query_params}"
    return f"api_cache:{hashlib.md5(cache_data.encode()).hexdigest()}"

async def get_cached_response(request: Request) -> Dict[str, Any] | None:
    """Get cached response if available."""
    if not app.state.cache_enabled or not hasattr(app.state, 'redis_client') or not app.state.redis_client:
        return None
    
    try:
        cache_key = await get_cache_key(request)
        cached_data = await app.state.redis_client.get(cache_key)
        
        if cached_data:
            app.state.metrics["cache_hits"] += 1
            return json.loads(cached_data)
        else:
            app.state.metrics["cache_misses"] += 1
            return None
            
    except Exception as e:
        logger.warning(f"Cache read error: {e}")
        app.state.metrics["cache_misses"] += 1
        return None

async def set_cached_response(request: Request, response_data: Dict[str, Any], ttl: int = None):
    """Cache response data."""
    if not app.state.cache_enabled or not hasattr(app.state, 'redis_client') or not app.state.redis_client:
        return
    
    try:
        cache_key = await get_cache_key(request)
        ttl = ttl or app.state.cache_ttl
        await app.state.redis_client.setex(
            cache_key, 
            ttl, 
            json.dumps(response_data, default=str)  # default=str handles datetime serialization
        )
    except Exception as e:
        logger.warning(f"Cache write error: {e}")

@app.middleware("http")
async def caching_middleware(request: Request, call_next):
    """Response caching middleware for GET requests."""
    cid = get_correlation_id()
    
    # Only cache GET requests and specific endpoints
    cacheable_endpoints = ['/failed-claims', '/analytics', '/status']
    if (request.method != "GET" or 
        not any(endpoint in request.url.path for endpoint in cacheable_endpoints)):
        return await call_next(request)
    
    # Check cache
    cached_response = await get_cached_response(request)
    if cached_response:
        logger.debug(f"[{cid}] Cache HIT for {request.url.path}")
        response = JSONResponse(content=cached_response)
        response.headers["X-Cache-Status"] = "HIT"
        response.headers["X-Correlation-ID"] = cid
        return response
    
    # Process request
    logger.debug(f"[{cid}] Cache MISS for {request.url.path}")
    response = await call_next(request)
    
    # Cache successful responses
    if response.status_code == 200 and hasattr(response, 'body'):
        try:
            response_body = response.body.decode('utf-8')
            response_data = json.loads(response_body)
            
            # Cache for different TTLs based on endpoint
            ttl = app.state.cache_ttl
            if 'analytics' in request.url.path:
                ttl = 600  # 10 minutes for analytics
            elif 'status' in request.url.path:
                ttl = 60   # 1 minute for status
            
            await set_cached_response(request, response_data, ttl)
            response.headers["X-Cache-Status"] = "MISS"
            
        except Exception as e:
            logger.warning(f"[{cid}] Failed to cache response: {e}")
            response.headers["X-Cache-Status"] = "ERROR"
    
    return response

# --- Rate Limiting Error Handler ---
if RATE_LIMITING_AVAILABLE:
    @app.exception_handler(RateLimitExceeded)
    async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
        cid = get_correlation_id()
        logger.warning(f"[{cid}] Rate limit exceeded for {request.client.host if request.client else 'unknown'}: {request.url.path}")
        
        response = JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={
                "error": {
                    "code": "RATE_LIMIT_EXCEEDED",
                    "message": f"Rate limit exceeded: {exc.detail}",
                    "retry_after": getattr(exc, 'retry_after', 60),
                    "correlation_id": cid
                }
            }
        )
        response.headers["Retry-After"] = str(getattr(exc, 'retry_after', 60))
        return response

# --- Custom Exception Handlers ---
@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException):
    cid = get_correlation_id()
    logger.error(
        f"[{cid}] AppException caught by API: {exc.message} (Code: {exc.error_code}, Category: {exc.category})",
        exc_info=isinstance(exc, (APIError,)),
        extra={"error_details": exc.details, "error_code": exc.error_code, "error_category": exc.category}
    )
    
    status_code = 500
    if isinstance(exc, APIError):
        status_code = exc.status_code
    
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": exc.error_code,
                "message": exc.message,
                "category": exc.category,
                "details": exc.details,
                "correlation_id": cid,
                "timestamp": datetime.utcnow().isoformat()
            }
        },
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    cid = get_correlation_id()
    logger.warning(
        f"[{cid}] Request validation error: {exc.errors()}",
        extra={"validation_errors": exc.errors(), "request_path": request.url.path}
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": "REQUEST_VALIDATION_ERROR",
                "message": "Invalid request parameters.",
                "details": exc.errors(),
                "correlation_id": cid,
                "timestamp": datetime.utcnow().isoformat()
            }
        },
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    cid = get_correlation_id()
    app.state.metrics["errors_total"] += 1
    logger.critical(
        f"[{cid}] Unhandled generic exception caught by API: {str(exc)}",
        exc_info=True,
        extra={"request_path": request.url.path, "request_method": request.method}
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": {
                "code": "UNEXPECTED_SERVER_ERROR",
                "message": "An unexpected internal server error occurred.",
                "details": {"exception_type": type(exc).__name__},
                "correlation_id": cid,
                "timestamp": datetime.utcnow().isoformat()
            }
        },
    )

# --- Health Check and Monitoring Endpoints ---

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint with detailed status."""
    cid = set_correlation_id("HEALTH_CHECK")
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": app.version,
        "correlation_id": cid,
        "checks": {}
    }
    
    # Database connectivity check
    try:
        from app.database.connection_manager import check_postgres_connection, check_sqlserver_connection
        health_status["checks"]["postgres"] = "healthy" if check_postgres_connection() else "unhealthy"
        health_status["checks"]["sqlserver"] = "healthy" if check_sqlserver_connection() else "unhealthy"
    except Exception as e:
        health_status["checks"]["database"] = f"error: {str(e)}"
        health_status["status"] = "degraded"
    
    # Cache connectivity check
    if app.state.cache_enabled and hasattr(app.state, 'redis_client') and app.state.redis_client:
        try:
            await app.state.redis_client.ping()
            health_status["checks"]["cache"] = "healthy"
        except Exception:
            health_status["checks"]["cache"] = "unhealthy"
            health_status["status"] = "degraded"
    else:
        health_status["checks"]["cache"] = "disabled"
    
    # Overall status determination
    if any(check == "unhealthy" for check in health_status["checks"].values()):
        health_status["status"] = "unhealthy"
    
    status_code = 200 if health_status["status"] != "unhealthy" else 503
    return JSONResponse(content=health_status, status_code=status_code)

@app.get("/metrics", tags=["Monitoring"])
@limiter.limit("10/minute") if RATE_LIMITING_AVAILABLE else lambda: None
async def get_metrics(request: Request):
    """API metrics endpoint."""
    cid = set_correlation_id("METRICS_REQUEST")
    
    uptime = datetime.utcnow() - app.state.metrics["startup_time"]
    response_times = app.state.metrics["response_times"]
    
    metrics = {
        "uptime_seconds": uptime.total_seconds(),
        "requests": {
            "total": app.state.metrics["requests_total"],
            "by_endpoint": app.state.metrics["requests_by_endpoint"],
            "errors_total": app.state.metrics["errors_total"]
        },
        "performance": {
            "avg_response_time": sum(response_times) / len(response_times) if response_times else 0,
            "min_response_time": min(response_times) if response_times else 0,
            "max_response_time": max(response_times) if response_times else 0,
            "samples_count": len(response_times)
        },
        "cache": {
            "enabled": app.state.cache_enabled,
            "hits": app.state.metrics["cache_hits"],
            "misses": app.state.metrics["cache_misses"],
            "hit_ratio": app.state.metrics["cache_hits"] / (app.state.metrics["cache_hits"] + app.state.metrics["cache_misses"]) if (app.state.metrics["cache_hits"] + app.state.metrics["cache_misses"]) > 0 else 0
        },
        "timestamp": datetime.utcnow().isoformat(),
        "correlation_id": cid
    }
    
    return metrics

# --- Cache Management Endpoints ---

@app.post("/cache/clear", tags=["Cache Management"])
@limiter.limit("5/minute") if RATE_LIMITING_AVAILABLE else lambda: None
async def clear_cache(request: Request):
    """Clear API response cache."""
    cid = set_correlation_id("CACHE_CLEAR")
    
    if not app.state.cache_enabled or not hasattr(app.state, 'redis_client') or not app.state.redis_client:
        raise HTTPException(status_code=503, detail="Cache not available")
    
    try:
        # Clear only API cache keys
        keys = await app.state.redis_client.keys("api_cache:*")
        if keys:
            await app.state.redis_client.delete(*keys)
            cleared_count = len(keys)
        else:
            cleared_count = 0
        
        logger.info(f"[{cid}] Cleared {cleared_count} cache entries")
        
        return {
            "message": f"Successfully cleared {cleared_count} cache entries",
            "correlation_id": cid,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"[{cid}] Failed to clear cache: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}")

# --- Include API Routers ---
app.include_router(api_endpoints.router, prefix="/api/v1")

# --- Root Endpoint ---
@app.get("/", tags=["Root"])
async def read_root():
    """Root endpoint providing API information and status."""
    cid = set_correlation_id("API_ROOT_GET")
    
    uptime = datetime.utcnow() - app.state.metrics["startup_time"] if hasattr(app.state, 'metrics') else timedelta(0)
    
    return {
        "message": "Welcome to the EDI Claims Processor API!",
        "version": app.version,
        "status": "operational",
        "uptime_seconds": uptime.total_seconds(),
        "features": {
            "rate_limiting": RATE_LIMITING_AVAILABLE,
            "response_caching": app.state.cache_enabled,
            "monitoring": True,
            "async_processing": True
        },
        "endpoints": {
            "documentation": "/docs",
            "metrics": "/metrics",
            "openapi": "/openapi.json"
        },
        "correlation_id": cid,
        "timestamp": datetime.utcnow().isoformat()
    }

# --- Development Server (Optional) ---
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server directly from api/main.py for development...")
    uvicorn.run(
        "app.api.main:app",
        host=app_config.get('host', '0.0.0.0'),
        port=app_config.get('port', 8000),
        reload=True,  # Enable auto-reload for development
        log_level="info",
        access_log=True
    )
 