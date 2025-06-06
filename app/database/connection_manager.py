# app/database/connection_manager.py
"""
Production-grade database connection manager with advanced features:
- Async support for high-performance concurrent processing
- Health checks with automatic reconnection and circuit breaker
- Read/write splitting for routing queries to appropriate instances
- Connection monitoring and detailed metrics
- Sophisticated pool warming with health monitoring
"""
import yaml
import os
import time
import threading
import asyncio
import re
import importlib
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum
import statistics # For statistical calculations like mean

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.pool import QueuePool # NullPool can be used for serverless environments
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

# Import logging utilities
from app.utils.logging_config import get_logger, get_correlation_id # Assuming set_correlation_id is not used here
from app.utils.error_handler import ConfigError # For configuration related errors

logger = get_logger('app.database.connection_manager')

# --- Pre-flight Check for Database Drivers ---
try:
    importlib.import_module("psycopg2")
    logger.debug("psycopg2 driver found.")
except ImportError:
    logger.critical("FATAL: The 'psycopg2' library is not installed, but it is required for PostgreSQL connections. Please run 'pip install psycopg2-binary'.")
    # In a real application, you might want to exit here.
    # For this interactive context, we'll let SQLAlchemy raise the error to show the full stack.

try:
    importlib.import_module("pyodbc")
    logger.debug("pyodbc driver found.")
except ImportError:
    logger.critical("FATAL: The 'pyodbc' library is not installed, but it is required for SQL Server connections. Please run 'pip install pyodbc'.")


class DatabaseType(Enum):
    """Enumeration for database types."""
    POSTGRES = "postgres"
    SQLSERVER = "sqlserver"

class ConnectionStatus(Enum):
    """Enumeration for connection health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded" # Potentially slow or minor issues
    UNHEALTHY = "unhealthy" # Major issues, connection likely unusable
    UNKNOWN = "unknown" # Initial state or after prolonged failure

@dataclass
class ConnectionMetrics:
    """Tracks connection pool metrics and health statistics."""
    total_connections_configured: int = 0 # Max pool size
    current_pool_size: int = 0 # Current number of actual connections in pool
    active_connections: int = 0 # Connections currently checked out
    idle_connections: int = 0 # Connections checked in and available
    failed_connection_attempts: int = 0 # Attempts to establish a raw connection that failed
    successful_connection_attempts: int = 0
    total_queries_executed: int = 0
    avg_query_response_time_ms: float = 0.0
    last_health_check_ts: Optional[datetime] = None
    health_status: ConnectionStatus = ConnectionStatus.UNKNOWN
    consecutive_health_check_failures: int = 0
    query_response_times_ms: List[float] = field(default_factory=lambda: []) # Stores recent response times
    
    def update_query_response_time(self, response_time_ms: float):
        """Update query response time metrics with a rolling window."""
        self.query_response_times_ms.append(response_time_ms)
        # Keep only last 100 measurements for rolling average
        if len(self.query_response_times_ms) > 100:
            self.query_response_times_ms = self.query_response_times_ms[-100:]
        
        if self.query_response_times_ms:
            self.avg_query_response_time_ms = statistics.mean(self.query_response_times_ms)

@dataclass
class DatabaseConfig:
    """Configuration for a database connection."""
    name: str # Unique name for this connection config (e.g., "postgres_primary", "sqlserver_replica1")
    db_type: DatabaseType
    host: str
    port: int
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    trusted_connection: bool = False # Default to False, require explicit yes for SQL Server Windows Auth
    driver: Optional[str] = None
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30 # Seconds to wait for a connection from the pool
    pool_recycle: int = 1800 # Seconds after which a connection is recycled
    connect_timeout: int = 5 # Seconds for establishing a new connection
    health_check_interval_seconds: int = 30 # How often to run background health checks
    health_check_query: str = "SELECT 1"
    max_retries_on_failure: int = 3 # Retries for operations after a failure
    circuit_breaker_failure_threshold: int = 5 # Failures to open circuit
    circuit_breaker_recovery_timeout_seconds: int = 60 # Time before attempting recovery

class CircuitBreakerState(Enum):
    """States for the circuit breaker."""
    CLOSED = "CLOSED"      # Normal operation, requests allowed
    OPEN = "OPEN"          # Failing, requests rejected immediately
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered, limited requests allowed

class CircuitBreaker:
    """Circuit breaker pattern to prevent cascading failures with database connections."""
    def __init__(self, failure_threshold: int, recovery_timeout_seconds: int, name: str):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.name = name # For logging
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitBreakerState.CLOSED
        self._lock = threading.RLock() # Thread-safe access to state
    
    def can_execute(self) -> bool:
        """Check if an operation can be executed based on the circuit breaker state."""
        with self._lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True
            elif self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time and \
                   (datetime.now() - self.last_failure_time).total_seconds() > self.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    logger.info(f"CircuitBreaker '{self.name}' transitioning to HALF_OPEN state.")
                    return True # Allow one test request
                logger.debug(f"CircuitBreaker '{self.name}' is OPEN. Request blocked.")
                return False
            elif self.state == CircuitBreakerState.HALF_OPEN:
                logger.debug(f"CircuitBreaker '{self.name}' is HALF_OPEN. Allowing test request.")
                return True # Allow limited requests to test recovery
            return False # Should not happen
    
    def record_success(self):
        """Record a successful operation. Resets failure count and closes circuit if half-open."""
        with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
                logger.info(f"CircuitBreaker '{self.name}' transitioned to CLOSED after successful recovery.")
            self.failure_count = 0
            self.last_failure_time = None # Clear last failure time on success
    
    def record_failure(self):
        """Record a failed operation. Opens circuit if threshold is met."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            if self.state == CircuitBreakerState.HALF_OPEN:
                # If a request fails in half-open, re-open immediately
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"CircuitBreaker '{self.name}' transitioned back to OPEN from HALF_OPEN due to failure.")
            elif self.failure_count >= self.failure_threshold and self.state == CircuitBreakerState.CLOSED:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"CircuitBreaker '{self.name}' transitioned to OPEN after {self.failure_count} failures.")

class DatabaseConnection:
    """Manages a single database connection, its pool, health, and metrics."""
    def __init__(self, db_config: DatabaseConfig, is_replica: bool = False):
        self.config = db_config
        self.is_replica = is_replica
        self.metrics = ConnectionMetrics(total_connections_configured=db_config.pool_size + db_config.max_overflow)
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=db_config.circuit_breaker_failure_threshold,
            recovery_timeout_seconds=db_config.circuit_breaker_recovery_timeout_seconds,
            name=f"{db_config.name}-{'replica' if is_replica else 'primary'}"
        )
        
        self.engine = None
        self.async_engine = None
        self.session_factory: Optional[sessionmaker] = None
        self.async_session_factory: Optional[async_sessionmaker[AsyncSession]] = None
        self.scoped_session_factory: Optional[scoped_session] = None 
        
        self._health_check_thread: Optional[threading.Thread] = None
        self._health_check_stop_event = threading.Event()
        
        self._initialize_connection()
        self._start_health_monitoring()
    
    def _get_connection_url(self, for_async: bool = False) -> str:
        db_type_str = self.config.db_type.value
        driver_part = ""
        if for_async:
            driver_map = {DatabaseType.POSTGRES: "+asyncpg", DatabaseType.SQLSERVER: "+aioodbc"}
            driver_part = driver_map.get(self.config.db_type)
            if not driver_part:
                raise ConfigError(f"Async not configured for DB type: {db_type_str}")
        else: # Sync
            driver_map = {DatabaseType.POSTGRES: "+psycopg2", DatabaseType.SQLSERVER: "+pyodbc"}
            driver_part = driver_map.get(self.config.db_type)
            if not driver_part:
                raise ConfigError(f"Sync not configured for DB type: {db_type_str}")

        protocol = f"{db_type_str}{driver_part}"

        if self.config.db_type == DatabaseType.SQLSERVER and self.config.driver:
            odbc_connect_parts = [f"DRIVER={{{self.config.driver}}}", f"SERVER={self.config.host},{self.config.port}", f"DATABASE={self.config.database}"]
            if self.config.trusted_connection:
                odbc_connect_parts.append("Trusted_Connection=yes")
            elif self.config.user and self.config.password:
                odbc_connect_parts.append(f"UID={self.config.user}")
                odbc_connect_parts.append(f"PWD={self.config.password}")
            
            odbc_connect_str = ";".join(odbc_connect_parts)
            return f"{protocol}:///?odbc_connect={odbc_connect_str}"
        else:
            auth_part = f"{self.config.user}:{self.config.password}@" if self.config.user and self.config.password else ""
            return f"{protocol}://{auth_part}{self.config.host}:{self.config.port}/{self.config.database}"

    def _initialize_connection(self):
        logger.info(f"Initializing connection for {self.config.name} ({'replica' if self.is_replica else 'primary'})...")
        
        try:
            sync_url = self._get_connection_url(for_async=False)
            connect_args = {'connect_timeout': self.config.connect_timeout} if self.config.db_type == DatabaseType.POSTGRES else {}

            self.engine = create_engine(sync_url, poolclass=QueuePool, pool_size=self.config.pool_size,
                                      max_overflow=self.config.max_overflow, pool_timeout=self.config.pool_timeout,
                                      pool_recycle=self.config.pool_recycle, pool_pre_ping=True,
                                      connect_args=connect_args, echo=False)
            
            self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
            self.scoped_session_factory = scoped_session(self.session_factory)

            async_url = self._get_connection_url(for_async=True)
            self.async_engine = create_async_engine(async_url, pool_size=self.config.pool_size,
                                                    max_overflow=self.config.max_overflow,
                                                    pool_timeout=self.config.pool_timeout,
                                                    pool_recycle=self.config.pool_recycle,
                                                    pool_pre_ping=True, connect_args=connect_args,
                                                    echo=False)
            self.async_session_factory = async_sessionmaker(bind=self.async_engine, class_=AsyncSession,
                                                          autoflush=False, autocommit=False)
            
            self._add_event_listeners()
            self._warm_connection_pool()
            self.metrics.health_status = ConnectionStatus.UNKNOWN
            logger.info(f"Connection successfully initialized for {self.config.name}")

        except Exception as e:
            logger.error(f"Failed to initialize database connection for {self.config.name}: {e}", exc_info=True)
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            self.metrics.failed_connection_attempts += 1
    
    def _add_event_listeners(self):
        if not self.engine: return
        @event.listens_for(self.engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            context._query_start_time = time.perf_counter()
        
        @event.listens_for(self.engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            if hasattr(context, '_query_start_time'):
                query_time_ms = (time.perf_counter() - context._query_start_time) * 1000
                self.metrics.total_queries_executed += 1
                self.metrics.update_query_response_time(query_time_ms)
        
        @event.listens_for(self.engine.pool, "connect")
        def on_pool_connect(dbapi_connection, connection_record):
            self.metrics.successful_connection_attempts += 1
            logger.debug(f"New DBAPI connection established for {self.config.name}")

        @event.listens_for(self.engine.pool, "connect_error")
        def on_pool_connect_error(dbapi_connection, connection_record, error):
            self.metrics.failed_connection_attempts += 1
            logger.warning(f"DBAPI connection error for {self.config.name}: {error}")
            
        @event.listens_for(self.engine.pool, "checkout")
        def on_pool_checkout(dbapi_connection, connection_record, connection_proxy):
            self.metrics.active_connections += 1
            self.metrics.idle_connections = self.engine.pool.checkedin()
            self.metrics.current_pool_size = self.engine.pool.size()

        @event.listens_for(self.engine.pool, "checkin")
        def on_pool_checkin(dbapi_connection, connection_record):
            self.metrics.active_connections = max(0, self.metrics.active_connections - 1)
            self.metrics.idle_connections = self.engine.pool.checkedin()
            self.metrics.current_pool_size = self.engine.pool.size()

    def _warm_connection_pool(self):
        if not self.engine:
            logger.warning(f"Engine for {self.config.name} not initialized. Skipping pool warming.")
            return
        logger.info(f"Warming connection pool for {self.config.name} (target: {self.config.pool_size // 2} connections)...")
        warmed_connections = []
        try:
            num_to_warm = max(1, self.config.pool_size // 2) 
            for i in range(num_to_warm):
                try:
                    conn = self.engine.connect()
                    conn.execute(text(self.config.health_check_query))
                    warmed_connections.append(conn)
                    logger.debug(f"Warmed connection {i+1}/{num_to_warm} for {self.config.name}")
                except Exception as e:
                    logger.warning(f"Failed to warm connection {i+1} for {self.config.name}: {e}")
                    self.metrics.failed_connection_attempts += 1
            
            self.metrics.current_pool_size = self.engine.pool.size()
            self.metrics.idle_connections = self.engine.pool.checkedin()
            logger.info(f"Pool warming completed for {self.config.name}. Warmed {len(warmed_connections)} connections. Pool size: {self.metrics.current_pool_size}, Idle: {self.metrics.idle_connections}.")
        finally:
            for conn in warmed_connections:
                try:
                    conn.close()
                except Exception:
                    pass

    def _start_health_monitoring(self):
        if not self.engine:
            logger.warning(f"Engine for {self.config.name} not initialized. Skipping health monitoring.")
            return
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True,
            name=f"HealthCheck-{self.config.name}"
        )
        self._health_check_thread.start()
        logger.info(f"Background health monitoring started for {self.config.name} (Interval: {self.config.health_check_interval_seconds}s).")

    def _health_check_loop(self):
        while not self._health_check_stop_event.is_set():
            try:
                self.check_connection_health()
                if self.engine:
                    pool = self.engine.pool
                    self.metrics.current_pool_size = pool.size()
                    self.metrics.idle_connections = pool.checkedin()
                    self.metrics.active_connections = self.metrics.current_pool_size - self.metrics.idle_connections
            except Exception as e:
                logger.error(f"Error during periodic health check for {self.config.name}: {e}", exc_info=True)
            self._health_check_stop_event.wait(self.config.health_check_interval_seconds)

    def check_connection_health(self) -> bool:
        if not self.engine:
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            logger.warning(f"Health check for {self.config.name}: Engine not initialized.")
            return False
        if not self.circuit_breaker.can_execute():
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            logger.warning(f"Health check for {self.config.name}: Circuit breaker is OPEN.")
            return False
        
        start_time = time.perf_counter()
        try:
            with self.engine.connect() as connection:
                connection.execute(text(self.config.health_check_query))
            
            response_time_ms = (time.perf_counter() - start_time) * 1000
            self.metrics.update_query_response_time(response_time_ms)
            self.metrics.last_health_check_ts = datetime.now()
            self.metrics.consecutive_health_check_failures = 0
            
            if response_time_ms > 5000:
                self.metrics.health_status = ConnectionStatus.DEGRADED
                logger.warning(f"Health check for {self.config.name} DEGRADED: Response time {response_time_ms:.2f}ms")
            else:
                self.metrics.health_status = ConnectionStatus.HEALTHY
                logger.debug(f"Health check for {self.config.name} HEALTHY: Response time {response_time_ms:.2f}ms")
            
            self.circuit_breaker.record_success()
            return True
        except OperationalError as oe:
            logger.warning(f"Health check for {self.config.name} FAILED (OperationalError): {oe}")
            self._handle_health_check_failure()
            return False
        except Exception as e:
            logger.warning(f"Health check for {self.config.name} FAILED (Exception): {e}", exc_info=True)
            self._handle_health_check_failure()
            return False

    def _handle_health_check_failure(self):
        self.metrics.consecutive_health_check_failures += 1
        self.metrics.health_status = ConnectionStatus.UNHEALTHY
        self.circuit_breaker.record_failure()
        
        if self.metrics.consecutive_health_check_failures >= self.config.max_retries_on_failure:
            logger.warning(f"{self.config.name} has failed {self.metrics.consecutive_health_check_failures} consecutive health checks. Attempting to reset pool.")
            self._attempt_connection_reset()
            self.metrics.consecutive_health_check_failures = 0

    def _attempt_connection_reset(self):
        logger.info(f"Attempting to reset connection for {self.config.name}...")
        try:
            if self.engine:
                self.engine.dispose()
                logger.info(f"Old engine for {self.config.name} disposed.")
            if self.async_engine:
                logger.info(f"Async engine for {self.config.name} would need async disposal (skipped in sync reset for now).")

            self._initialize_connection()
            if self.engine:
                 logger.info(f"Connection for {self.config.name} reset and re-initialized successfully.")
                 self.metrics.health_status = ConnectionStatus.UNKNOWN
            else:
                logger.error(f"Failed to re-initialize engine for {self.config.name} after reset.")
                self.metrics.health_status = ConnectionStatus.UNHEALTHY

        except Exception as e:
            logger.error(f"Error during connection reset for {self.config.name}: {e}", exc_info=True)
            self.metrics.health_status = ConnectionStatus.UNHEALTHY

    def get_session(self) -> Session:
        if not self.engine or not self.scoped_session_factory:
            logger.error(f"Engine or session factory for {self.config.name} not initialized.")
            raise SQLAlchemyError(f"Connection {self.config.name} not ready.")

        if not self.circuit_breaker.can_execute():
            msg = f"Circuit breaker for {self.config.name} is OPEN. Database unavailable."
            logger.error(msg)
            raise OperationalError(msg, None, None)

        try:
            session = self.scoped_session_factory()
            return session
        except OperationalError as oe:
            logger.warning(f"OperationalError getting session for {self.config.name}: {oe}. Marking as failure.")
            self.circuit_breaker.record_failure()
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            self.metrics.failed_connection_attempts += 1
            if self.metrics.consecutive_health_check_failures == 0:
                 self._attempt_connection_reset()
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting session for {self.config.name}: {e}", exc_info=True)
            self.circuit_breaker.record_failure()
            raise SQLAlchemyError(f"Failed to get session for {self.config.name}: {e}")

    async def get_async_session(self) -> AsyncSession:
        if not self.async_engine or not self.async_session_factory:
            logger.error(f"Async engine or session factory for {self.config.name} not initialized.")
            raise SQLAlchemyError(f"Async connection {self.config.name} not ready.")

        if not self.circuit_breaker.can_execute():
            msg = f"Circuit breaker for {self.config.name} (async) is OPEN. Database unavailable."
            logger.error(msg)
            raise OperationalError(msg, None, None)
        
        try:
            session = self.async_session_factory()
            return session
        except OperationalError as oe:
            logger.warning(f"OperationalError getting async session for {self.config.name}: {oe}.")
            self.circuit_breaker.record_failure()
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            self.metrics.failed_connection_attempts += 1
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting async session for {self.config.name}: {e}", exc_info=True)
            self.circuit_breaker.record_failure()
            raise SQLAlchemyError(f"Failed to get async session for {self.config.name}: {e}")
            
    def close(self):
        logger.info(f"Closing connection for {self.config.name}...")
        self._health_check_stop_event.set()
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=max(1, self.config.health_check_interval_seconds // 2))
            if self._health_check_thread.is_alive():
                 logger.warning(f"Health check thread for {self.config.name} did not terminate gracefully.")
        
        if self.scoped_session_factory:
            try:
                self.scoped_session_factory.remove()
            except Exception as e:
                logger.warning(f"Error removing scoped session for {self.config.name}: {e}")

        if self.engine:
            try:
                self.engine.dispose()
                logger.info(f"Engine for {self.config.name} disposed.")
            except Exception as e:
                logger.error(f"Error disposing sync engine for {self.config.name}: {e}")
        
        if self.async_engine:
            try:
                async def dispose_async():
                    await self.async_engine.dispose()
                
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(dispose_async())
                    else:
                        loop.run_until_complete(dispose_async())
                    logger.info(f"Async engine for {self.config.name} disposed.")
                except RuntimeError:
                     logger.warning(f"No running event loop to dispose async engine for {self.config.name}. Manual async disposal might be needed.")
            except Exception as e:
                logger.error(f"Error disposing async engine for {self.config.name}: {e}")
        logger.info(f"Connection for {self.config.name} closed.")


class EnhancedConnectionManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(EnhancedConnectionManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._config: Optional[Dict[str, Any]] = None
        self.connections: Dict[str, DatabaseConnection] = {}
        self._read_write_splitting_enabled: bool = False
        
        self._monitoring_thread: Optional[threading.Thread] = None
        self._monitoring_stop_event = threading.Event()
        self._initialized = True

    def _load_config_from_file(self) -> Dict[str, Any]:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, 'config', 'config.yaml')
        
        if not os.path.exists(config_path):
            msg = f"Configuration file not found at {config_path}"
            logger.critical(msg)
            raise ConfigError(msg)
        try:
            with open(config_path, 'rt') as f:
                loaded_config = yaml.safe_load(f)
            logger.info(f"Successfully loaded configuration from {config_path}")
            return loaded_config
        except Exception as e:
            logger.critical(f"Error loading configuration from {config_path}: {e}", exc_info=True)
            raise ConfigError(f"Error loading config.yaml: {e}")

    def initialize_connections(self):
        if self._config is None:
            self._config = self._load_config_from_file()

        logger.info("Initializing database connections via EnhancedConnectionManager...")
        
        performance_config = self._config.get('performance', {})
        self._read_write_splitting_enabled = performance_config.get('enable_read_write_splitting', False)
        logger.info(f"Read/Write splitting is {'ENABLED' if self._read_write_splitting_enabled else 'DISABLED'}.")

        db_configs_map = {
            'postgres_staging': ('postgres_primary', DatabaseType.POSTGRES, 'postgres_read_replica_host'),
            'sql_server_production': ('sqlserver_primary', DatabaseType.SQLSERVER, 'sql_server_read_replica_server')
        }

        for config_key, (primary_conn_name, db_type, replica_config_key) in db_configs_map.items():
            db_yaml_config = self._config.get(config_key)
            if not db_yaml_config:
                logger.warning(f"Configuration for '{config_key}' not found in config.yaml. Skipping this connection.")
                continue

            primary_db_config_obj = self._create_db_config_object(db_yaml_config, primary_conn_name, db_type)
            self.connections[primary_conn_name] = DatabaseConnection(primary_db_config_obj, is_replica=False)

            replica_host_unresolved = performance_config.get(replica_config_key)
            if self._read_write_splitting_enabled and replica_host_unresolved:
                resolved_replica_host = self._resolve_config_value(replica_host_unresolved)
                if resolved_replica_host:
                    replica_conn_name = primary_conn_name.replace("_primary", "_replica")
                    replica_yaml_config = db_yaml_config.copy()
                    replica_yaml_config['host'] = resolved_replica_host
                    
                    replica_db_config_obj = self._create_db_config_object(replica_yaml_config, replica_conn_name, db_type)
                    self.connections[replica_conn_name] = DatabaseConnection(replica_db_config_obj, is_replica=True)
                    logger.info(f"Initialized READ REPLICA: {replica_conn_name} for {primary_conn_name} at {resolved_replica_host}")
        
        if not self.connections:
            logger.error("No database connections were initialized. Check config.yaml.")
            return

        self._start_global_monitoring()
        logger.info("EnhancedConnectionManager successfully initialized all configured connections.")

    def _resolve_config_value(self, value: Any) -> str:
        if not isinstance(value, str):
            return str(value)
        
        pattern = r'\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]+))?\}'
        
        def replace_match(match):
            var_name, default_value = match.groups()
            return os.environ.get(var_name, default_value if default_value is not None else "")
        
        return re.sub(pattern, replace_match, value)

    def _create_db_config_object(self, yaml_config_section: Dict, conn_name: str, db_type: DatabaseType) -> DatabaseConfig:
        default_ports = {DatabaseType.POSTGRES: 5432, DatabaseType.SQLSERVER: 1433}
        perf_conf = self._config.get('performance', {})
        
        resolved_config = {k: self._resolve_config_value(v) for k, v in yaml_config_section.items()}
        
        return DatabaseConfig(
            name=conn_name,
            db_type=db_type,
            host=resolved_config.get('host', 'localhost'),
            port=int(resolved_config.get('port', default_ports[db_type])),
            database=resolved_config.get('database'),
            user=resolved_config.get('user'),
            password=resolved_config.get('password'),
            trusted_connection=(str(resolved_config.get('trusted_connection', 'no')).lower() == 'yes') if db_type == DatabaseType.SQLSERVER else False,
            driver=resolved_config.get('driver') if db_type == DatabaseType.SQLSERVER else None,
            pool_size=int(self._resolve_config_value(yaml_config_section.get('pool_size', perf_conf.get('default_pool_size', 10)))),
            max_overflow=int(self._resolve_config_value(yaml_config_section.get('max_overflow', perf_conf.get('default_max_overflow', 20)))),
            pool_timeout=int(self._resolve_config_value(yaml_config_section.get('pool_timeout', perf_conf.get('default_pool_timeout', 30)))),
            pool_recycle=int(self._resolve_config_value(yaml_config_section.get('pool_recycle', perf_conf.get('default_pool_recycle', 1800)))),
            connect_timeout=int(self._resolve_config_value(yaml_config_section.get('connect_timeout', perf_conf.get('default_connect_timeout', 5)))),
            health_check_interval_seconds=int(self._resolve_config_value(yaml_config_section.get('health_check_interval', perf_conf.get('default_health_check_interval', 30)))),
            health_check_query=resolved_config.get('health_check_query', "SELECT 1"),
            max_retries_on_failure=int(self._resolve_config_value(yaml_config_section.get('max_retries', perf_conf.get('default_max_retries', 3)))),
            circuit_breaker_failure_threshold=int(self._resolve_config_value(yaml_config_section.get('circuit_breaker_failure_threshold', perf_conf.get('default_cb_failure_threshold', 5)))),
            circuit_breaker_recovery_timeout_seconds=int(self._resolve_config_value(yaml_config_section.get('circuit_breaker_recovery_timeout', perf_conf.get('default_cb_recovery_timeout', 60))))
        )

    def _get_connection(self, base_name: str, read_only: bool = False) -> Optional[DatabaseConnection]:
        if read_only and self._read_write_splitting_enabled:
            replica_name = base_name.replace("_primary", "_replica")
            replica_conn = self.connections.get(replica_name)
            if replica_conn and replica_conn.metrics.health_status in (ConnectionStatus.HEALTHY, ConnectionStatus.DEGRADED):
                logger.debug(f"Using READ REPLICA: {replica_name} for read_only query.")
                return replica_conn
            elif replica_conn:
                 logger.warning(f"Read replica {replica_name} is {replica_conn.metrics.health_status.value}. Falling back to primary for read_only query.")
        
        primary_conn = self.connections.get(base_name)
        if not primary_conn:
             logger.error(f"Primary connection {base_name} not found/initialized.")
        return primary_conn

    def get_postgres_session(self, read_only: bool = False) -> Session:
        conn = self._get_connection("postgres_primary", read_only)
        if conn:
            return conn.get_session()
        raise SQLAlchemyError("PostgreSQL connection 'postgres_primary' not available.")

    async def get_postgres_async_session(self, read_only: bool = False) -> AsyncSession:
        conn = self._get_connection("postgres_primary", read_only)
        if conn:
            return await conn.get_async_session()
        raise SQLAlchemyError("Async PostgreSQL connection 'postgres_primary' not available.")

    def get_sqlserver_session(self, read_only: bool = False) -> Session:
        conn = self._get_connection("sqlserver_primary", read_only)
        if conn:
            return conn.get_session()
        raise SQLAlchemyError("SQL Server connection 'sqlserver_primary' not available.")

    async def get_sqlserver_async_session(self, read_only: bool = False) -> AsyncSession:
        conn = self._get_connection("sqlserver_primary", read_only)
        if conn:
            return await conn.get_async_session()
        raise SQLAlchemyError("Async SQL Server connection 'sqlserver_primary' not available.")

    def _start_global_monitoring(self):
        if not self._config.get('monitoring', {}).get('connection_metrics_logging_enabled', True):
            logger.info("Global connection metrics logging is disabled in config.")
            return
        self._monitoring_thread = threading.Thread(
            target=self._global_monitoring_loop,
            daemon=True,
            name="GlobalConnectionMonitor"
        )
        self._monitoring_thread.start()
        logger.info("Global connection monitoring thread started.")

    def _global_monitoring_loop(self):
        interval = self._config.get('monitoring', {}).get('connection_metrics_log_interval_seconds', 60)
        while not self._monitoring_stop_event.is_set():
            try:
                self.log_all_connection_metrics()
            except Exception as e:
                logger.error(f"Error in global monitoring loop: {e}", exc_info=True)
            self._monitoring_stop_event.wait(interval)

    def log_all_connection_metrics(self):
        logger.info("--- Global Database Connection Metrics Report ---")
        for name, conn_obj in self.connections.items():
            m = conn_obj.metrics
            cb_state = conn_obj.circuit_breaker.state.value
            logger.info(
                f"  Connection: {name} ({'Replica' if conn_obj.is_replica else 'Primary'}) | "
                f"Status: {m.health_status.value} | CB: {cb_state} | "
                f"Pool (Cur/Max): {m.current_pool_size}/{m.total_connections_configured} | "
                f"Active: {m.active_connections} | Idle: {m.idle_connections} | "
                f"Queries: {m.total_queries_executed} | Avg Latency: {m.avg_query_response_time_ms:.2f}ms | "
                f"Health Check Fails (Consec): {m.consecutive_health_check_failures} | "
                f"Total Connection Fails: {m.failed_connection_attempts}"
            )
        logger.info("--- End of Report ---")
    
    def get_all_connection_metrics_details(self) -> Dict[str, Any]:
        report = {}
        for name, conn_obj in self.connections.items():
            m = conn_obj.metrics
            report[name] = {
                "name": conn_obj.config.name,
                "type": conn_obj.config.db_type.value,
                "is_replica": conn_obj.is_replica,
                "health_status": m.health_status.value,
                "circuit_breaker_state": conn_obj.circuit_breaker.state.value,
                "pool_configured_size": m.total_connections_configured,
                "pool_current_size": m.current_pool_size,
                "pool_active_connections": m.active_connections,
                "pool_idle_connections": m.idle_connections,
                "connection_attempts_failed": m.failed_connection_attempts,
                "connection_attempts_successful": m.successful_connection_attempts,
                "queries_executed_total": m.total_queries_executed,
                "avg_query_response_time_ms": m.avg_query_response_time_ms,
                "last_health_check_timestamp": m.last_health_check_ts.isoformat() if m.last_health_check_ts else None,
                "consecutive_health_check_failures": m.consecutive_health_check_failures,
            }
        return report

    def run_all_health_checks(self) -> Dict[str, bool]:
        results = {}
        for name, conn_obj in self.connections.items():
            results[name] = conn_obj.check_connection_health()
        return results

    def close_all_scoped_sessions(self):
        for conn_obj in self.connections.values():
            if conn_obj.scoped_session_factory:
                try:
                    conn_obj.scoped_session_factory.remove()
                except Exception as e:
                    logger.warning(f"Error removing scoped session for {conn_obj.config.name}: {e}")
                    
    def dispose_all_connections(self):
        logger.info("Disposing all database connections and stopping monitors...")
        self._monitoring_stop_event.set()
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            self._monitoring_thread.join(timeout=5)
            if self._monitoring_thread.is_alive():
                 logger.warning("Global monitoring thread did not terminate gracefully.")

        for name, conn_obj in list(self.connections.items()):
            try:
                conn_obj.close()
            except Exception as e:
                logger.error(f"Error closing connection {name}: {e}", exc_info=True)
        self.connections.clear()
        logger.info("All database connections disposed and manager reset.")
        EnhancedConnectionManager._instance = None 
        EnhancedConnectionManager._initialized = False

db_manager = EnhancedConnectionManager()

def init_database_connections():
    global db_manager
    if not db_manager._config:
        db_manager.initialize_connections()
    else:
        logger.info("Database connections already initialized.")


def get_postgres_session(read_only: bool = False) -> Session:
    if not db_manager.connections: init_database_connections()
    return db_manager.get_postgres_session(read_only=read_only)

async def get_postgres_async_session(read_only: bool = False) -> AsyncSession:
    if not db_manager.connections: init_database_connections()
    return await db_manager.get_postgres_async_session(read_only=read_only)

def get_sqlserver_session(read_only: bool = False) -> Session:
    if not db_manager.connections: init_database_connections()
    return db_manager.get_sqlserver_session(read_only=read_only)

async def get_sqlserver_async_session(read_only: bool = False) -> AsyncSession:
    if not db_manager.connections: init_database_connections()
    return await db_manager.get_sqlserver_async_session(read_only=read_only)

def check_postgres_connection(session=None) -> bool:
    if not db_manager.connections: init_database_connections()
    conn = db_manager.connections.get("postgres_primary")
    return conn.check_connection_health() if conn else False

def check_sqlserver_connection(session=None) -> bool:
    if not db_manager.connections: init_database_connections()
    conn = db_manager.connections.get("sqlserver_primary")
    return conn.check_connection_health() if conn else False

def dispose_engines():
    global db_manager
    if db_manager and db_manager.connections:
        db_manager.dispose_all_connections()

def close_scoped_sessions():
    if db_manager and db_manager.connections:
        db_manager.close_all_scoped_sessions()

CONFIG: Optional[Dict[str, Any]] = None
if db_manager and db_manager._config:
    CONFIG = db_manager._config
elif os.path.exists(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.yaml')):
    try:
        with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.yaml'), 'rt') as f:
            CONFIG = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Fallback config loading failed: {e}")
        CONFIG = {}
else:
    CONFIG = {}


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging
    setup_logging()
    main_cid = get_correlation_id()

    async def main_test_async():
        logger.info(f"[{main_cid}] Starting EnhancedConnectionManager test...")
        init_database_connections()

        logger.info("--- Testing Synchronous Sessions ---")
        try:
            with get_postgres_session() as pg_sess_write:
                logger.info(f"Got PostgreSQL write session ({pg_sess_write.bind.url.host})")
                pg_sess_write.execute(text("SELECT 1 AS testval;"))
            with get_postgres_session(read_only=True) as pg_sess_read:
                logger.info(f"Got PostgreSQL read session ({pg_sess_read.bind.url.host})")
                pg_sess_read.execute(text("SELECT version();"))
            
            with get_sqlserver_session() as sql_sess_write:
                logger.info(f"Got SQL Server write session ({sql_sess_write.bind.url.host})")
                sql_sess_write.execute(text("SELECT @@SERVERNAME AS servername;"))
            with get_sqlserver_session(read_only=True) as sql_sess_read:
                logger.info(f"Got SQL Server read session ({sql_sess_read.bind.url.host})")
                sql_sess_read.execute(text("SELECT @@VERSION AS version;"))
        except Exception as e:
            logger.error(f"Error during synchronous session tests: {e}", exc_info=True)

        logger.info("\n--- Testing Asynchronous Sessions ---")
        try:
            async with await get_postgres_async_session() as pg_async_sess_write:
                logger.info(f"Got async PostgreSQL write session ({pg_async_sess_write.bind.url.host})")
                await pg_async_sess_write.execute(text("SELECT 1;"))
            async with await get_postgres_async_session(read_only=True) as pg_async_sess_read:
                logger.info(f"Got async PostgreSQL read session ({pg_async_sess_read.bind.url.host})")
                await pg_async_sess_read.execute(text("SELECT version();"))

            if db_manager.connections.get("sqlserver_primary") and db_manager.connections["sqlserver_primary"].async_engine:
                async with await get_sqlserver_async_session() as sql_async_sess_write:
                    logger.info(f"Got async SQL Server write session ({sql_async_sess_write.bind.url.host})")
                    await sql_async_sess_write.execute(text("SELECT @@SERVERNAME;"))
                async with await get_sqlserver_async_session(read_only=True) as sql_async_sess_read:
                    logger.info(f"Got async SQL Server read session ({sql_async_sess_read.bind.url.host})")
                    await sql_async_sess_read.execute(text("SELECT @@VERSION;"))
            else:
                logger.warning("Async SQL Server engine not available, skipping async SQL Server tests.")
        except Exception as e:
            logger.error(f"Error during asynchronous session tests: {e}", exc_info=True)

        logger.info("\n--- Health Checks & Metrics ---")
        health_status = db_manager.run_all_health_checks()
        logger.info(f"Overall Health Status: {health_status}")

        metrics_report = db_manager.get_all_connection_metrics_details()
        import json
        logger.info(f"Connection Metrics Report:\n{json.dumps(metrics_report, indent=2)}")

        logger.info("Simulating work and monitoring for 30 seconds...")
        await asyncio.sleep(30) 

        logger.info("Final metrics before shutdown:")
        db_manager.log_all_connection_metrics()
        
        dispose_engines()
        logger.info(f"[{main_cid}] EnhancedConnectionManager test finished.")

    if __name__ == "__main__":
        asyncio.run(main_test_async())
