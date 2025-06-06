# app/database/connection_manager.py
"""
Production-grade database connection manager with enhanced SQL Server support for Windows.
Fixed to resolve SQLAlchemy 2.0 pyodbc dialect loading issues on Windows.
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
import statistics

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError, InvalidRequestError, NoSuchModuleError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import ConfigError

logger = get_logger('app.database.connection_manager')

# --- Enhanced Pre-flight Check for Database Drivers ---
def check_database_drivers():
    """Check for required database drivers and provide helpful error messages."""
    drivers_status = {
        'psycopg2': False,
        'pyodbc': False,
        'sqlalchemy_mssql_dialect': False
    }
    
    # Check psycopg2
    try:
        importlib.import_module("psycopg2")
        drivers_status['psycopg2'] = True
        logger.debug("psycopg2 driver found.")
    except ImportError:
        logger.critical("FATAL: The 'psycopg2' library is not installed. Please run 'pip install psycopg2-binary'.")

    # Check pyodbc
    try:
        import pyodbc
        drivers_status['pyodbc'] = True
        logger.debug(f"pyodbc driver found, version: {pyodbc.version}")
        
        # Test if pyodbc can be imported properly
        drivers = pyodbc.drivers()
        logger.debug(f"Available ODBC drivers: {drivers}")
        
        # Check for SQL Server drivers specifically
        sql_server_drivers = [d for d in drivers if 'SQL Server' in d]
        if sql_server_drivers:
            logger.info(f"SQL Server ODBC drivers found: {sql_server_drivers}")
        else:
            logger.warning("No SQL Server ODBC drivers found. Consider installing 'ODBC Driver 17 for SQL Server'")
            
    except ImportError:
        logger.critical("FATAL: The 'pyodbc' library is not installed. Please run 'pip install pyodbc'.")
    except Exception as e:
        logger.error(f"pyodbc library found but error during initialization: {e}")

    # Check SQLAlchemy SQL Server dialect
    try:
        from sqlalchemy.dialects import mssql
        from sqlalchemy.dialects.mssql import pyodbc as mssql_pyodbc
        drivers_status['sqlalchemy_mssql_dialect'] = True
        logger.debug("SQLAlchemy SQL Server dialect found.")
    except ImportError as e:
        logger.critical(f"FATAL: SQLAlchemy SQL Server dialect not available: {e}")
        
    return drivers_status

# Perform driver check at module import
DRIVER_STATUS = check_database_drivers()

class DatabaseType(Enum):
    POSTGRES = "postgresql"
    SQLSERVER = "mssql"  # Changed from "sqlserver" to "mssql" for SQLAlchemy 2.0

class ConnectionStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class ConnectionMetrics:
    total_connections_configured: int = 0
    current_pool_size: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    failed_connection_attempts: int = 0
    successful_connection_attempts: int = 0
    total_queries_executed: int = 0
    avg_query_response_time_ms: float = 0.0
    last_health_check_ts: Optional[datetime] = None
    health_status: ConnectionStatus = ConnectionStatus.UNKNOWN
    consecutive_health_check_failures: int = 0
    query_response_times_ms: List[float] = field(default_factory=lambda: [])
    
    def update_query_response_time(self, response_time_ms: float):
        self.query_response_times_ms.append(response_time_ms)
        if len(self.query_response_times_ms) > 100:
            self.query_response_times_ms = self.query_response_times_ms[-100:]
        if self.query_response_times_ms:
            self.avg_query_response_time_ms = statistics.mean(self.query_response_times_ms)

@dataclass
class DatabaseConfig:
    name: str
    db_type: DatabaseType
    host: str
    port: int
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    trusted_connection: bool = False
    driver: Optional[str] = None
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 1800
    connect_timeout: int = 5
    health_check_interval_seconds: int = 30
    health_check_query: str = "SELECT 1"
    max_retries_on_failure: int = 3
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout_seconds: int = 60

class CircuitBreakerState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout_seconds: int, name: str):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.name = name
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitBreakerState.CLOSED
        self._lock = threading.RLock()
    
    def can_execute(self) -> bool:
        with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time and (datetime.now() - self.last_failure_time).total_seconds() > self.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    return True
                return False
            return True
    
    def record_success(self):
        with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
    
    def record_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            if self.state == CircuitBreakerState.HALF_OPEN or self.failure_count >= self.failure_threshold:
                if self.state != CircuitBreakerState.OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"CircuitBreaker '{self.name}' opened.")

class DatabaseConnection:
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
        """Build connection URL with enhanced SQL Server support for Windows."""
        db_type = self.config.db_type
        
        if db_type == DatabaseType.POSTGRES:
            driver_part = "+asyncpg" if for_async else "+psycopg2"
            auth_part = f"{self.config.user}:{self.config.password}@" if self.config.user and self.config.password else ""
            return f"postgresql{driver_part}://{auth_part}{self.config.host}:{self.config.port}/{self.config.database}"
        
        elif db_type == DatabaseType.SQLSERVER:
            # Enhanced SQL Server connection handling for Windows
            if for_async:
                # For async, we need aioodbc but it's often problematic
                # Fall back to sync for now or use a different approach
                logger.warning("Async SQL Server connections may have limited support. Using sync-compatible URL.")
                driver_part = "+pyodbc"
            else:
                driver_part = "+pyodbc"
            
            # Build connection string based on configuration
            if self.config.trusted_connection:
                # Windows Authentication
                if self.config.driver:
                    # Using explicit driver with Windows auth
                    odbc_params = [
                        f"DRIVER={{{self.config.driver}}}",
                        f"SERVER={self.config.host},{self.config.port}",
                        f"DATABASE={self.config.database}",
                        "Trusted_Connection=yes",
                        "TrustServerCertificate=yes"  # For local development
                    ]
                    return f"mssql{driver_part}:///?odbc_connect={';'.join(odbc_params)}"
                else:
                    # Try direct URL with trusted connection
                    return f"mssql{driver_part}://{self.config.host}:{self.config.port}/{self.config.database}?trusted_connection=yes&TrustServerCertificate=yes"
            else:
                # SQL Server Authentication
                if self.config.driver:
                    # Using explicit driver with SQL auth
                    odbc_params = [
                        f"DRIVER={{{self.config.driver}}}",
                        f"SERVER={self.config.host},{self.config.port}",
                        f"DATABASE={self.config.database}",
                        f"UID={self.config.user}",
                        f"PWD={self.config.password}",
                        "TrustServerCertificate=yes"
                    ]
                    return f"mssql{driver_part}:///?odbc_connect={';'.join(odbc_params)}"
                else:
                    # Direct URL with SQL auth
                    auth_part = f"{self.config.user}:{self.config.password}@" if self.config.user and self.config.password else ""
                    return f"mssql{driver_part}://{auth_part}{self.config.host}:{self.config.port}/{self.config.database}?TrustServerCertificate=yes"
        
        else:
            raise ConfigError(f"Unsupported database type: {db_type}")

    def _initialize_connection(self):
        logger.info(f"Initializing connection for {self.config.name}...")
        
        # Check if required drivers are available
        if self.config.db_type == DatabaseType.POSTGRES and not DRIVER_STATUS['psycopg2']:
            raise ConfigError("PostgreSQL driver (psycopg2) is not available")
        elif self.config.db_type == DatabaseType.SQLSERVER and not DRIVER_STATUS['pyodbc']:
            raise ConfigError("SQL Server driver (pyodbc) is not available")
        
        try:
            sync_url = self._get_connection_url(for_async=False)
            logger.debug(f"Sync connection URL for {self.config.name}: {sync_url}")
            
            # Build connect_args based on database type
            connect_args = {}
            if self.config.db_type == DatabaseType.POSTGRES:
                connect_args = {'connect_timeout': self.config.connect_timeout}
            elif self.config.db_type == DatabaseType.SQLSERVER:
                # SQL Server specific connection arguments
                connect_args = {
                    'timeout': self.config.connect_timeout,
                    'autocommit': False
                }
            
            self.engine = create_engine(
                sync_url, 
                poolclass=QueuePool, 
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow, 
                pool_timeout=self.config.pool_timeout,
                pool_recycle=self.config.pool_recycle, 
                pool_pre_ping=True,
                connect_args=connect_args, 
                echo=False,
                # Additional SQL Server optimizations
                fast_executemany=True if self.config.db_type == DatabaseType.SQLSERVER else None
            )
            
            self.session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
            self.scoped_session_factory = scoped_session(self.session_factory)

            # Async engine setup (limited for SQL Server)
            if self.config.db_type == DatabaseType.POSTGRES:
                try:
                    async_url = self._get_connection_url(for_async=True)
                    self.async_engine = create_async_engine(async_url, pool_size=self.config.pool_size,
                                                            max_overflow=self.config.max_overflow, echo=False)
                    self.async_session_factory = async_sessionmaker(bind=self.async_engine, class_=AsyncSession)
                except Exception as e:
                    logger.warning(f"Async engine setup failed for {self.config.name}: {e}")
            
            self._add_event_listeners()
            self._warm_connection_pool()
            self.metrics.health_status = ConnectionStatus.UNKNOWN
            logger.info(f"Connection initialized for {self.config.name}")

        except NoSuchModuleError as nsme:
            logger.critical(f"SQLAlchemy driver error for {self.config.name}: {nsme}. This means a required library (like 'pyodbc') is not installed or accessible. Please check your environment.", exc_info=True)
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            
            # Provide specific guidance for SQL Server issues
            if self.config.db_type == DatabaseType.SQLSERVER:
                logger.critical("SQL Server connection failed. Please ensure:")
                logger.critical("1. pyodbc is installed: pip install pyodbc")
                logger.critical("2. Microsoft ODBC Driver for SQL Server is installed")
                logger.critical("3. Your config.yaml specifies a valid driver name")
                logger.critical("Available drivers on your system:")
                try:
                    import pyodbc
                    for driver in pyodbc.drivers():
                        logger.critical(f"   - {driver}")
                except:
                    logger.critical("   Could not enumerate drivers")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize connection for {self.config.name}: {e}", exc_info=True)
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            self.metrics.failed_connection_attempts += 1
            raise
    
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
        
        @event.listens_for(self.engine.pool, "checkout")
        def on_pool_checkout(dbapi_connection, connection_record, connection_proxy):
            self.metrics.active_connections += 1
            if self.engine: self.metrics.idle_connections = self.engine.pool.checkedin()

        @event.listens_for(self.engine.pool, "checkin")
        def on_pool_checkin(dbapi_connection, connection_record):
            self.metrics.active_connections = max(0, self.metrics.active_connections - 1)
            if self.engine: self.metrics.idle_connections = self.engine.pool.checkedin()

        @event.listens_for(self.engine, "handle_error")
        def on_engine_error(exception_context):
            self.metrics.failed_connection_attempts += 1
            self.circuit_breaker.record_failure()

    def _warm_connection_pool(self):
        if not self.engine: return
        warmed_connections = []
        try:
            for _ in range(max(1, self.config.pool_size // 2)):
                conn = self.engine.connect()
                conn.execute(text(self.config.health_check_query))
                warmed_connections.append(conn)
        except Exception as e:
            logger.warning(f"Pool warming failed for {self.config.name}: {e}")
        finally:
            for conn in warmed_connections: 
                try:
                    conn.close()
                except:
                    pass
            logger.info(f"Pool warming for {self.config.name} completed.")

    def _start_health_monitoring(self):
        if not self.engine: return
        self._health_check_thread = threading.Thread(target=self._health_check_loop, daemon=True, name=f"HealthCheck-{self.config.name}")
        self._health_check_thread.start()

    def _health_check_loop(self):
        while not self._health_check_stop_event.is_set():
            self.check_connection_health()
            self._health_check_stop_event.wait(self.config.health_check_interval_seconds)

    def check_connection_health(self) -> bool:
        if not self.engine or not self.circuit_breaker.can_execute():
            self.metrics.health_status = ConnectionStatus.UNHEALTHY
            return False
        
        try:
            with self.engine.connect() as connection:
                connection.execute(text(self.config.health_check_query))
            self.circuit_breaker.record_success()
            self.metrics.health_status = ConnectionStatus.HEALTHY
            self.metrics.consecutive_health_check_failures = 0
            return True
        except Exception as e:
            logger.debug(f"Health check failed for {self.config.name}: {e}")
            self._handle_health_check_failure()
            return False

    def _handle_health_check_failure(self):
        self.metrics.consecutive_health_check_failures += 1
        self.metrics.health_status = ConnectionStatus.UNHEALTHY
        self.circuit_breaker.record_failure()
        if self.metrics.consecutive_health_check_failures >= self.config.max_retries_on_failure:
            logger.warning(f"Multiple health check failures for {self.config.name}, attempting connection reset")
            self._attempt_connection_reset()
            self.metrics.consecutive_health_check_failures = 0

    def _attempt_connection_reset(self):
        try:
            if self.engine: 
                self.engine.dispose()
            time.sleep(2)  # Brief pause before reinitializing
            self._initialize_connection()
            if self.engine: 
                self.metrics.health_status = ConnectionStatus.UNKNOWN
        except Exception as e:
            logger.error(f"Error during connection reset for {self.config.name}: {e}")

    def get_session(self) -> Session:
        if not self.scoped_session_factory: 
            raise SQLAlchemyError(f"Connection {self.config.name} not ready.")
        if not self.circuit_breaker.can_execute(): 
            raise OperationalError(f"Circuit breaker for {self.config.name} is OPEN.", None, None)
        return self.scoped_session_factory()

    async def get_async_session(self) -> AsyncSession:
        if not self.async_session_factory: 
            raise SQLAlchemyError(f"Async connection {self.config.name} not ready.")
        if not self.circuit_breaker.can_execute(): 
            raise OperationalError(f"Circuit breaker for {self.config.name} (async) is OPEN.", None, None)
        return self.async_session_factory()
            
    def close(self):
        self._health_check_stop_event.set()
        if self._health_check_thread: 
            self._health_check_thread.join(timeout=2)
        if self.scoped_session_factory: 
            self.scoped_session_factory.remove()
        if self.engine: 
            self.engine.dispose()
        if self.async_engine: 
            try:
                asyncio.run(self.async_engine.dispose())
            except:
                pass

class EnhancedConnectionManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        self._config: Optional[Dict[str, Any]] = None
        self.connections: Dict[str, DatabaseConnection] = {}
        self._read_write_splitting_enabled: bool = False
        self._monitoring_thread: Optional[threading.Thread] = None
        self._monitoring_stop_event = threading.Event()
        self._initialized = True

    def _load_config_from_file(self) -> Dict[str, Any]:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, 'config', 'config.yaml')
        logger.info(f"Attempting to load configuration from: {config_path}")
        if not os.path.exists(config_path):
            raise ConfigError(f"Config file not found: {config_path}")
        with open(config_path, 'rt') as f:
            return yaml.safe_load(f)

    def initialize_connections(self):
        if self.connections: return
        self._config = self._load_config_from_file()
        performance_config = self._config.get('performance', {})
        self._read_write_splitting_enabled = performance_config.get('enable_read_write_splitting', False)

        for key, (name, db_type, replica_key) in {
            'postgres_staging': ('postgres_primary', DatabaseType.POSTGRES, 'postgres_read_replica_host'),
            'sql_server_production': ('sqlserver_primary', DatabaseType.SQLSERVER, 'sql_server_read_replica_server')
        }.items():
            if db_config := self._config.get(key):
                try:
                    self.connections[name] = DatabaseConnection(self._create_db_config_object(db_config, name, db_type))
                    logger.info(f"Successfully initialized connection: {name}")
                except Exception as e:
                    logger.error(f"Failed to initialize connection {name}: {e}")
                    # Continue with other connections even if one fails
                    
                if self._read_write_splitting_enabled and (replica_host := self._resolve_config_value(performance_config.get(replica_key))):
                    replica_name = name.replace("_primary", "_replica")
                    replica_config = {**db_config, 'host': replica_host}
                    try:
                        self.connections[replica_name] = DatabaseConnection(self._create_db_config_object(replica_config, replica_name, db_type), is_replica=True)
                        logger.info(f"Successfully initialized replica connection: {replica_name}")
                    except Exception as e:
                        logger.error(f"Failed to initialize replica connection {replica_name}: {e}")
                        
        self._start_global_monitoring()

    def _resolve_config_value(self, value: Any) -> str:
        if not isinstance(value, str): return str(value)
        pattern = r'\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]+))?\}'
        return re.sub(pattern, lambda m: os.environ.get(m.group(1), m.group(2) or ""), value)

    def _create_db_config_object(self, yaml_config: Dict, name: str, db_type: DatabaseType) -> DatabaseConfig:
        resolved = {k: self._resolve_config_value(v) for k, v in yaml_config.items()}
        perf_conf = self._config.get('performance', {})
        
        # Default ports for each database type
        default_ports = {DatabaseType.POSTGRES: 5432, DatabaseType.SQLSERVER: 1433}
        port_str = str(resolved.get('port', default_ports[db_type]))
        
        try:
            port_val = int(port_str)
        except ValueError:
            raise ConfigError(f"Database port for '{name}' could not be converted to an integer. "
                              f"Check environment variables. Value received: '{port_str}'")
        
        # Parse server field for SQL Server (might include port)
        host = resolved.get('host', 'localhost')
        if db_type == DatabaseType.SQLSERVER and 'server' in resolved:
            server_str = resolved['server']
            if ',' in server_str:
                host, port_str = server_str.split(',', 1)
                try:
                    port_val = int(port_str)
                except ValueError:
                    logger.warning(f"Could not parse port from server string: {server_str}")
            else:
                host = server_str
        
        return DatabaseConfig(
            name=name, db_type=db_type,
            host=host, port=port_val,
            database=resolved.get('database'), 
            user=resolved.get('user'), 
            password=resolved.get('password'),
            trusted_connection=(resolved.get('trusted_connection', 'no').lower() in ['yes', 'true', '1']),
            driver=resolved.get('driver'),
            pool_size=int(resolved.get('pool_size', 10)),
            max_overflow=int(resolved.get('max_overflow', 20)),
            pool_timeout=int(resolved.get('pool_timeout', 30)),
            pool_recycle=int(resolved.get('pool_recycle', 1800)),
            connect_timeout=int(resolved.get('connect_timeout', 5)),
            health_check_interval_seconds=int(resolved.get('health_check_interval', 30)),
            max_retries_on_failure=int(resolved.get('max_retries', 3)),
            circuit_breaker_failure_threshold=int(resolved.get('circuit_breaker_failure_threshold', 5)),
            circuit_breaker_recovery_timeout_seconds=int(resolved.get('circuit_breaker_recovery_timeout', 60))
        )

    def _get_connection(self, base_name: str, read_only: bool = False) -> Optional[DatabaseConnection]:
        if read_only and self._read_write_splitting_enabled:
            replica_name = base_name.replace("_primary", "_replica")
            if (replica_conn := self.connections.get(replica_name)) and replica_conn.metrics.health_status != ConnectionStatus.UNHEALTHY:
                return replica_conn
        return self.connections.get(base_name)

    def get_postgres_session(self, read_only: bool = False) -> Session:
        if conn := self._get_connection("postgres_primary", read_only): 
            return conn.get_session()
        raise SQLAlchemyError("PostgreSQL connection 'postgres_primary' not available.")

    async def get_postgres_async_session(self, read_only: bool = False) -> AsyncSession:
        if conn := self._get_connection("postgres_primary", read_only): 
            return await conn.get_async_session()
        raise SQLAlchemyError("Async PostgreSQL 'postgres_primary' not available.")

    def get_sqlserver_session(self, read_only: bool = False) -> Session:
        if conn := self._get_connection("sqlserver_primary", read_only): 
            return conn.get_session()
        raise SQLAlchemyError("SQL Server connection 'sqlserver_primary' not available.")

    async def get_sqlserver_async_session(self, read_only: bool = False) -> AsyncSession:
        if conn := self._get_connection("sqlserver_primary", read_only): 
            return await conn.get_async_session()
        raise SQLAlchemyError("Async SQL Server 'sqlserver_primary' not available.")

    def _start_global_monitoring(self):
        if self._config.get('monitoring', {}).get('connection_metrics_logging_enabled', True):
            self._monitoring_thread = threading.Thread(target=self._global_monitoring_loop, daemon=True)
            self._monitoring_thread.start()

    def _global_monitoring_loop(self):
        interval = self._config.get('monitoring', {}).get('connection_metrics_log_interval_seconds', 60)
        while not self._monitoring_stop_event.is_set():
            self.log_all_connection_metrics()
            self._monitoring_stop_event.wait(interval)

    def log_all_connection_metrics(self):
        logger.info("--- Global Database Connection Metrics Report ---")
        for name, conn in self.connections.items():
            m = conn.metrics
            logger.info(f"  {name}: Status={m.health_status.value}, CB={conn.circuit_breaker.state.value}, "
                       f"Queries={m.total_queries_executed}, AvgTime={m.avg_query_response_time_ms:.2f}ms")

    def dispose_all_connections(self):
        self._monitoring_stop_event.set()
        if self._monitoring_thread: self._monitoring_thread.join(timeout=2)
        for conn in self.connections.values(): conn.close()
        self.connections.clear()
        EnhancedConnectionManager._instance = None
        EnhancedConnectionManager._initialized = False

# --- Public API for the Singleton ---
db_manager = EnhancedConnectionManager()

def init_database_connections():
    if not db_manager.connections: db_manager.initialize_connections()

def get_postgres_session(read_only: bool = False) -> Session:
    init_database_connections(); return db_manager.get_postgres_session(read_only)

async def get_postgres_async_session(read_only: bool = False) -> AsyncSession:
    init_database_connections(); return await db_manager.get_postgres_async_session(read_only)

def get_sqlserver_session(read_only: bool = False) -> Session:
    init_database_connections(); return db_manager.get_sqlserver_session(read_only)

async def get_sqlserver_async_session(read_only: bool = False) -> AsyncSession:
    init_database_connections(); return await db_manager.get_sqlserver_async_session(read_only)

def check_postgres_connection(session=None) -> bool:
    init_database_connections()
    if conn := db_manager.connections.get("postgres_primary"): return conn.check_connection_health()
    return False

def check_sqlserver_connection(session=None) -> bool:
    init_database_connections()
    if conn := db_manager.connections.get("sqlserver_primary"): return conn.check_connection_health()
    return False

def dispose_engines():
    if db_manager and db_manager.connections: db_manager.dispose_all_connections()

def close_scoped_sessions():
    if db_manager and db_manager.connections:
        for conn in db_manager.connections.values():
            if conn.scoped_session_factory: conn.scoped_session_factory.remove()

CONFIG: Dict[str, Any] = {}
try:
    if not db_manager._config: db_manager._config = db_manager._load_config_from_file()
    CONFIG = db_manager._config
except (ConfigError, FileNotFoundError):
    logger.warning("Could not load CONFIG from connection_manager; it may not be initialized yet.")