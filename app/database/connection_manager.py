# app/database/connection_manager.py
"""
Manages database connections and sessions for PostgreSQL and SQL Server.
Includes connection pooling, warming, health checks, and read/write splitting logic.
Reads configuration from config.yaml.
"""
import yaml
import os
import time
import threading
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool, NullPool
from sqlalchemy.exc import DisconnectionError, SQLAlchemyError

# Assuming logging_config is in app.utils
# To avoid circular dependency if connection_manager is imported by logging_config (e.g. for db logging)
# we might need a more careful import strategy or pass logger instances.
# For now, direct import:
from app.utils.logging_config import get_logger, set_correlation_id

logger = get_logger('app.database.connection_manager')

# Global engine and sessionmaker variables
_postgres_engine = None
_postgres_session_factory = None
_postgres_scoped_session = None

_sqlserver_engine = None
_sqlserver_session_factory = None
_sqlserver_scoped_session = None

# Read replica engines (optional)
_postgres_read_replica_engine = None
_sqlserver_read_replica_engine = None


def _load_config():
    """Loads database configuration from config.yaml."""
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    if not os.path.exists(config_path):
        logger.error("config.yaml not found. Cannot initialize database connections.")
        raise FileNotFoundError("config.yaml not found.")
    with open(config_path, 'rt') as f:
        return yaml.safe_load(f)

CONFIG = _load_config()

def _get_postgres_connection_url(replica=False):
    """Constructs PostgreSQL connection URL from config."""
    db_config_key = 'postgres_staging'
    if replica:
        # Assuming replica config might be under a specific key or derived
        # For simplicity, let's assume a similar structure or a dedicated key if provided in config
        replica_host = CONFIG.get('performance', {}).get('postgres_read_replica_host')
        if not replica_host:
            logger.warning("PostgreSQL read replica host not configured. Using primary for reads.")
            return _get_postgres_connection_url(replica=False) # Fallback to primary
        
        pg_conf = CONFIG.get(db_config_key, {})
        return f"postgresql+psycopg2://{pg_conf.get('user')}:{pg_conf.get('password')}@{replica_host}:{pg_conf.get('port')}/{pg_conf.get('database')}"
    
    pg_conf = CONFIG.get(db_config_key, {})
    if not all([pg_conf.get('host'), pg_conf.get('port'), pg_conf.get('database'), pg_conf.get('user')]):
        logger.error(f"Incomplete PostgreSQL configuration for '{db_config_key}'.")
        raise ValueError(f"Incomplete PostgreSQL configuration for '{db_config_key}'.")
    return f"postgresql+psycopg2://{pg_conf.get('user')}:{pg_conf.get('password')}@{pg_conf.get('host')}:{pg_conf.get('port')}/{pg_conf.get('database')}"

def _get_sqlserver_connection_url(replica=False):
    """Constructs SQL Server connection URL from config."""
    db_config_key = 'sql_server_production'
    sql_conf = CONFIG.get(db_config_key, {})
    
    server = sql_conf.get('server')
    if replica:
        replica_server = CONFIG.get('performance', {}).get('sql_server_read_replica_server')
        if not replica_server:
            logger.warning("SQL Server read replica server not configured. Using primary for reads.")
            return _get_sqlserver_connection_url(replica=False) # Fallback to primary
        server = replica_server

    if not all([sql_conf.get('driver'), server, sql_conf.get('database')]):
        logger.error(f"Incomplete SQL Server configuration for '{db_config_key}'.")
        raise ValueError(f"Incomplete SQL Server configuration for '{db_config_key}'.")

    conn_str = f"mssql+pyodbc:///?odbc_connect=DRIVER={sql_conf.get('driver')};SERVER={server};DATABASE={sql_conf.get('database')}"
    if sql_conf.get('trusted_connection', 'yes').lower() == 'yes':
        conn_str += ";Trusted_Connection=yes"
    else:
        if not all([sql_conf.get('user'), sql_conf.get('password')]):
            logger.error(f"User/password required for SQL Server when Trusted_Connection is not 'yes' for '{db_config_key}'.")
            raise ValueError(f"User/password required for SQL Server for '{db_config_key}'.")
        conn_str += f";UID={sql_conf.get('user')};PWD={sql_conf.get('password')}"
    return conn_str

def _create_db_engine(db_type, replica=False, pool_warming=True):
    """Creates a SQLAlchemy engine with connection pooling and optional warming."""
    if db_type == 'postgres':
        url = _get_postgres_connection_url(replica=replica)
        pool_size = CONFIG.get('postgres_staging', {}).get('pool_size', 5)
        max_overflow = CONFIG.get('postgres_staging', {}).get('max_overflow', 10)
    elif db_type == 'sqlserver':
        url = _get_sqlserver_connection_url(replica=replica)
        pool_size = CONFIG.get('sql_server_production', {}).get('pool_size', 5)
        max_overflow = CONFIG.get('sql_server_production', {}).get('max_overflow', 10)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    logger.info(f"Creating engine for {db_type} {'replica' if replica else 'primary'} with pool_size={pool_size}, max_overflow={max_overflow}")
    
    engine = create_engine(
        url,
        poolclass=QueuePool, # Recommended for production
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_recycle=1800,  # Recycle connections every 30 minutes to prevent stale connections
        pool_pre_ping=True, # Enable pre-ping to check connection liveness
        connect_args={'connect_timeout': 10} if db_type == 'postgres' else {} # Example connect_args
    )

    if pool_warming and pool_size > 0:
        _warm_connection_pool(engine, pool_size)
        
    # Add event listener for connection health check (optional, as pool_pre_ping is True)
    # event.listen(engine, "checkout", _ping_connection)

    return engine

def _ping_connection(dbapi_connection, connection_record, connection_proxy):
    """Pings the database to ensure the connection is live."""
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1") # Standard ping query
        logger.debug(f"Connection {connection_record} is live.")
    except Exception as e: # dbapi_connection.OperationalError, etc.
        logger.warning(f"Connection {connection_record} is stale, discarding: {e}")
        # This will raise DisconnectionError, and SQLAlchemy will handle it by trying a new connection.
        raise DisconnectionError()
    finally:
        cursor.close()

def _warm_connection_pool(engine, num_connections_to_warm):
    """Warms the connection pool by creating a specified number of connections."""
    logger.info(f"Warming connection pool for engine {engine.url.database} with {num_connections_to_warm} connections.")
    connections = []
    try:
        for _ in range(num_connections_to_warm):
            conn = engine.connect()
            connections.append(conn)
        logger.info(f"Successfully warmed up {len(connections)} connections.")
    except Exception as e:
        logger.error(f"Error warming connection pool for {engine.url.database}: {e}", exc_info=True)
    finally:
        for conn in connections:
            conn.close() # Return connections to the pool

def init_database_connections():
    """Initializes all database engines and session factories."""
    global _postgres_engine, _postgres_session_factory, _postgres_scoped_session
    global _sqlserver_engine, _sqlserver_session_factory, _sqlserver_scoped_session
    global _postgres_read_replica_engine, _sqlserver_read_replica_engine

    set_correlation_id("DB_INIT") # Correlation ID for initialization phase

    logger.info("Initializing database connections...")
    try:
        # Primary PostgreSQL
        _postgres_engine = _create_db_engine('postgres', pool_warming=True)
        _postgres_session_factory = sessionmaker(bind=_postgres_engine, autoflush=False, autocommit=False)
        _postgres_scoped_session = scoped_session(_postgres_session_factory)
        logger.info(f"PostgreSQL primary engine and session factory initialized for: {_postgres_engine.url.database}")

        # Primary SQL Server
        _sqlserver_engine = _create_db_engine('sqlserver', pool_warming=True)
        _sqlserver_session_factory = sessionmaker(bind=_sqlserver_engine, autoflush=False, autocommit=False)
        _sqlserver_scoped_session = scoped_session(_sqlserver_session_factory)
        logger.info(f"SQL Server primary engine and session factory initialized for: {_sqlserver_engine.url.database}")

        # Optional Read Replicas
        if CONFIG.get('performance', {}).get('enable_read_write_splitting', False):
            if CONFIG.get('performance', {}).get('postgres_read_replica_host'):
                _postgres_read_replica_engine = _create_db_engine('postgres', replica=True, pool_warming=True)
                logger.info(f"PostgreSQL read replica engine initialized for: {_postgres_read_replica_engine.url.database}")
            else:
                logger.info("PostgreSQL read replica not configured. Read operations will use primary.")

            if CONFIG.get('performance', {}).get('sql_server_read_replica_server'):
                _sqlserver_read_replica_engine = _create_db_engine('sqlserver', replica=True, pool_warming=True)
                logger.info(f"SQL Server read replica engine initialized for: {_sqlserver_read_replica_engine.url.database}")
            else:
                logger.info("SQL Server read replica not configured. Read operations will use primary.")

        logger.info("Database connections initialized successfully.")

    except Exception as e:
        logger.critical(f"Failed to initialize database connections: {e}", exc_info=True)
        # Depending on the application, you might want to exit or retry.
        raise  # Re-raise the exception to halt application startup if critical

def get_postgres_session(read_only=False):
    """Provides a SQLAlchemy session for PostgreSQL."""
    if not _postgres_scoped_session:
        logger.error("PostgreSQL session factory not initialized. Call init_database_connections() first.")
        raise RuntimeError("PostgreSQL session factory not initialized.")
    
    if read_only and CONFIG.get('performance', {}).get('enable_read_write_splitting', False) and _postgres_read_replica_engine:
        logger.debug("Using PostgreSQL read replica session.")
        # Create a new session factory for the replica if not already done, or use a separate scoped session
        # For simplicity, creating a new session from a replica-bound factory:
        replica_session_factory = sessionmaker(bind=_postgres_read_replica_engine, autoflush=False, autocommit=False)
        return replica_session_factory() # Not scoped for this example, manage lifecycle carefully
    
    logger.debug("Using PostgreSQL primary session.")
    return _postgres_scoped_session()

def get_sqlserver_session(read_only=False):
    """Provides a SQLAlchemy session for SQL Server."""
    if not _sqlserver_scoped_session:
        logger.error("SQL Server session factory not initialized. Call init_database_connections() first.")
        raise RuntimeError("SQL Server session factory not initialized.")

    if read_only and CONFIG.get('performance', {}).get('enable_read_write_splitting', False) and _sqlserver_read_replica_engine:
        logger.debug("Using SQL Server read replica session.")
        replica_session_factory = sessionmaker(bind=_sqlserver_read_replica_engine, autoflush=False, autocommit=False)
        return replica_session_factory() # Not scoped for this example
        
    logger.debug("Using SQL Server primary session.")
    return _sqlserver_scoped_session()

def close_scoped_sessions():
    """Removes scoped sessions. Typically called at the end of a request in a web app."""
    if _postgres_scoped_session:
        _postgres_scoped_session.remove()
        logger.debug("PostgreSQL scoped session removed.")
    if _sqlserver_scoped_session:
        _sqlserver_scoped_session.remove()
        logger.debug("SQL Server scoped session removed.")

def dispose_engines():
    """Disposes of all engine connection pools. Called at application shutdown."""
    global _postgres_engine, _sqlserver_engine, _postgres_read_replica_engine, _sqlserver_read_replica_engine
    
    if _postgres_engine:
        _postgres_engine.dispose()
        logger.info(f"PostgreSQL primary engine ({_postgres_engine.url.database}) disposed.")
        _postgres_engine = None
    if _sqlserver_engine:
        _sqlserver_engine.dispose()
        logger.info(f"SQL Server primary engine ({_sqlserver_engine.url.database}) disposed.")
        _sqlserver_engine = None
    if _postgres_read_replica_engine:
        _postgres_read_replica_engine.dispose()
        logger.info(f"PostgreSQL read replica engine ({_postgres_read_replica_engine.url.database}) disposed.")
        _postgres_read_replica_engine = None
    if _sqlserver_read_replica_engine:
        _sqlserver_read_replica_engine.dispose()
        logger.info(f"SQL Server read replica engine ({_sqlserver_read_replica_engine.url.database}) disposed.")
        _sqlserver_read_replica_engine = None
    logger.info("All database engines disposed.")

# Perform initialization when the module is loaded,
# or provide a separate setup function to be called by the application's entry point.
# For simplicity here, let's assume it's called explicitly by the app.
# init_database_connections() # Or call this from app/main.py

# Health Check Functions
def check_postgres_connection(session=None):
    """Performs a simple query to check PostgreSQL connection health."""
    close_session_after = False
    if session is None:
        session = get_postgres_session()
        close_session_after = True
    try:
        session.execute("SELECT 1")
        logger.info("PostgreSQL connection health check: OK")
        return True
    except SQLAlchemyError as e:
        logger.error(f"PostgreSQL connection health check FAILED: {e}", exc_info=True)
        return False
    finally:
        if close_session_after:
            session.close()

def check_sqlserver_connection(session=None):
    """Performs a simple query to check SQL Server connection health."""
    close_session_after = False
    if session is None:
        session = get_sqlserver_session()
        close_session_after = True
    try:
        session.execute("SELECT 1")
        logger.info("SQL Server connection health check: OK")
        return True
    except SQLAlchemyError as e:
        logger.error(f"SQL Server connection health check FAILED: {e}", exc_info=True)
        return False
    finally:
        if close_session_after:
            session.close()

# Automatic Reconnection (SQLAlchemy's pool_pre_ping and pool_recycle handle much of this)
# The event listener for checkout with _ping_connection can also help.
# If specific retry logic is needed beyond SQLAlchemy's capabilities, it would be implemented
# in the database handler methods (postgres_handler.py, sqlserver_handler.py).

if __name__ == '__main__':
    # Example usage:
    # This should be called at the very start of your application
    from app.utils.logging_config import setup_logging, set_correlation_id
    setup_logging()
    set_correlation_id("CONN_MAN_TEST")

    try:
        init_database_connections()

        # Test PostgreSQL
        pg_sess = None
        try:
            pg_sess = get_postgres_session()
            if check_postgres_connection(pg_sess):
                logger.info("Successfully connected to PostgreSQL and executed a query.")
                # from app.database.models.postgres_models import Organization # Example query
                # org_count = pg_sess.query(Organization).count()
                # logger.info(f"Found {org_count} organizations in PostgreSQL.")
            else:
                logger.error("PostgreSQL connection check failed after init.")
        except Exception as e:
            logger.error(f"Error during PostgreSQL test: {e}", exc_info=True)
        finally:
            if pg_sess:
                pg_sess.close()
                # For scoped sessions, remove is typically called at end of request
                # _postgres_scoped_session.remove() 

        # Test SQL Server
        sql_sess = None
        try:
            sql_sess = get_sqlserver_session()
            if check_sqlserver_connection(sql_sess):
                logger.info("Successfully connected to SQL Server and executed a query.")
                # from app.database.models.sqlserver_models import ProductionOrganization # Example query
                # prod_org_count = sql_sess.query(ProductionOrganization).count()
                # logger.info(f"Found {prod_org_count} organizations in SQL Server.")
            else:
                logger.error("SQL Server connection check failed after init.")
        except Exception as e:
            logger.error(f"Error during SQL Server test: {e}", exc_info=True)
        finally:
            if sql_sess:
                sql_sess.close()
                # _sqlserver_scoped_session.remove()

    except Exception as e:
        logger.critical(f"Critical error in connection manager example: {e}", exc_info=True)
    finally:
        dispose_engines()
        logger.info("Connection manager example finished.")