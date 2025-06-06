# app/main.py
"""
Main application entry point for the EDI Claims Processor.
Orchestrates the overall workflow:
1. Initialize logging, configurations, and database connections (with pool warming).
2. Initialize monitoring services.
3. Initiate batch processing of staged claims from the PostgreSQL database.
   - Batch processor fetches claims and uses ClaimsProcessor for each.
   - ClaimsProcessor handles validation, ML prediction, reimbursement, and saving to production/failed logs.
4. Ensure graceful shutdown of all resources.
"""
import os
import sys
import time
import argparse
import asyncio
import multiprocessing

# Ensure app directory is in Python path (if running main.py directly from project root)
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, '..'))
# if project_root not in sys.path:
#    sys.path.insert(0, project_root)

from app.utils.logging_config import setup_logging, get_logger, set_correlation_id, get_correlation_id
from app.utils.error_handler import AppException, ConfigError, handle_exception
from app.database.connection_manager import (
    init_database_connections, dispose_engines,
    get_postgres_session,
    get_sqlserver_session,
    CONFIG as APP_CONFIG
)
# The edi_parser module is no longer needed as we are not processing files.
# from app.processing.edi_parser import process_edi_file 
from app.processing.batch_handler import OptimizedPipelineProcessor
from app.utils.platform_config import PLATFORM_CONFIG, configure_multiprocessing as configure_platform_mp, HAS_PLATFORM_CONFIG

from app.database.postgres_handler import create_db_processing_batch

# Monitoring imports
from app.utils.metrics import get_metrics_collector
from app.monitoring.performance_monitor import PerformanceMonitor
from app.monitoring.health_checker import HealthChecker
from app.monitoring.alert_manager import AlertManager
from app.utils.caching import shutdown_caches

# Setup initial logging as early as possible
setup_logging() 
logger = get_logger('app.main')

# Global references to monitoring components for graceful shutdown
metrics_collector_instance = None
performance_monitor_instance = None
alert_manager_instance = None
pipeline_processor_instance = None # For OptimizedPipelineProcessor

def initialize_monitoring_services(config: dict):
    """Initializes and starts monitoring services."""
    global metrics_collector_instance, performance_monitor_instance, alert_manager_instance
    cid = get_correlation_id()
    logger.info(f"[{cid}] Initializing monitoring services...")
    try:
        metrics_collector_instance = get_metrics_collector(config)
        if metrics_collector_instance and config.get('metrics',{}).get('auto_start_background', True):
            metrics_collector_instance.start_background_processing()
            logger.info(f"[{cid}] MetricsCollector background processing started.")

        performance_monitor_instance = PerformanceMonitor(config)
        if performance_monitor_instance and config.get('monitoring',{}).get('system_monitoring_enabled', True):
            logger.info(f"[{cid}] PerformanceMonitor system monitoring started.")

        health_checker_instance = HealthChecker(config)

        alert_manager_instance = AlertManager(config, health_checker_instance, performance_monitor_instance)
        if alert_manager_instance and config.get('alerting',{}).get('auto_start_monitoring', True):
            logger.info(f"[{cid}] AlertManager monitoring started.")
        
        logger.info(f"[{cid}] Monitoring services initialized.")
    except Exception as e:
        logger.error(f"[{cid}] Failed to initialize one or more monitoring services: {e}", exc_info=True)

def shutdown_monitoring_services():
    """Gracefully shuts down monitoring services."""
    cid = get_correlation_id()
    logger.info(f"[{cid}] Shutting down monitoring services...")
    if metrics_collector_instance:
        try:
            metrics_collector_instance.stop_background_processing()
            logger.info(f"[{cid}] MetricsCollector background processing stopped.")
        except Exception as e:
            logger.warning(f"[{cid}] Error stopping MetricsCollector: {e}", exc_info=True)
    if performance_monitor_instance:
        try:
            performance_monitor_instance.stop_system_monitoring()
            logger.info(f"[{cid}] PerformanceMonitor system monitoring stopped.")
        except Exception as e:
            logger.warning(f"[{cid}] Error stopping PerformanceMonitor: {e}", exc_info=True)
    if alert_manager_instance:
        try:
            alert_manager_instance.stop_monitoring()
            logger.info(f"[{cid}] AlertManager monitoring stopped.")
        except Exception as e:
            logger.warning(f"[{cid}] Error stopping AlertManager: {e}", exc_info=True)
    logger.info(f"[{cid}] Monitoring services shutdown complete.")


async def start_claim_processing_pipeline(pg_session_factory, sql_session_factory, app_config):
    """
    Initiates the asynchronous batch processing pipeline for staged claims.
    """
    global pipeline_processor_instance
    main_cid = get_correlation_id()
    pipeline_run_cid = set_correlation_id(f"{main_cid}_PIPELINE_RUN")
    logger.info(f"[{pipeline_run_cid}] Starting asynchronous claim batch processing pipeline.")

    pipeline_processor_instance = OptimizedPipelineProcessor(pg_session_factory, sql_session_factory, app_config)
    
    try:
        await pipeline_processor_instance.run_pipeline()
        logger.info(f"[{pipeline_run_cid}] Asynchronous claim batch processing pipeline completed.")
    except asyncio.CancelledError:
        logger.info(f"[{pipeline_run_cid}] Claim processing pipeline was cancelled.")
    except Exception as e:
        logger.critical(f"[{pipeline_run_cid}] Unhandled exception during claim batch processing pipeline: {e}", exc_info=True)
        if pipeline_processor_instance and not pipeline_processor_instance.shutdown_event.is_set():
            await pipeline_processor_instance.stop_pipeline()
    finally:
        logger.info(f"[{pipeline_run_cid}] Pipeline processing finalization.")
        set_correlation_id(main_cid)

async def main_async_wrapper(args):
    """Wraps the main logic to be run by asyncio.run() for pipeline processing."""
    overall_cid = get_correlation_id()

    pg_session_factory = lambda read_only=False: get_postgres_session(read_only=read_only)
    sql_session_factory = lambda read_only=False: get_sqlserver_session(read_only=read_only)

    # The file ingestion logic has been removed. The pipeline will start processing directly from the database.
    if args.run_batch_processing or args.run_all_processing:
        logger.info("--- Starting Claim Batch Processing Pipeline ---")
        await start_claim_processing_pipeline(pg_session_factory, sql_session_factory, APP_CONFIG)
        logger.info("--- Claim Batch Processing Pipeline Completed ---")


def main():
    """
    Main entry point for the application.
    Parses command line arguments and orchestrates the processing flow.
    """
    overall_cid = set_correlation_id("EDI_PROC_APP_RUN_MAIN") 
    logger.info(f"[{overall_cid}] EDI Claims Processor application starting...")
    
    if HAS_PLATFORM_CONFIG:
        logger.info(f"Running with platform-specific configurations from app.utils.platform_config: {PLATFORM_CONFIG}")
        configure_platform_mp()
    else:
        logger.warning("Platform configuration module not fully available. Using fallback multiprocessing setup.")
        if sys.platform == "win32":
            multiprocessing.freeze_support()

    parser = argparse.ArgumentParser(description="EDI Claims Processor")
    # Arguments related to file ingestion have been removed.
    parser.add_argument("--run_batch_processing", action="store_true", help="Run the claim batch processing pipeline from the database.")
    parser.add_argument("--run_api", action="store_true", help="Run the FastAPI API server for UI interaction.")
    parser.add_argument("--run_all_processing", action="store_true", help="Run the full claim processing pipeline from the database.")
    
    args = parser.parse_args()

    action_specified = args.run_batch_processing or args.run_all_processing or args.run_api
    if not action_specified:
        logger.warning("No action specified. Use --run_batch_processing, --run_all_processing, or --run_api.")
        parser.print_help()
        sys.exit(1)
        
    try:
        init_database_connections() 
        logger.info(f"[{overall_cid}] Database connections initialized and pools warmed.")

        initialize_monitoring_services(APP_CONFIG)

        if args.run_api:
            logger.info("--- Starting FastAPI API Server ---")
            try:
                import uvicorn
                from app.api.main import app as fastapi_app
                
                api_host = APP_CONFIG.get('api',{}).get('host', '0.0.0.0')
                api_port = APP_CONFIG.get('api',{}).get('port', 8000)
                logger.info(f"Starting Uvicorn server for API on {api_host}:{api_port}...")
                uvicorn.run(fastapi_app, host=api_host, port=api_port, lifespan="on")
            except ImportError:
                logger.error("Uvicorn or FastAPI is not installed. Cannot run the API server. Please install requirements.")
                sys.exit(5)
            except Exception as api_e:
                logger.critical(f"Failed to start API server: {api_e}", exc_info=True)
                sys.exit(6)
        else:
            asyncio.run(main_async_wrapper(args))
            logger.info(f"[{overall_cid}] EDI Claims Processor application tasks finished successfully.")

    except ConfigError as ce:
        logger.critical(f"[{overall_cid}] CRITICAL CONFIGURATION ERROR: {ce.message}", exc_info=True)
        sys.exit(2)
    except AppException as ae:
        logger.critical(f"[{overall_cid}] CRITICAL APPLICATION ERROR: {ae.message} (Code: {ae.error_code})", exc_info=True)
        sys.exit(3)
    except KeyboardInterrupt:
        logger.info(f"[{overall_cid}] Application interrupted by user (Ctrl+C). Initiating graceful shutdown...")
        if pipeline_processor_instance and not pipeline_processor_instance.shutdown_event.is_set():
            logger.info(f"[{overall_cid}] Attempting to stop pipeline processor due to KeyboardInterrupt...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(pipeline_processor_instance.stop_pipeline())
            except Exception as e_pipe_stop:
                logger.error(f"[{overall_cid}] Error stopping pipeline processor during KeyboardInterrupt: {e_pipe_stop}")
            finally:
                loop.close()
        sys.exit(130)
    except Exception as e:
        logger.critical(f"[{overall_cid}] AN UNEXPECTED CRITICAL ERROR OCCURRED IN MAIN: {e}", exc_info=True)
        sys.exit(4)
    finally:
        shutdown_cid = set_correlation_id(f"{overall_cid}_SHUTDOWN")
        logger.info(f"[{shutdown_cid}] Initiating final application shutdown procedures...")
        
        if pipeline_processor_instance and not args.run_api and not pipeline_processor_instance.shutdown_event.is_set():
            logger.info(f"[{shutdown_cid}] Ensuring pipeline processor is stopped...")
            try:
                temp_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(temp_loop)
                temp_loop.run_until_complete(pipeline_processor_instance.stop_pipeline())
                temp_loop.close()
            except RuntimeError:
                 logger.warning(f"[{shutdown_cid}] Could not cleanly run stop_pipeline in a new loop during final shutdown.")
            except Exception as e_final_stop:
                logger.error(f"[{shutdown_cid}] Error during final stop of pipeline processor: {e_final_stop}")

        shutdown_monitoring_services()
        shutdown_caches()

        if not args.run_api: 
            dispose_engines() 
        
        logger.info(f"[{shutdown_cid}] Application shutdown process complete.")
        set_correlation_id(overall_cid)

if __name__ == "__main__":
    if sys.platform == "win32":
        if HAS_PLATFORM_CONFIG:
            configure_platform_mp()
        else:
            multiprocessing.freeze_support() 
    main()
