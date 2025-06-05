# app/main.py
"""
Main application entry point for the EDI Claims Processor.
Orchestrates the overall workflow:
1. Initialize logging, configurations, and database connections (with pool warming).
2. Initialize monitoring services.
3. Watch a directory for new EDI files (or process specified files).
4. For each file, parse EDI content.
5. Add parsed claims to the PostgreSQL staging database.
6. Initiate batch processing of staged claims.
   - Batch processor fetches claims and uses ClaimsProcessor for each.
   - ClaimsProcessor handles validation, ML prediction, reimbursement, and saving to production/failed logs.
7. Ensure graceful shutdown of all resources.
"""
import os
import sys
import time
import argparse
import glob # For finding files
from datetime import datetime # Added datetime for batch name
import asyncio # Added for asyncio.run and main_async_wrapper
import multiprocessing # Added for freeze_support and platform config

# Ensure app directory is in Python path (if running main.py directly from project root)
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, '..'))
# if project_root not in sys.path:
#    sys.path.insert(0, project_root)

from app.utils.logging_config import setup_logging, get_logger, set_correlation_id, get_correlation_id # Corrected import
from app.utils.error_handler import AppException, ConfigError, handle_exception # handle_exception can be used for generic cases
from app.database.connection_manager import (
    init_database_connections, dispose_engines,
    get_postgres_session, # Used for direct session needs in main if any
    get_sqlserver_session, # Not directly used in main after refactor, batch_handler uses factories
    CONFIG as APP_CONFIG # Load config once from connection_manager
)
from app.processing.edi_parser import process_edi_file
# Using OptimizedPipelineProcessor from batch_handler
from app.processing.batch_handler import OptimizedPipelineProcessor # Removed HAS_PLATFORM_CONFIG, configure_platform_mp from here
# Import PLATFORM_CONFIG and related utils directly
from app.utils.platform_config import PLATFORM_CONFIG, configure_multiprocessing as configure_platform_mp, HAS_PLATFORM_CONFIG

from app.database.postgres_handler import create_db_processing_batch # For creating a main batch record

# Monitoring imports
from app.utils.metrics import get_metrics_collector
from app.monitoring.performance_monitor import PerformanceMonitor
from app.monitoring.health_checker import HealthChecker
from app.monitoring.alert_manager import AlertManager
from app.utils.caching import shutdown_caches # For explicit cache shutdown

# Setup initial logging as early as possible
# This ensures that even early configuration errors are logged.
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
        metrics_collector_instance = get_metrics_collector(config) # Pass config for its own settings
        if metrics_collector_instance and config.get('metrics',{}).get('auto_start_background', True):
            metrics_collector_instance.start_background_processing()
            logger.info(f"[{cid}] MetricsCollector background processing started.")

        performance_monitor_instance = PerformanceMonitor(config) # Pass full config
        if performance_monitor_instance and config.get('monitoring',{}).get('system_monitoring_enabled', True):
            # performance_monitor_instance.start_system_monitoring() is called in its __init__
            logger.info(f"[{cid}] PerformanceMonitor system monitoring started.")

        # HealthChecker might be used by AlertManager or other components
        health_checker_instance = HealthChecker(config) # Pass full config

        alert_manager_instance = AlertManager(config, health_checker_instance, performance_monitor_instance)
        if alert_manager_instance and config.get('alerting',{}).get('auto_start_monitoring', True):
            # alert_manager_instance.start_monitoring() is called in its __init__
            logger.info(f"[{cid}] AlertManager monitoring started.")
        
        logger.info(f"[{cid}] Monitoring services initialized.")
    except Exception as e:
        logger.error(f"[{cid}] Failed to initialize one or more monitoring services: {e}", exc_info=True)
        # Depending on policy, might raise an error or continue with degraded monitoring

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


def process_incoming_edi_files(source_directory: str, pg_session_factory):
    """
    Scans a source directory for EDI files, processes each one to parse
    and stage the claims. Uses a session factory for database operations.
    """
    main_cid = get_correlation_id() # Inherit or set new if needed
    run_ingestion_cid = set_correlation_id(f"{main_cid}_EDI_INGEST")
    logger.info(f"[{run_ingestion_cid}] Starting EDI file ingestion from directory: {source_directory}")

    if not os.path.isdir(source_directory):
        logger.error(f"[{run_ingestion_cid}] Source directory '{source_directory}' not found or is not a directory.")
        return

    edi_extensions = ["*.edi", "*.txt", "*.x12", "*.837"] 
    edi_files = []
    for ext in edi_extensions:
        edi_files.extend(glob.glob(os.path.join(source_directory, ext)))
    
    if not edi_files:
        logger.info(f"[{run_ingestion_cid}] No EDI files with extensions {edi_extensions} found in '{source_directory}'.")
        return

    logger.info(f"[{run_ingestion_cid}] Found {len(edi_files)} EDI files to process.")
    
    total_claims_parsed_run = 0
    total_claims_failed_parsing_run = 0
    files_processed_count = 0

    for edi_file_path in edi_files:
        file_processing_cid = set_correlation_id(f"{run_ingestion_cid}_FILE_{os.path.basename(edi_file_path)[:15]}")
        logger.info(f"[{file_processing_cid}] Processing file: {edi_file_path}")
        pg_session = None
        try:
            pg_session = pg_session_factory() # Get a new session for this file
            parsed_count, failed_count = process_edi_file(edi_file_path, pg_session)
            
            total_claims_parsed_run += parsed_count
            total_claims_failed_parsing_run += failed_count
            files_processed_count +=1
            
            # process_edi_file commits parsed claims from one file.
            logger.info(f"[{file_processing_cid}] Finished processing file {edi_file_path}. Parsed: {parsed_count}, Failed map/parse: {failed_count}")
            
        except Exception as e_file:
            logger.error(f"[{file_processing_cid}] Critical error processing file {edi_file_path}: {e_file}", exc_info=True)
            total_claims_failed_parsing_run += 1 
            if pg_session and pg_session.is_active: # Check if session exists and is active
                try:
                    pg_session.rollback() 
                    logger.info(f"[{file_processing_cid}] Rolled back transaction for file {edi_file_path} due to error.")
                except Exception as rb_err:
                    logger.error(f"[{file_processing_cid}] Error during rollback for file {edi_file_path}: {rb_err}", exc_info=True)
        finally:
            if pg_session:
                try:
                    pg_session.close()
                except Exception as close_err:
                    logger.warning(f"[{file_processing_cid}] Error closing session for file {edi_file_path}: {close_err}")
            set_correlation_id(run_ingestion_cid) # Restore parent CID for this run

    logger.info(f"[{run_ingestion_cid}] EDI file ingestion run finished. Files processed: {files_processed_count}.")
    logger.info(f"Total claims successfully parsed and staged in this run: {total_claims_parsed_run}")
    logger.info(f"Total claims/files that failed parsing or mapping in this run: {total_claims_failed_parsing_run}")
    set_correlation_id(main_cid) # Restore overall main CID


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
        # Ensure pipeline attempts to stop on critical error
        if pipeline_processor_instance and not pipeline_processor_instance.shutdown_event.is_set():
            await pipeline_processor_instance.stop_pipeline()
    finally:
        # Cleanup is handled within run_pipeline's finally block
        logger.info(f"[{pipeline_run_cid}] Pipeline processing finalization.")
        set_correlation_id(main_cid)

async def main_async_wrapper(args):
    """Wraps the main logic to be run by asyncio.run() for pipeline processing."""
    overall_cid = get_correlation_id() # Inherit from main
    
    # These factories will be used by OptimizedPipelineProcessor for thread/task-safe sessions
    pg_session_factory = lambda read_only=False: get_postgres_session(read_only=read_only)
    sql_session_factory = lambda read_only=False: get_sqlserver_session(read_only=read_only) # SQL factory usually for write

    if args.run_ingestion or args.run_all_processing:
        if args.source_dir:
            logger.info("--- Starting EDI File Ingestion Phase ---")
            # Run synchronous ingestion in a thread pool executor to not block asyncio loop if pipeline runs concurrently
            # For simplicity, if run_all_processing, it runs sequentially.
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, process_incoming_edi_files, args.source_dir, pg_session_factory)
            logger.info("--- EDI File Ingestion Phase Completed ---")
    
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
    
    if HAS_PLATFORM_CONFIG: # PLATFORM_CONFIG is now imported directly
        logger.info(f"Running with platform-specific configurations from app.utils.platform_config: {PLATFORM_CONFIG}")
        configure_platform_mp() # Configure multiprocessing based on platform
    else:
        logger.warning("Platform configuration module not fully available. Using fallback multiprocessing setup.")
        if sys.platform == "win32": # Fallback freeze_support for Windows if platform_config was minimal
            multiprocessing.freeze_support()


    parser = argparse.ArgumentParser(description="EDI Claims Processor")
    parser.add_argument("--source_dir", type=str, help="Directory containing EDI files to process for ingestion.")
    parser.add_argument("--run_ingestion", action="store_true", help="Run the EDI file ingestion phase.")
    parser.add_argument("--run_batch_processing", action="store_true", help="Run the claim batch processing pipeline.")
    parser.add_argument("--run_api", action="store_true", help="Run the FastAPI API server for UI interaction.")
    parser.add_argument("--run_all_processing", action="store_true", help="Run all processing phases (ingestion then batch processing pipeline).")
    
    args = parser.parse_args()

    action_specified = args.run_ingestion or args.run_batch_processing or args.run_all_processing or args.run_api
    if not action_specified:
        logger.warning("No action specified. Use --run_ingestion, --run_batch_processing, --run_all_processing, or --run_api.")
        parser.print_help()
        sys.exit(1)
        
    if (args.run_ingestion or args.run_all_processing) and not args.source_dir:
        logger.error("--source_dir is required when --run_ingestion or --run_all_processing is specified.")
        parser.print_help()
        sys.exit(1)

    try:
        # Initialize database connections (includes pool warming)
        init_database_connections() 
        logger.info(f"[{overall_cid}] Database connections initialized and pools warmed.")

        # Initialize monitoring services
        initialize_monitoring_services(APP_CONFIG) # Pass loaded app config

        if args.run_api:
            logger.info("--- Starting FastAPI API Server ---")
            try:
                import uvicorn
                from app.api.main import app as fastapi_app # Import the FastAPI app instance
                
                api_host = APP_CONFIG.get('api',{}).get('host', '0.0.0.0')
                api_port = APP_CONFIG.get('api',{}).get('port', 8000)
                logger.info(f"Starting Uvicorn server for API on {api_host}:{api_port}...")
                # Uvicorn runs its own asyncio loop.
                # If other async tasks need to run alongside, consider tools like `hypercorn` or structured concurrency.
                uvicorn.run(fastapi_app, host=api_host, port=api_port, lifespan="on") # lifespan="on" is important for startup/shutdown events
            except ImportError:
                logger.error("Uvicorn or FastAPI is not installed. Cannot run the API server. Please install requirements.")
                sys.exit(5) # Specific exit code for missing API deps
            except Exception as api_e:
                logger.critical(f"Failed to start API server: {api_e}", exc_info=True)
                sys.exit(6) # Specific exit code for API startup failure
        else:
            # Run processing tasks (ingestion and/or batch processing pipeline)
            # Use asyncio for the pipeline part.
            asyncio.run(main_async_wrapper(args))
            logger.info(f"[{overall_cid}] EDI Claims Processor application tasks finished successfully.")

    except ConfigError as ce:
        logger.critical(f"[{overall_cid}] CRITICAL CONFIGURATION ERROR: {ce.message}", exc_info=True)
        sys.exit(2) # Standard exit code for configuration errors
    except AppException as ae:
        logger.critical(f"[{overall_cid}] CRITICAL APPLICATION ERROR: {ae.message} (Code: {ae.error_code})", exc_info=True)
        sys.exit(3) # Standard exit code for application-specific errors
    except KeyboardInterrupt:
        logger.info(f"[{overall_cid}] Application interrupted by user (Ctrl+C). Initiating graceful shutdown...")
        # Graceful shutdown for pipeline if it was running
        if pipeline_processor_instance and not pipeline_processor_instance.shutdown_event.is_set():
            logger.info(f"[{overall_cid}] Attempting to stop pipeline processor due to KeyboardInterrupt...")
            # Running async shutdown in a new loop if main one was interrupted
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(pipeline_processor_instance.stop_pipeline())
            except Exception as e_pipe_stop:
                logger.error(f"[{overall_cid}] Error stopping pipeline processor during KeyboardInterrupt: {e_pipe_stop}")
            finally:
                loop.close()
        sys.exit(130) # Standard exit code for Ctrl+C
    except Exception as e:
        # Catch any other unexpected exceptions
        logger.critical(f"[{overall_cid}] AN UNEXPECTED CRITICAL ERROR OCCURRED IN MAIN: {e}", exc_info=True)
        sys.exit(4) # General critical error
    finally:
        # Graceful shutdown of all resources
        shutdown_cid = set_correlation_id(f"{overall_cid}_SHUTDOWN")
        logger.info(f"[{shutdown_cid}] Initiating final application shutdown procedures...")
        
        # Shutdown pipeline processor if it was initialized and not an API run (API handles its own lifespan)
        if pipeline_processor_instance and not args.run_api and not pipeline_processor_instance.shutdown_event.is_set():
            logger.info(f"[{shutdown_cid}] Ensuring pipeline processor is stopped...")
            loop = asyncio.get_event_loop() # Get the current loop
            # Check if loop is running only if it's not None and not closed.
            # asyncio.run() manages its own loop creation and closing.
            # If pipeline_processor.stop_pipeline() is async, it should be awaited or run in a loop.
            if loop and not loop.is_closed() and loop.is_running():
                # If inside an already running loop (e.g., if main itself was async and not using asyncio.run for wrapper)
                asyncio.create_task(pipeline_processor_instance.stop_pipeline())
            else:
                # If no loop is running, or if the previous asyncio.run completed,
                # we might need a new loop context to run the async stop_pipeline method.
                # However, if asyncio.run(main_async_wrapper) completed, stop_pipeline should have been called there.
                # This final check is more of a safeguard.
                try:
                    temp_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(temp_loop)
                    temp_loop.run_until_complete(pipeline_processor_instance.stop_pipeline())
                    temp_loop.close()
                except RuntimeError: # Handles cases where a loop might already be set or closed
                     logger.warning(f"[{shutdown_cid}] Could not cleanly run stop_pipeline in a new loop during final shutdown.")
                except Exception as e_final_stop:
                    logger.error(f"[{shutdown_cid}] Error during final stop of pipeline processor: {e_final_stop}")


        shutdown_monitoring_services()
        shutdown_caches() # Explicitly shut down caches if needed

        # Dispose database engines
        # FastAPI handles its own dispose_engines via lifespan event if API was run.
        if not args.run_api: 
            dispose_engines() 
        
        logger.info(f"[{shutdown_cid}] Application shutdown process complete.")
        set_correlation_id(overall_cid) # Restore for any final logging from Python itself

if __name__ == "__main__":
    # Platform-specific setup for multiprocessing, especially for Windows when __name__ is involved.
    # This is critical if ProcessPoolExecutor is used and the script is run directly.
    # The `configure_platform_mp()` or its fallback should handle freeze_support() and set_start_method('spawn').
    if sys.platform == "win32":
        if HAS_PLATFORM_CONFIG: # PLATFORM_CONFIG is now imported directly
            configure_platform_mp() # Should call freeze_support if applicable
        else: # Minimal fallback
            multiprocessing.freeze_support() 
            # set_start_method might be needed here too if ProcessPoolExecutor is used without platform_config
            # For now, assuming ThreadPoolExecutor is the safe default on Windows if platform_config is missing.

    main()
