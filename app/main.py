# app/main.py
"""
Main application entry point for the EDI Claims Processor.
Orchestrates the overall workflow:
1. Initialize logging, configurations, and database connections.
2. Watch a directory for new EDI files (or process specified files).
3. For each file, parse EDI content.
4. Add parsed claims to the PostgreSQL staging database.
5. Initiate batch processing of staged claims.
   - Batch processor fetches claims and uses ClaimsProcessor for each.
   - ClaimsProcessor handles validation, ML prediction, reimbursement, and saving to production/failed logs.
"""
import os
import sys
import time
import argparse
import glob # For finding files
from datetime import datetime, timedelta # Added datetime for batch name, timedelta for batch handler test

# Ensure app directory is in Python path (if running main.py directly from project root)
# This might be needed if you run `python app/main.py` from the project root.
# If you run `python -m app.main` from the project root, it's usually handled.
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, '..'))
# if project_root not in sys.path:
#    sys.path.insert(0, project_root)

from app.utils.logging_config import setup_logging, get_logger, set_correlation_id
from app.utils.error_handler import AppException, ConfigError
from app.database.connection_manager import (
    init_database_connections, dispose_engines,
    get_postgres_session, get_sqlserver_session, CONFIG as APP_CONFIG # Load config once
)
from app.processing.edi_parser import process_edi_file
from app.processing.batch_handler import BatchProcessor
from app.database.postgres_handler import create_db_processing_batch # For creating a main batch record

# Setup initial logging as early as possible
setup_logging() # Reads from config/config.yaml by default
logger = get_logger('app.main')

def process_incoming_edi_files(source_directory: str):
    """
    Scans a source directory for EDI files, processes each one to parse
    and stage the claims.
    """
    main_cid = set_correlation_id("EDI_FILE_INGESTION_RUN")
    logger.info(f"[{main_cid}] Starting EDI file ingestion from directory: {source_directory}")

    if not os.path.isdir(source_directory):
        logger.error(f"Source directory '{source_directory}' not found or is not a directory.")
        return

    # Common EDI file extensions
    edi_extensions = ["*.edi", "*.txt", "*.x12", "*.837"] 
    edi_files = []
    for ext in edi_extensions:
        edi_files.extend(glob.glob(os.path.join(source_directory, ext)))
    
    if not edi_files:
        logger.info(f"No EDI files with extensions {edi_extensions} found in '{source_directory}'.")
        return

    logger.info(f"Found {len(edi_files)} EDI files to process: {edi_files}")
    
    total_claims_parsed_run = 0
    total_claims_failed_parsing_run = 0
    files_processed_count = 0

    pg_session = None
    try:
        pg_session = get_postgres_session()

        for edi_file_path in edi_files:
            file_cid = set_correlation_id(f"{main_cid}_FILE_{os.path.basename(edi_file_path)[:15]}")
            logger.info(f"[{file_cid}] Processing file: {edi_file_path}")
            try:
                # process_edi_file is expected to handle its own transaction for claims within that file
                # or to prepare them for a larger batch commit if designed that way.
                # The current edi_parser.process_edi_file implies it commits parsed claims from one file.
                parsed_count, failed_count = process_edi_file(edi_file_path, pg_session)
                
                total_claims_parsed_run += parsed_count
                total_claims_failed_parsing_run += failed_count
                files_processed_count +=1
                logger.info(f"[{file_cid}] Finished processing file {edi_file_path}. Parsed: {parsed_count}, Failed map/parse: {failed_count}")
                
                # If process_edi_file doesn't commit, commit here per file:
                # pg_session.commit() 
                # logger.info(f"[{file_cid}] Committed changes for file {edi_file_path}.")

            except Exception as e_file:
                logger.error(f"[{file_cid}] Critical error processing file {edi_file_path}: {e_file}", exc_info=True)
                total_claims_failed_parsing_run += 1 # Count file as failed
                if pg_session.is_active:
                    pg_session.rollback() 
                    logger.info(f"[{file_cid}] Rolled back transaction for file {edi_file_path} due to error.")
            finally:
                set_correlation_id(main_cid) 

        logger.info(f"[{main_cid}] EDI file ingestion run finished. Files processed: {files_processed_count}.")
        logger.info(f"Total claims successfully parsed and staged in this run: {total_claims_parsed_run}")
        logger.info(f"Total claims/files that failed parsing or mapping in this run: {total_claims_failed_parsing_run}")

    except Exception as e:
        logger.critical(f"[{main_cid}] Unhandled exception during EDI file ingestion phase: {e}", exc_info=True)
        if pg_session and pg_session.is_active:
            pg_session.rollback()
    finally:
        if pg_session:
            pg_session.close()
        set_correlation_id(main_cid)


def start_claim_processing_batches():
    """
    Initiates the batch processing of claims that have been staged.
    """
    main_cid = set_correlation_id("CLAIM_BATCH_PROCESSING_RUN")
    logger.info(f"[{main_cid}] Starting claim batch processing run.")

    # These factories will be used by BatchProcessor to create sessions for worker threads
    pg_session_factory = lambda: get_postgres_session(read_only=False)
    sql_session_factory = lambda: get_sqlserver_session(read_only=False)

    batch_processor = BatchProcessor(pg_session_factory, sql_session_factory, APP_CONFIG)
    
    pg_session_for_loop_control = None
    try:
        pg_session_for_loop_control = pg_session_factory()
        
        # Optional: Create a main batch record in the database for this overall run
        # This batch_id_in_db isn't directly used by run_main_batch_processing_loop in current BatchHandler,
        # but could be passed for logging or if BatchHandler was to update a master batch record.
        batch_name_for_run = f"MainBatchRun_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        # batch_id_in_db = create_db_processing_batch(pg_session_for_loop_control, batch_name_for_run)
        # pg_session_for_loop_control.commit()
        # logger.info(f"Master batch record created with ID: {batch_id_in_db} for this run: {batch_name_for_run}")

        # run_main_batch_processing_loop will fetch its own batches internally.
        # It needs a session to start its loop, but workers get sessions from factory.
        batch_processor.run_main_batch_processing_loop(pg_session_for_loop_control) 
        
        logger.info(f"[{main_cid}] Claim batch processing run completed.")

    except Exception as e:
        logger.critical(f"[{main_cid}] Unhandled exception during claim batch processing phase: {e}", exc_info=True)
        # Rollback if this session was doing something transactional, though it's mostly for control here.
        if pg_session_for_loop_control and pg_session_for_loop_control.is_active:
            pg_session_for_loop_control.rollback()
    finally:
        if pg_session_for_loop_control:
            pg_session_for_loop_control.close()
        batch_processor.shutdown_executors() 
        set_correlation_id(main_cid)


def main():
    """
    Main entry point for the application.
    Parses command line arguments and orchestrates the processing flow.
    """
    overall_cid = set_correlation_id("EDI_PROC_APP_RUN") 
    logger.info(f"[{overall_cid}] EDI Claims Processor application starting...")
    
    parser = argparse.ArgumentParser(description="EDI Claims Processor")
    parser.add_argument("--source_dir", type=str, help="Directory containing EDI files to process for ingestion.")
    parser.add_argument("--run_ingestion", action="store_true", help="Run the EDI file ingestion phase.")
    parser.add_argument("--run_batch_processing", action="store_true", help="Run the claim batch processing phase.")
    parser.add_argument("--run_api", action="store_true", help="Run the FastAPI API server for UI interaction.")
    parser.add_argument("--run_all_processing", action="store_true", help="Run all processing phases (ingestion then batch processing).")
    
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
        init_database_connections() 

        if args.run_ingestion or args.run_all_processing:
            if args.source_dir:
                logger.info("--- Starting EDI File Ingestion Phase ---")
                process_incoming_edi_files(args.source_dir)
                logger.info("--- EDI File Ingestion Phase Completed ---")
        
        if args.run_batch_processing or args.run_all_processing:
            logger.info("--- Starting Claim Batch Processing Phase ---")
            start_claim_processing_batches()
            logger.info("--- Claim Batch Processing Phase Completed ---")
        
        if args.run_api:
            logger.info("--- Starting FastAPI API Server ---")
            # Import uvicorn here to avoid making it a hard dependency if only processing is run
            try:
                import uvicorn
                from app.api.main import app as fastapi_app # Import the FastAPI app instance
                
                api_host = APP_CONFIG.get('api',{}).get('host', '0.0.0.0')
                api_port = APP_CONFIG.get('api',{}).get('port', 8000)
                logger.info(f"Starting Uvicorn server for API on {api_host}:{api_port}...")
                uvicorn.run(fastapi_app, host=api_host, port=api_port) # reload=True for dev
            except ImportError:
                logger.error("Uvicorn is not installed. Cannot run the API server. Please install with `pip install uvicorn`.")
            except Exception as api_e:
                logger.critical(f"Failed to start API server: {api_e}", exc_info=True)


        if not args.run_api: # Only log successful finish if not running API (which blocks)
             logger.info(f"[{overall_cid}] EDI Claims Processor application tasks finished successfully.")

    except ConfigError as ce:
        logger.critical(f"[{overall_cid}] Configuration error: {ce.message}", exc_info=True)
        sys.exit(2)
    except AppException as ae:
        logger.critical(f"[{overall_cid}] Application error: {ae.message} (Code: {ae.error_code})", exc_info=True)
        sys.exit(3)
    except KeyboardInterrupt:
        logger.info(f"[{overall_cid}] Application interrupted by user (Ctrl+C). Shutting down...")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"[{overall_cid}] An unexpected critical error occurred in main: {e}", exc_info=True)
        sys.exit(4)
    finally:
        # Dispose engines only if not running API, as API manages its own lifecycle for connections
        if not args.run_api:
            dispose_engines() 
            logger.info(f"[{overall_cid}] Application shutdown complete (non-API mode).")
        # If API was run, dispose_engines is called by FastAPI's shutdown event.
        set_correlation_id(overall_cid)

if __name__ == "__main__":
    main()