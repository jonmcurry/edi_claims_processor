# app/main.py
"""
Main entry point for the EDI Claims Processor application.
Handles both API startup (via uvicorn) and batch processing execution.
"""
import argparse
import sys
import os

# Add the project root to the Python path
# This is crucial for running this script as a module from the root directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.utils.logging_config import setup_logging, get_logger, set_correlation_id
from app.database.connection_manager import init_database_connections, dispose_engines, get_postgres_session, get_sqlserver_session, CONFIG as APP_CONFIG
from app.processing.batch_handler import BatchProcessor

# Setup logging at the application's entry point
setup_logging()
logger = get_logger('app.main')

def run_batch_processing():
    """Initializes and runs the main batch processing loop."""
    main_cid = set_correlation_id("BATCH_PROCESSING_RUN")
    logger.info(f"[{main_cid}] Starting main batch processing run...")

    try:
        # Initialize database connections which are used by the batch processor
        init_database_connections()
        
        # The session factories are needed by the BatchProcessor.
        # CORRECTED: The lambda functions now correctly accept the `read_only` keyword argument
        # and pass it to the underlying session creation functions.
        pg_session_factory = lambda read_only=False: get_postgres_session(read_only=read_only)
        sql_session_factory = lambda read_only=False: get_sqlserver_session(read_only=read_only)

        # Initialize the BatchProcessor with the session factories and config
        batch_processor = BatchProcessor(
            pg_session_factory=pg_session_factory,
            sql_session_factory=sql_session_factory,
            config=APP_CONFIG
        )

        # Start the processing loop
        batch_processor.run_processing_pipeline()

        logger.info(f"[{main_cid}] Batch processing run completed.")

    except Exception as e:
        logger.critical(f"[{main_cid}] A critical error occurred during batch processing run: {e}", exc_info=True)
    finally:
        # Ensure database connections are cleaned up
        dispose_engines()
        logger.info(f"[{main_cid}] Database connections disposed. Batch processing finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EDI Claims Processor Main Application")
    parser.add_argument(
        "--run-all-processing",
        action="store_true",
        help="Run the main batch processing application."
    )
    args = parser.parse_args()

    if args.run_all_processing:
        run_batch_processing()
    else:
        logger.info("No action specified. To run batch processing, use --run-all-processing.")
        logger.info("To run the API, use: uvicorn app.api.main:app --reload")
