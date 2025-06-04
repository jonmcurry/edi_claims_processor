# app/processing/batch_handler.py
"""
Manages processing claims in batches.
Incorporates asynchronous processing and pipeline parallelization concepts.
"""
import asyncio
import time
from typing import List, Any, Coroutine
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor # For CPU-bound or mixed tasks

from sqlalchemy.orm import Session

from app.utils.logging_config import get_logger, set_correlation_id, get_correlation_id
from app.utils.error_handler import handle_exception, AppException
# from app.database.postgres_handler import ( # These would be in postgres_handler.py
#     get_pending_claims_batch, update_batch_processing_status,
#     create_processing_batch_record, assign_claims_to_batch
# )
# from app.processing.claims_processor import ClaimsProcessor # To process individual claims

logger = get_logger('app.processing.batch_handler')

# Determine optimal pool size based on CPU cores and task nature
# For I/O bound tasks (like DB calls, network requests), ThreadPoolExecutor is often good.
# For CPU bound tasks (heavy computation like some ML preprocessing), ProcessPoolExecutor.
# EDI parsing can be CPU intensive, ML prediction can be, DB calls are I/O.
# A mix might be needed, or configure based on primary bottleneck.
MAX_WORKERS_DEFAULT = os.cpu_count() or 4 # Default to number of CPUs or 4

class BatchProcessor:
    """
    Handles fetching claims in batches and processing them, potentially in parallel.
    """
    def __init__(self, pg_session_factory, sql_session_factory, config): # Pass factories and config
        self.pg_session_factory = pg_session_factory # Callable that returns a new session
        self.sql_session_factory = sql_session_factory # Callable
        self.config = config
        self.batch_size = self.config.get('processing', {}).get('batch_size', 100)
        
        # Initialize ClaimsProcessor (which itself might take session factories or config)
        # from app.processing.claims_processor import ClaimsProcessor # Avoid top-level import if circular
        # self.claims_processor = ClaimsProcessor(pg_session_factory, sql_session_factory, config)
        
        # Using ThreadPoolExecutor for I/O-bound tasks like individual claim processing (which involves DB I/O)
        # If parts of claim_processor are CPU-bound, might need a ProcessPoolExecutor for those parts,
        # or a more complex setup.
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS_DEFAULT)
        # self.process_executor = ProcessPoolExecutor(max_workers=MAX_WORKERS_DEFAULT) # For CPU-bound parts

    async def _process_single_claim_async_wrapper(self, claim_id: str, claim_data: Any): # claim_data could be StagingClaim ORM obj
        """
        Asynchronous wrapper to process a single claim using ClaimsProcessor.
        This runs the synchronous ClaimsProcessor.process_claim in a thread.
        """
        loop = asyncio.get_running_loop()
        # Each thread needs its own DB sessions if ClaimsProcessor creates them per call.
        # Or ClaimsProcessor is designed to accept sessions.
        
        # Re-import locally if there are circular dependency concerns at module level
        from app.processing.claims_processor import ClaimsProcessor
        
        # For each claim, a new correlation ID can be beneficial for detailed tracing
        parent_cid = get_correlation_id()
        claim_cid = set_correlation_id(f"{parent_cid}_CLM_{claim_id[:8]}")
        logger.debug(f"Starting async wrapper for claim {claim_id} with CID {claim_cid}")

        try:
            # Create new sessions for this specific task if ClaimsProcessor needs them
            # This ensures thread safety for sessions.
            pg_sess = self.pg_session_factory()
            sql_sess = self.sql_session_factory()
            
            # claims_processor instance should be created here or accept sessions
            # to ensure thread-safety of sessions if it holds them.
            # If ClaimsProcessor is stateless and accepts sessions:
            claims_processor_instance = ClaimsProcessor(pg_sess, sql_sess, self.config)
            
            # Run the synchronous process_claim method in a separate thread
            # Pass the claim_id or the full claim ORM object fetched by the batch handler.
            # If passing ORM object, ensure it's handled correctly across threads/processes (detachment/reattachment if needed).
            # For simplicity, let's assume claim_id is passed and ClaimsProcessor fetches/updates.
            
            # Placeholder: result = await loop.run_in_executor(self.executor, self.claims_processor.process_claim, claim_id)
            # If process_claim itself is async:
            # result = await self.claims_processor.process_claim(claim_id)
            
            # For this example, let's assume process_claim is synchronous and takes claim_id
            # and the ClaimsProcessor instance handles its own session management or is passed sessions.
            
            # Simulating processing:
            # For a real call:
            # await loop.run_in_executor(
            #     self.executor, 
            #     claims_processor_instance.process_claim_by_id, # Assuming this method exists
            #     claim_id
            # )
            logger.info(f"Simulating processing for claim {claim_id} in executor.")
            await asyncio.sleep(0.01) # Simulate work
            
            logger.info(f"Finished processing claim {claim_id} (CID {claim_cid}) successfully.")
            return {"claim_id": claim_id, "status": "PROCESSED_SUCCESS"}
        
        except Exception as e:
            logger.error(f"Error processing claim {claim_id} (CID {claim_cid}) in async wrapper: {e}", exc_info=True)
            # Log this failure specifically, perhaps update claim status to ERROR_BATCH_PROCESSING
            return {"claim_id": claim_id, "status": "PROCESSED_ERROR", "error": str(e)}
        finally:
            if 'pg_sess' in locals() and pg_sess: pg_sess.close()
            if 'sql_sess' in locals() and sql_sess: sql_sess.close()
            set_correlation_id(parent_cid) # Restore parent CID for the batch log context


    async def process_claim_batch(self, claims_to_process: List[Any]) -> List[dict]: # List of StagingClaim IDs or objects
        """
        Processes a list of claims asynchronously.
        `claims_to_process` could be a list of claim IDs or minimal data.
        The `_process_single_claim_async_wrapper` would then fetch full data if needed.
        """
        if not claims_to_process:
            logger.info("No claims in the current batch to process.")
            return []

        logger.info(f"Processing batch of {len(claims_to_process)} claims.")
        
        tasks: List[Coroutine] = []
        for claim_obj in claims_to_process: # Assuming claim_obj is a StagingClaim ORM instance
            # Detach from session if passing ORM object to different threads/processes, then reattach in worker.
            # Or, more simply, pass claim IDs and let worker fetch.
            # For this example, let's assume we pass claim_id.
            claim_id = claim_obj.claim_id 
            # claim_data_dict = { "claim_id": claim_obj.claim_id, ... } # Pass necessary data if not full ORM
            
            # tasks.append(self._process_single_claim_async_wrapper(claim_id, claim_data_dict))
            # If _process_single_claim_async_wrapper expects the ORM object (and handles session detachment/reattachment):
            tasks.append(self._process_single_claim_async_wrapper(claim_id, claim_obj))


        results = await asyncio.gather(*tasks, return_exceptions=False) # Set return_exceptions=True to get exception objects instead of raising
        
        successful_count = sum(1 for r in results if r and r.get("status") == "PROCESSED_SUCCESS")
        failed_count = len(results) - successful_count
        logger.info(f"Batch processing finished. Success: {successful_count}, Failed: {failed_count}")
        
        return results


    def run_main_batch_processing_loop(self, source_pg_session: Session, batch_id_in_db: int = None):
        """
        Main loop to fetch and process batches of claims.
        This is a synchronous method that uses asyncio.run for the async part.
        """
        loop_cid = get_correlation_id() # CID for the entire loop run
        logger.info(f"[{loop_cid}] Starting main batch processing loop.")
        
        # This function would be in postgres_handler.py
        # from app.database.postgres_handler import get_pending_claims_batch, update_batch_processing_status
        from app.database.models.postgres_models import StagingClaim # For querying
        
        total_claims_processed_in_run = 0
        
        try:
            while True:
                set_correlation_id(f"{loop_cid}_BATCH_FETCH") # CID for this specific batch fetch/process cycle
                logger.info(f"Fetching new batch of claims (size: {self.batch_size}).")
                
                # claims_in_batch = get_pending_claims_batch(source_pg_session, self.batch_size)
                # Simplified fetch for example:
                claims_in_batch = source_pg_session.query(StagingClaim)\
                    .filter(StagingClaim.processing_status == 'PARSED') # Or 'PENDING_VALIDATION' etc.
                    .limit(self.batch_size)\
                    .all()

                if not claims_in_batch:
                    logger.info("No more pending claims to process in this loop.")
                    break
                
                logger.info(f"Fetched {len(claims_in_batch)} claims for processing.")
                
                # Detach objects from source_pg_session if they will be processed in other threads/sessions
                # This is important if the worker threads create their own sessions.
                # for claim in claims_in_batch:
                #     source_pg_session.expunge(claim)

                # Process the batch asynchronously
                batch_start_time = time.perf_counter()
                # asyncio.run() can be used if this is the top-level entry point for async code.
                # If called from an already running asyncio loop, use `await self.process_claim_batch(...)`
                try:
                    # If this method is called from a sync context:
                    batch_results = asyncio.run(self.process_claim_batch(claims_in_batch))
                    # If called from an async context:
                    # batch_results = await self.process_claim_batch(claims_in_batch)
                except RuntimeError as re: # Handle "asyncio.run() cannot be called from a running event loop"
                    if "cannot be called from a running event loop" in str(re):
                        logger.warning("asyncio.run() called from a running event loop. Attempting direct await.")
                        # This part is tricky. The design should be consistently async or sync at this level.
                        # For a simple script, asyncio.run is fine. For a larger async app, this needs to be `await`.
                        # Let's assume for a command-line tool, asyncio.run is the entry.
                        # If this were part of an async web server, the outer loop would be `async def`.
                        # This indicates a design consideration for how BatchProcessor is invoked.
                        # For now, we'll assume this is the main entry for batch processing.
                        raise # Or handle differently if this is expected to be part of larger async flow.
                    else:
                        raise


                batch_end_time = time.perf_counter()
                logger.info(f"Batch processed in {batch_end_time - batch_start_time:.2f} seconds.")
                
                # Update statuses based on batch_results (this part needs careful implementation)
                # For example, iterate through batch_results and update claim statuses in DB.
                # This should be done carefully to handle transactions and potential failures during update.
                # For simplicity, this detailed update logic is omitted here but is crucial.
                # Example:
                # for res in batch_results:
                #    update_claim_status_in_db(source_pg_session, res['claim_id'], res['status'], res.get('error'))

                total_claims_processed_in_run += len(claims_in_batch)

                # Commit changes made by status updates (if any were done in this loop)
                # source_pg_session.commit() # Or handled by individual claim processor calls

                # Optional: break after one batch for testing, or add a limit
                # break # Remove for continuous processing
            
            logger.info(f"[{loop_cid}] Main batch processing loop finished. Total claims processed in this run: {total_claims_processed_in_run}")

        except Exception as e:
            logger.critical(f"[{get_correlation_id()}] Critical error in batch processing loop: {e}", exc_info=True)
            # handle_exception(e, context="BatchProcessingLoop") # Use your error handler
            if source_pg_session.is_active:
                source_pg_session.rollback()
        finally:
            set_correlation_id(loop_cid) # Restore loop CID
            self.shutdown_executors()


    def shutdown_executors(self):
        logger.info("Shutting down executors...")
        self.executor.shutdown(wait=True)
        # if hasattr(self, 'process_executor'):
        #     self.process_executor.shutdown(wait=True)
        logger.info("Executors shut down.")


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import init_database_connections, get_postgres_session, get_sqlserver_session, dispose_engines, CONFIG as APP_CONFIG
    from app.database.models.postgres_models import StagingClaim, Base as PostgresBase # For creating dummy data

    setup_logging()
    main_cid = set_correlation_id("BATCH_HANDLER_TEST")

    pg_session = None
    sql_session = None # Not directly used by BatchProcessor in this example, but ClaimsProcessor would need it.

    try:
        init_database_connections()
        pg_session_factory = lambda: get_postgres_session() # Simple factory
        sql_session_factory = lambda: get_sqlserver_session() # Simple factory

        pg_sess_for_setup = pg_session_factory()
        # --- Setup: Create dummy PARSED claims for testing ---
        PostgresBase.metadata.create_all(pg_sess_for_setup.get_bind()) # Ensure tables exist
        
        existing_claims = pg_sess_for_setup.query(StagingClaim).filter(StagingClaim.processing_status == 'PARSED').count()
        if existing_claims < 5: # Add some if not enough
            logger.info("Adding dummy parsed claims for batch test...")
            for i in range(5 - existing_claims):
                claim = StagingClaim(
                    claim_id=f"BATCH_TEST_C{i+1:03}",
                    facility_id="TESTFAC001", patient_account_number=f"ACC_BT_{i}",
                    service_date=datetime.now().date(), financial_class_id="TESTFC01",
                    patient_dob=(datetime.now() - timedelta(days=365*30)).date(),
                    processing_status='PARSED', # Status that batch processor picks up
                    total_charge_amount=Decimal("100.00") * (i+1)
                )
                pg_sess_for_setup.add(claim)
            pg_sess_for_setup.commit()
        logger.info(f"Total PARSED claims before test: {pg_sess_for_setup.query(StagingClaim).filter(StagingClaim.processing_status == 'PARSED').count()}")
        pg_sess_for_setup.close()
        # --- End Setup ---

        batch_processor = BatchProcessor(pg_session_factory, sql_session_factory, APP_CONFIG)
        
        # Run the main loop (using a fresh session for the loop itself)
        main_loop_pg_session = pg_session_factory()
        batch_processor.run_main_batch_processing_loop(main_loop_pg_session)
        main_loop_pg_session.close()

    except Exception as e:
        logger.critical(f"Error in Batch Handler test script: {e}", exc_info=True)
    finally:
        dispose_engines()
        logger.info("Batch Handler test script finished.")
        set_correlation_id(main_cid) # Restore original CID