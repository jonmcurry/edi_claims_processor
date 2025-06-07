# app/processing/batch_handler.py
"""
High-performance batch processing system with pipeline parallelization,
dynamic scaling, backpressure handling, and optimized resource management.
This version has been optimized for bulk database operations.
"""
import asyncio
import time
import os
import sys
import multiprocessing
import psutil
from typing import List, Any, Dict, Optional, Callable
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError

from app.utils.logging_config import get_logger, set_correlation_id, get_correlation_id
from app.database.postgres_handler import get_pending_claims_for_processing
from app.database.models.postgres_models import StagingClaim
from app.processing.rules_engine import RulesEngine
from app.processing.ml_predictor import get_ml_predictor, MLPredictor
from app.processing.reimbursement_calculator import ReimbursementCalculator
from app.processing.claims_processor import OptimizedClaimsProcessor, IndividualClaimProcessingResult
from app.utils.error_handler import StagingDBError, ProductionDBError

# Platform configuration
try:
    from app.utils.platform_config import PLATFORM_CONFIG, configure_multiprocessing as configure_platform_mp
    HAS_PLATFORM_CONFIG = True
except ImportError:
    logger = get_logger('app.processing.batch_handler_fallback_config')
    logger.warning("app.utils.platform_config not found. Using fallback configuration.")
    PLATFORM_CONFIG = {"platform": sys.platform, "max_workers": 4, "preferred_executor": "thread", "use_multiprocessing": False}
    def configure_platform_mp(): pass
    HAS_PLATFORM_CONFIG = False

logger = get_logger('app.processing.batch_handler')

class ProcessingStage(Enum):
    FETCH = "FETCH"
    PROCESS = "PROCESS" # Combined processing stage
    EXPORT = "EXPORT"

@dataclass
class StageMetrics:
    processed_count: int = 0
    error_count: int = 0
    total_processing_time_seconds: float = 0.0
    avg_processing_time_ms: float = 0.0
    items_in_queue: int = 0

@dataclass
class PipelineProcessingMetrics:
    total_claims_processed: int = 0
    total_claims_failed: int = 0
    pipeline_start_time: float = field(default_factory=time.perf_counter)
    pipeline_end_time: Optional[float] = None
    overall_avg_throughput_claims_per_second: float = 0.0
    stage_specific_metrics: Dict[ProcessingStage, StageMetrics] = field(default_factory=lambda: {stage: StageMetrics() for stage in ProcessingStage})

@dataclass
class ClaimWorkItem:
    """A lightweight object to pass between pipeline stages, carrying only essential data."""
    claim_id: str
    service_date: datetime.date
    # After processing, this will hold the result object
    processing_result: Optional[IndividualClaimProcessingResult] = None
    
@dataclass
class ExportBatch:
    """A batch of processed claims ready for bulk export."""
    valid_claims: List[StagingClaim] = field(default_factory=list)
    failed_claims: List[StagingClaim] = field(default_factory=list)
    results: List[IndividualClaimProcessingResult] = field(default_factory=list)

    def add(self, claim: StagingClaim, result: IndividualClaimProcessingResult):
        self.results.append(result)
        if result.is_overall_valid:
            self.valid_claims.append(claim)
        else:
            self.failed_claims.append(claim)
    
    def size(self):
        return len(self.valid_claims) + len(self.failed_claims)

    def clear(self):
        self.valid_claims.clear()
        self.failed_claims.clear()
        self.results.clear()

class OptimizedPipelineProcessor:
    """
    Manages the high-throughput, asynchronous claims processing pipeline.
    """
    def __init__(self, pg_session_factory: Callable[..., Session], sql_session_factory: Callable[..., Session], config: Dict[str, Any]):
        if HAS_PLATFORM_CONFIG:
            configure_platform_mp()
        
        self.pg_session_factory = pg_session_factory
        self.sql_session_factory = sql_session_factory
        self.config = config
        
        proc_config = self.config.get('processing', {})
        self.fetch_batch_size = proc_config.get('batch_size', 2000)
        self.export_batch_size = proc_config.get('export_batch_size', 1000)
        self.max_queue_size = proc_config.get('pipeline_max_queue_size', self.fetch_batch_size * 2)

        self.num_processing_workers = PLATFORM_CONFIG.get('max_workers', 4) * 2 # Increase workers
        self.num_export_workers = 2
        
        self.process_queue = asyncio.Queue(maxsize=self.max_queue_size)
        self.export_queue = asyncio.Queue(maxsize=self.max_queue_size)

        self.claims_processor: Optional[OptimizedClaimsProcessor] = None
        self.metrics = PipelineProcessingMetrics()
        self.shutdown_event = asyncio.Event()

    def _initialize_shared_components(self):
        """Initializes components shared across workers."""
        with self.pg_session_factory(read_only=True) as temp_ro_session:
            rules_engine = RulesEngine(temp_ro_session)
        ml_predictor = get_ml_predictor()
        reimbursement_calculator = ReimbursementCalculator()
        self.claims_processor = OptimizedClaimsProcessor(
            self.pg_session_factory, self.sql_session_factory, self.config,
            rules_engine, ml_predictor, reimbursement_calculator
        )
        logger.info("Shared processing components initialized.")

    async def run_pipeline(self):
        """Sets up and runs the entire processing pipeline."""
        self.metrics.pipeline_start_time = time.perf_counter()
        self._initialize_shared_components()
        if not self.claims_processor:
            logger.critical("Claims processor failed to initialize. Aborting pipeline.")
            return

        producer_task = asyncio.create_task(self._fetch_stage_producer())
        processing_workers = [asyncio.create_task(self._processing_worker()) for _ in range(self.num_processing_workers)]
        export_workers = [asyncio.create_task(self._export_worker()) for _ in range(self.num_export_workers)]
        
        all_tasks = [producer_task] + processing_workers + export_workers
        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            logger.info("Pipeline tasks were cancelled.")
        finally:
            self.metrics.pipeline_end_time = time.perf_counter()
            self.metrics.update_throughput()
            logger.info(f"Pipeline finished. Throughput: {self.metrics.overall_avg_throughput_claims_per_second:.2f} claims/sec")

    async def _fetch_stage_producer(self):
        """Continuously fetches claims and puts their IDs into the processing queue."""
        while not self.shutdown_event.is_set():
            try:
                if self.process_queue.full():
                    await asyncio.sleep(0.1) # Backpressure
                    continue

                with self.pg_session_factory() as session:
                    claims = get_pending_claims_for_processing(session, self.fetch_batch_size)
                    session.commit() # Commit the status update to 'PROCESSING'

                if claims:
                    for claim in claims:
                        work_item = ClaimWorkItem(claim_id=claim.claim_id, service_date=claim.service_date)
                        await self.process_queue.put(work_item)
                    logger.info(f"Fetched and queued {len(claims)} new claims for processing.")
                else:
                    logger.debug("No pending claims found. Waiting...")
                    await asyncio.sleep(2) # Wait longer if no claims
            except Exception as e:
                logger.error(f"[FETCH STAGE] Error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _processing_worker(self):
        """Worker that takes a claim ID, processes it, and passes result to export queue."""
        # Create a single session per worker to be reused
        pg_session = self.pg_session_factory()
        try:
            while not self.shutdown_event.is_set():
                work_item = await self.process_queue.get()
                try:
                    # Fetch the full claim ORM object using the ID
                    claim_orm = pg_session.query(StagingClaim).options(
                        selectinload(StagingClaim.cms1500_diagnoses),
                        selectinload(StagingClaim.cms1500_line_items)
                    ).filter(StagingClaim.claim_id == work_item.claim_id).one_or_none()
                    
                    if not claim_orm:
                        logger.warning(f"Claim ID {work_item.claim_id} not found for processing, likely processed by another worker. Skipping.")
                        self.process_queue.task_done()
                        continue

                    # Process the claim
                    result = self.claims_processor.process_single_claim_pipeline_style(claim_orm)
                    work_item.processing_result = result
                    
                    await self.export_queue.put((claim_orm, result))
                except Exception as e:
                    logger.error(f"[PROCESS WORKER] Error processing claim {work_item.claim_id}: {e}", exc_info=True)
                    self.metrics.total_claims_failed += 1
                finally:
                    self.process_queue.task_done()
        finally:
            pg_session.close()

    async def _export_worker(self):
        """Worker that collects processed claims and flushes them in bulk."""
        export_batch = ExportBatch()
        last_flush_time = time.monotonic()
        
        while not self.shutdown_event.is_set():
            try:
                # Wait for an item with a timeout to allow for periodic flushing
                claim_orm, result = await asyncio.wait_for(self.export_queue.get(), timeout=1.0)
                export_batch.add(claim_orm, result)
                self.export_queue.task_done()
            except asyncio.TimeoutError:
                # If queue is empty but there's a batch, flush it
                pass

            # Flush batch if it's full or if a timeout has been reached
            if export_batch.size() >= self.export_batch_size or \
               (export_batch.size() > 0 and (time.monotonic() - last_flush_time) > 2.0):
                await self._flush_export_batch(export_batch)
                export_batch.clear()
                last_flush_time = time.monotonic()

    async def _flush_export_batch(self, batch: ExportBatch):
        """Performs bulk database operations for a batch of claims."""
        cid = get_correlation_id()
        batch_size = batch.size()
        logger.info(f"[{cid}] Flushing export batch of {batch_size} claims.")
        
        if batch_size == 0:
            return

        pg_session = self.pg_session_factory()
        sql_session = self.sql_session_factory()
        try:
            # --- Bulk Insert into SQL Server ---
            if batch.valid_claims:
                prod_orms = [self.claims_processor._map_staging_to_production_orm(claim, res.ml_assigned_filters) for claim, res in zip(batch.valid_claims, batch.results) if res.is_overall_valid]
                sql_session.bulk_save_objects(prod_orms)
                logger.info(f"[{cid}] Prepared {len(prod_orms)} valid claims for SQL Server bulk insert.")

            if batch.failed_claims:
                failed_orms = [self.claims_processor._create_failed_claim_detail_orm(claim, res) for claim, res in zip(batch.failed_claims, batch.results) if not res.is_overall_valid]
                sql_session.bulk_save_objects(failed_orms)
                logger.info(f"[{cid}] Prepared {len(failed_orms)} failed claims for SQL Server bulk insert.")

            sql_session.commit()
            logger.info(f"[{cid}] Committed {batch_size} results to SQL Server.")

            # --- Bulk Update in PostgreSQL ---
            update_mappings = []
            for result in batch.results:
                update_mappings.append({
                    'claim_id': result.claim_id,
                    'processing_status': result.final_status_staging,
                    'exported_to_production': result.is_overall_valid,
                    'export_date': datetime.utcnow() if result.is_overall_valid else None,
                    'updated_date': datetime.utcnow()
                })
            
            pg_session.bulk_update_mappings(StagingClaim, update_mappings)
            pg_session.commit()
            logger.info(f"[{cid}] Committed status updates for {len(update_mappings)} claims in PostgreSQL.")
            
            self.metrics.total_claims_processed += batch_size
            self.metrics.total_claims_failed += len(batch.failed_claims)

        except (SQLAlchemyError, StagingDBError, ProductionDBError) as e:
            logger.error(f"[{cid}] CRITICAL: Database error during bulk flush of {batch_size} claims. Rolling back. Error: {e}", exc_info=True)
            sql_session.rollback()
            pg_session.rollback()
            self.metrics.total_claims_failed += batch_size # Assume all failed
        finally:
            pg_session.close()
            sql_session.close()

    async def stop(self):
        """Gracefully stops the pipeline."""
        logger.info("Shutdown signal received. Stopping pipeline...")
        self.shutdown_event.set()
        # Allow running tasks to finish
        await asyncio.sleep(1) 
        # Cancel any tasks that are still running
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

class BatchProcessor:
    def __init__(self, pg_session_factory, sql_session_factory, config):
        self.pipeline_processor = OptimizedPipelineProcessor(pg_session_factory, sql_session_factory, config)

    def run_processing_pipeline(self):
        """The main entry point to start the processing loop."""
        if sys.platform == "win32":
            loop = asyncio.ProactorEventLoop()
            asyncio.set_event_loop(loop)
        else:
            try:
                import uvloop
                uvloop.install()
            except ImportError:
                pass # uvloop not installed, use default loop
            loop = asyncio.get_event_loop()

        try:
            loop.run_until_complete(self.pipeline_processor.run_pipeline())
        except KeyboardInterrupt:
            logger.info("Shutdown signal received in BatchProcessor. Stopping...")
            loop.run_until_complete(self.pipeline_processor.stop())
        finally:
            logger.info("BatchProcessor has been shut down.")
