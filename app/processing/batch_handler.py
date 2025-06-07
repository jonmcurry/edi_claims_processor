# app/processing/batch_handler.py
"""
High-performance batch processing system with pipeline parallelization,
dynamic scaling, backpressure handling, and optimized resource management.
"""
import asyncio
import time
import os
import sys
import multiprocessing
import psutil
from typing import List, Any, Dict, Optional, Callable, Awaitable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy.orm import selectinload

from app.utils.logging_config import get_logger, set_correlation_id, get_correlation_id
from app.database.postgres_handler import get_pending_claims_for_processing, update_staging_claim_status
from app.processing.rules_engine import RulesEngine, ClaimValidationResult
from app.processing.ml_predictor import get_ml_predictor
from app.processing.reimbursement_calculator import ReimbursementCalculator
from app.processing.claims_processor import OptimizedClaimsProcessor, StagingClaim

try:
    from app.utils.platform_config import PLATFORM_CONFIG, configure_multiprocessing as configure_platform_mp
    HAS_PLATFORM_CONFIG = True
except ImportError:
    # Fallback configuration
    logger = get_logger('app.processing.batch_handler_fallback_config')
    logger.warning("app.utils.platform_config not found. Using fallback configuration.")
    PLATFORM_CONFIG = {"platform": sys.platform, "max_workers": 4, "preferred_executor": "thread", "use_multiprocessing": False, "supports_fork": False}
    def configure_platform_mp(): pass
    HAS_PLATFORM_CONFIG = False

logger = get_logger('app.processing.batch_handler')

class ProcessingStage(Enum):
    FETCH = "FETCH"
    VALIDATE = "VALIDATE"
    ML_PREDICT = "ML_PREDICT"
    CALCULATE_REIMBURSEMENT = "CALCULATE_REIMBURSEMENT"
    EXPORT = "EXPORT"

@dataclass
class StageMetrics:
    processed_count: int = 0
    error_count: int = 0
    total_processing_time_seconds: float = 0.0
    avg_processing_time_ms: float = 0.0
    items_in_queue: int = 0

    def record_item_processed(self, duration_seconds: float):
        self.processed_count += 1
        self.total_processing_time_seconds += duration_seconds
        if self.processed_count > 0:
            self.avg_processing_time_ms = (self.total_processing_time_seconds / self.processed_count) * 1000

    def record_error(self):
        self.error_count += 1

@dataclass
class PipelineProcessingMetrics:
    total_claims_processed_successfully: int = 0
    total_claims_failed: int = 0
    pipeline_start_time: float = field(default_factory=time.perf_counter)
    pipeline_end_time: Optional[float] = None
    overall_avg_throughput_claims_per_second: float = 0.0
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    stage_specific_metrics: Dict[ProcessingStage, StageMetrics] = field(default_factory=lambda: {stage: StageMetrics() for stage in ProcessingStage})

    def update_throughput(self):
        duration = (self.pipeline_end_time or time.perf_counter()) - self.pipeline_start_time
        total_completed = self.total_claims_processed_successfully + self.total_claims_failed
        if duration > 0:
            self.overall_avg_throughput_claims_per_second = total_completed / duration

    def get_queue_depths(self, queues: Dict[ProcessingStage, asyncio.Queue]):
        for stage, metrics in self.stage_specific_metrics.items():
            if stage in queues:
                metrics.items_in_queue = queues[stage].qsize()

@dataclass
class ClaimWorkItem:
    claim_id: str
    current_stage: ProcessingStage
    data: Dict[str, Any] # Changed to Dict to hold serialized data
    created_at: float = field(default_factory=time.perf_counter)
    stage_start_time: float = field(default_factory=time.perf_counter)
    retries: int = 0
    max_retries: int = 3
    validation_result: Any = None
    ml_prediction: Any = None
    reimbursement_details: Any = None

def _setup_event_loop_for_windows():
    if sys.platform == "win32":
        try:
            import winloop
            asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
        except ImportError:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

class OptimizedPipelineProcessor:
    def __init__(self, pg_session_factory: Callable[..., Session], sql_session_factory: Callable[..., Session], config: Dict[str, Any]):
        if HAS_PLATFORM_CONFIG:
            configure_platform_mp()
        else:
            if sys.platform == "win32":
                multiprocessing.freeze_support()
        _setup_event_loop_for_windows()
        
        self.pg_session_factory = pg_session_factory
        self.sql_session_factory = sql_session_factory
        self.config = config
        self.app_config = config
        
        proc_config = self.config.get('processing', {})
        perf_config = self.config.get('performance', {})

        self.initial_fetch_batch_size = proc_config.get('batch_size', 1000)
        self.max_queue_size = perf_config.get('pipeline_max_queue_size', self.initial_fetch_batch_size * 2)
        
        self._configure_workers_from_platform()
        
        self.stage_queues = {stage: asyncio.Queue(maxsize=self.max_queue_size) for stage in ProcessingStage if stage != ProcessingStage.FETCH}
        
        self._init_executors()
        
        self.rules_engine: Optional[RulesEngine] = None
        self.ml_predictor = get_ml_predictor()
        self.reimbursement_calculator = ReimbursementCalculator()
        self.claims_processor: Optional[OptimizedClaimsProcessor] = None

        self.metrics = PipelineProcessingMetrics()
        self.shutdown_event = asyncio.Event()
        self.active_pipeline_tasks: List[asyncio.Task] = []
        
        self.queue_high_water_mark = int(self.max_queue_size * proc_config.get('queue_high_watermark_factor', 0.85))
        self.queue_low_water_mark = int(self.max_queue_size * proc_config.get('queue_low_watermark_factor', 0.25))
        
    def _configure_workers_from_platform(self):
        cpu_count = os.cpu_count() or 1
        proc_config = self.config.get('processing', {})
        self.min_workers_per_stage_type = {'cpu': proc_config.get('min_cpu_workers', 1), 'io': proc_config.get('min_io_workers', 2)}
        self.max_workers_per_stage_type = {'cpu': proc_config.get('max_cpu_workers', cpu_count), 'io': proc_config.get('max_io_workers', cpu_count * 2)}
        self.current_workers_per_stage_type = self.min_workers_per_stage_type.copy()

    def _init_executors(self):
        self.io_executor = ThreadPoolExecutor(max_workers=self.max_workers_per_stage_type['io'], thread_name_prefix="pipeline_io_worker")
        self.cpu_executor = ThreadPoolExecutor(max_workers=self.max_workers_per_stage_type['cpu'], thread_name_prefix="pipeline_cpu_worker")

    def _initialize_shared_components(self):
        with self.pg_session_factory(read_only=True) as temp_ro_session:
            self.rules_engine = RulesEngine(temp_ro_session)
        self.claims_processor = OptimizedClaimsProcessor(self.pg_session_factory, self.sql_session_factory, self.app_config, self.rules_engine, self.ml_predictor, self.reimbursement_calculator)

    async def start_processing(self):
        """The main async method that sets up and runs the pipeline stages."""
        self._initialize_shared_components()
        if not self.rules_engine:
            logger.critical("Pipeline cannot start: RulesEngine failed to initialize.")
            return

        stage_methods = {
            ProcessingStage.FETCH: self._fetch_stage_producer,
            ProcessingStage.VALIDATE: self._create_worker_manager(self.stage_queues[ProcessingStage.VALIDATE], self.stage_queues[ProcessingStage.ML_PREDICT], self._process_item_validation, "Validation", 'io'),
            ProcessingStage.ML_PREDICT: self._create_worker_manager(self.stage_queues[ProcessingStage.ML_PREDICT], self.stage_queues[ProcessingStage.CALCULATE_REIMBURSEMENT], self._process_item_ml_prediction, "MLPrediction", 'cpu'),
            ProcessingStage.CALCULATE_REIMBURSEMENT: self._create_worker_manager(self.stage_queues[ProcessingStage.CALCULATE_REIMBURSEMENT], self.stage_queues[ProcessingStage.EXPORT], self._process_item_reimbursement, "Reimbursement", 'cpu'),
            ProcessingStage.EXPORT: self._create_worker_manager(self.stage_queues[ProcessingStage.EXPORT], None, self._process_item_export, "Export", 'io')
        }
        self.active_pipeline_tasks = [asyncio.create_task(coro()) for coro in stage_methods.values()]
        await asyncio.gather(*self.active_pipeline_tasks, return_exceptions=True)

    def _serialize_claim(self, claim: StagingClaim) -> Dict[str, Any]:
        """Serializes a SQLAlchemy ORM object to a dictionary, including relationships."""
        if not claim:
            return {}
        
        data = {c.name: getattr(claim, c.name) for c in claim.__table__.columns}
        
        if 'cms1500_diagnoses' in claim.__dict__:
            data['cms1500_diagnoses'] = [{c.name: getattr(diag, c.name) for c in diag.__table__.columns} for diag in claim.cms1500_diagnoses]
        if 'cms1500_line_items' in claim.__dict__:
             data['cms1500_line_items'] = [{c.name: getattr(line, c.name) for c in line.__table__.columns} for line in claim.cms1500_line_items]
    
        def convert_types(obj):
            if isinstance(obj, dict):
                return {k: convert_types(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [convert_types(i) for i in obj]
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            if isinstance(obj, Decimal):
                return float(obj)
            return obj

        return convert_types(data)

    def _fetch_claims_in_thread(self, session_factory, batch_size, status):
        """Synchronous function to fetch claims and serialize them before the session closes."""
        with session_factory() as session:
            try:
                claims_orm = get_pending_claims_for_processing(session, batch_size, status)
                session.commit()
                return [self._serialize_claim(claim) for claim in claims_orm]
            except Exception as e:
                logger.error(f"Error in _fetch_claims_in_thread: {e}", exc_info=True)
                session.rollback()
                raise

    async def _fetch_stage_producer(self):
        loop = asyncio.get_running_loop()
        while not self.shutdown_event.is_set():
            try:
                if self.stage_queues[ProcessingStage.VALIDATE].qsize() >= self.queue_high_water_mark:
                    await asyncio.sleep(0.1)
                    continue

                claims_data_list = await loop.run_in_executor(
                    self.io_executor,
                    self._fetch_claims_in_thread,
                    self.pg_session_factory,
                    self.initial_fetch_batch_size,
                    'PENDING'
                )

                if claims_data_list:
                    for claim_dict in claims_data_list:
                        if claim_dict: # Ensure dict is not empty
                            await self.stage_queues[ProcessingStage.VALIDATE].put(ClaimWorkItem(
                                claim_id=claim_dict['claim_id'],
                                current_stage=ProcessingStage.FETCH,
                                data=claim_dict
                            ))
                else:
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"[FETCH] Error: {e}", exc_info=True)
                await asyncio.sleep(5)

    def _create_worker_manager(self, input_q, output_q, processor, stage_name, worker_type):
        async def manager():
            workers = [asyncio.create_task(self._stage_worker_loop(input_q, output_q, processor, stage_name)) for _ in range(self.current_workers_per_stage_type[worker_type])]
            await asyncio.gather(*workers)
        return manager

    async def _stage_worker_loop(self, input_q, output_q, processor, stage_name):
        while not self.shutdown_event.is_set():
            try:
                item = await input_q.get()
                processed_item = await processor(item)
                if processed_item and output_q:
                    await output_q.put(processed_item)
                elif not output_q and processed_item:
                    self.metrics.total_claims_processed_successfully += 1
                elif not processed_item:
                    self.metrics.total_claims_failed += 1
                input_q.task_done()
            except Exception as e:
                logger.error(f"[{stage_name}] Error processing item: {e}", exc_info=True)
                if 'item' in locals():
                    await self._handle_failed_item(item, e)
                    self.metrics.total_claims_failed += 1
                input_q.task_done()

    async def _process_item_validation(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        loop = asyncio.get_running_loop()
        def sync_validate_and_update_on_fail():
            with self.pg_session_factory() as session:
                try:
                    claim_orm = session.query(StagingClaim).options(
                        selectinload(StagingClaim.cms1500_diagnoses),
                        selectinload(StagingClaim.cms1500_line_items)
                    ).filter(StagingClaim.claim_id == work_item.claim_id).one_or_none()

                    if not claim_orm:
                        logger.error(f"Claim {work_item.claim_id} not found in DB for validation stage.")
                        return None

                    validation_result = self.rules_engine.validate_claim(session, claim_orm)
                    work_item.validation_result = validation_result

                    if not validation_result.is_overall_valid:
                        update_staging_claim_status(
                            session,
                            work_item.claim_id,
                            validation_result.final_status_staging,
                            [e['message'] for e in validation_result.errors]
                        )
                        session.commit()
                        return None
                    
                    return work_item
                except Exception as e:
                    logger.error(f"Exception in validation sync block for claim {work_item.claim_id}: {e}", exc_info=True)
                    session.rollback()
                    raise
        return await loop.run_in_executor(self.io_executor, sync_validate_and_update_on_fail)

    async def _process_item_ml_prediction(self, work_item: ClaimWorkItem) -> ClaimWorkItem:
        loop = asyncio.get_running_loop()
        ml_input = self.claims_processor._prepare_ml_input(work_item.data)
        prediction = await loop.run_in_executor(self.cpu_executor, self.ml_predictor.predict_filters, ml_input)
        work_item.ml_prediction = {"filters": prediction[0], "probability": prediction[1]}
        return work_item
    
    async def _process_item_reimbursement(self, work_item: ClaimWorkItem) -> ClaimWorkItem:
        loop = asyncio.get_running_loop()
        def sync_reimbursement():
             with self.pg_session_factory() as session:
                claim_orm = session.query(StagingClaim).options(
                    selectinload(StagingClaim.cms1500_line_items)
                ).filter(StagingClaim.claim_id == work_item.claim_id).one_or_none()
                if claim_orm:
                    self.reimbursement_calculator.process_claim_reimbursement(claim_orm)
                    return {"total": float(getattr(claim_orm, 'total_estimated_reimbursement', 0))}
                return {"total": 0.0}

        work_item.reimbursement_details = await loop.run_in_executor(self.io_executor, sync_reimbursement)
        return work_item
        
    async def _process_item_export(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        loop = asyncio.get_running_loop()
        def db_operations():
            try:
                with self.pg_session_factory() as pg_session, self.sql_session_factory() as sql_session:
                    staging_claim_orm = pg_session.query(StagingClaim).options(
                        selectinload(StagingClaim.cms1500_diagnoses),
                        selectinload(StagingClaim.cms1500_line_items)
                    ).filter(StagingClaim.claim_id == work_item.claim_id).one_or_none()

                    if not staging_claim_orm:
                        logger.error(f"Claim {work_item.claim_id} disappeared from DB before export.")
                        return False

                    if work_item.ml_prediction:
                        staging_claim_orm.pipeline_ml_prediction = work_item.ml_prediction

                    processing_result = self.claims_processor.process_single_claim_pipeline_style(staging_claim_orm)
                    
                    if processing_result.is_overall_valid:
                        prod_orm = self.claims_processor._map_staging_to_production_orm(staging_claim_orm, processing_result.ml_assigned_filters)
                        sql_session.add(prod_orm)
                    else:
                        failed_orm = self.claims_processor._create_failed_claim_detail_orm(staging_claim_orm, processing_result)
                        sql_session.add(failed_orm)
                    
                    sql_session.commit()
                    
                    update_staging_claim_status(
                        pg_session, 
                        work_item.claim_id, 
                        processing_result.final_status_staging, 
                        [e['message'] for e in processing_result.errors_for_failed_log]
                    )
                    pg_session.commit()
                return True
            except Exception as e:
                logger.error(f"Error during DB operations for export of claim {work_item.claim_id}: {e}", exc_info=True)
                return False

        export_successful = await loop.run_in_executor(self.io_executor, db_operations)
        return work_item if export_successful else None

    async def _handle_failed_item(self, item: ClaimWorkItem, error: Exception):
        logger.error(f"Item {item.claim_id} failed in stage {item.current_stage.value}: {error}", exc_info=True)
        try:
            with self.pg_session_factory() as session:
                update_staging_claim_status(session, item.claim_id, "ERROR_PIPELINE", [f"Pipeline error in stage {item.current_stage.value}: {str(error)}"])
                session.commit()
        except Exception as db_err:
            logger.critical(f"CRITICAL: Failed to mark claim {item.claim_id} as ERROR in DB after pipeline failure: {db_err}")

    async def stop_pipeline(self):
        self.shutdown_event.set()
        for task in self.active_pipeline_tasks:
            task.cancel()
        await asyncio.gather(*self.active_pipeline_tasks, return_exceptions=True)
        self.io_executor.shutdown(wait=False, cancel_futures=True)
        self.cpu_executor.shutdown(wait=False, cancel_futures=True)


class BatchProcessor:
    def __init__(self, pg_session_factory, sql_session_factory, config):
        self.pipeline_processor = OptimizedPipelineProcessor(pg_session_factory, sql_session_factory, config)

    def run_processing_pipeline(self):
        """The main entry point to start the processing loop."""
        if sys.platform == "win32":
            loop = asyncio.get_event_loop()
        else:
            try:
                import uvloop
                uvloop.install()
                loop = uvloop.new_event_loop()
                asyncio.set_event_loop(loop)
            except ImportError:
                loop = asyncio.get_event_loop()

        try:
            loop.run_until_complete(self.pipeline_processor.start_processing())
        except KeyboardInterrupt:
            logger.info("Shutdown signal received. Stopping pipeline...")
            loop.run_until_complete(self.pipeline_processor.stop_pipeline())
        finally:
            logger.info("Pipeline has been shut down.")
