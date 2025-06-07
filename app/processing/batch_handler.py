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
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy.orm import selectinload

from app.utils.logging_config import get_logger, set_correlation_id, get_correlation_id
from app.database.postgres_handler import get_pending_claims_for_processing, update_staging_claim_status
from app.processing.rules_engine import RulesEngine, ClaimValidationResult
from app.processing.ml_predictor import get_ml_predictor
from app.processing.reimbursement_calculator import ReimbursementCalculator
from app.processing.claims_processor import OptimizedClaimsProcessor

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
    data: Any
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

    async def run_pipeline(self):
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

    async def _fetch_stage_producer(self):
        while not self.shutdown_event.is_set():
            try:
                if self.stage_queues[ProcessingStage.VALIDATE].qsize() >= self.queue_high_water_mark:
                    await asyncio.sleep(0.1)
                    continue
                with self.pg_session_factory(read_only=True) as ro_pg_session:
                    claims = await asyncio.get_running_loop().run_in_executor(self.io_executor, get_pending_claims_for_processing, ro_pg_session, self.initial_fetch_batch_size, 'PENDING')
                if claims:
                    for claim in claims:
                        await self.stage_queues[ProcessingStage.VALIDATE].put(ClaimWorkItem(claim_id=claim.claim_id, current_stage=ProcessingStage.FETCH, data=claim))
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
                else:
                    self.metrics.total_claims_failed += 1
                input_q.task_done()
            except Exception as e:
                logger.error(f"[{stage_name}] Error processing item: {e}", exc_info=True)
                if 'item' in locals():
                    await self._handle_failed_item(item, e)

    async def _process_item_validation(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        loop = asyncio.get_running_loop()
        def sync_validate():
            with self.pg_session_factory(read_only=True) as validation_session:
                return self.rules_engine.validate_claim(validation_session, work_item.data)

        validation_result = await loop.run_in_executor(self.io_executor, sync_validate)
        work_item.validation_result = validation_result
        
        if not validation_result.is_overall_valid:
            logger.warning(f"Claim {work_item.claim_id} failed validation: {validation_result.errors}")
            with self.pg_session_factory() as write_session:
                update_staging_claim_status(write_session, work_item.claim_id, validation_result.final_status_staging, [e['message'] for e in validation_result.errors])
                write_session.commit()
            return None
        return work_item

    async def _process_item_ml_prediction(self, work_item: ClaimWorkItem) -> ClaimWorkItem:
        loop = asyncio.get_running_loop()
        ml_input = self.claims_processor._prepare_ml_input(work_item.data)
        prediction = await loop.run_in_executor(self.cpu_executor, self.ml_predictor.predict_filters, ml_input)
        work_item.ml_prediction = {"filters": prediction[0], "probability": prediction[1]}
        return work_item
    
    async def _process_item_reimbursement(self, work_item: ClaimWorkItem) -> ClaimWorkItem:
        loop = asyncio.get_running_loop()
        def sync_reimbursement():
            self.reimbursement_calculator.process_claim_reimbursement(work_item.data)
            return {"total": float(getattr(work_item.data, 'total_estimated_reimbursement', 0))}
        work_item.reimbursement_details = await loop.run_in_executor(self.io_executor, sync_reimbursement)
        return work_item
        
    async def _process_item_export(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        loop = asyncio.get_running_loop()
        def sync_export():
            return self.claims_processor.process_single_claim_pipeline_style(work_item.data)
        
        processing_result = await loop.run_in_executor(self.io_executor, sync_export)
        
        # Now, perform the DB operations based on the result
        def db_operations():
            try:
                if processing_result.is_overall_valid:
                    with self.sql_session_factory() as sql_session:
                        prod_orm = self.claims_processor._map_staging_to_production_orm(work_item.data, processing_result.ml_assigned_filters)
                        sql_session.add(prod_orm)
                        sql_session.commit()
                else:
                    with self.sql_session_factory() as sql_session:
                        failed_orm = self.claims_processor._create_failed_claim_detail_orm(work_item.data, processing_result)
                        sql_session.add(failed_orm)
                        sql_session.commit()
                
                with self.pg_session_factory() as pg_session:
                    update_staging_claim_status(pg_session, work_item.claim_id, processing_result.final_status_staging, [e['message'] for e in processing_result.errors_for_failed_log])
                    pg_session.commit()
                return True
            except Exception as e:
                logger.error(f"Error during DB operations for export of claim {work_item.claim_id}: {e}", exc_info=True)
                return False

        export_successful = await loop.run_in_executor(self.io_executor, db_operations)
        return work_item if export_successful else None

    async def _handle_failed_item(self, item, error):
        logger.error(f"Item {item.claim_id} failed in stage {item.current_stage.value}: {error}", exc_info=True)
        # Implement dead-letter queue or error state update logic here
        
    async def stop_pipeline(self):
        self.shutdown_event.set()
        for task in self.active_pipeline_tasks:
            task.cancel()
        await asyncio.gather(*self.active_pipeline_tasks, return_exceptions=True)

class BatchProcessor:
    def __init__(self, pg_session_factory, sql_session_factory, config):
        self.pipeline_processor = OptimizedPipelineProcessor(pg_session_factory, sql_session_factory, config)

    def run_main_batch_processing_loop(self):
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.pipeline_processor.run_pipeline())
        except KeyboardInterrupt:
            loop.run_until_complete(self.pipeline_processor.stop_pipeline())
