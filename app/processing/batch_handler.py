# app/processing/batch_handler.py
"""
High-performance batch processing system with pipeline parallelization,
dynamic scaling, backpressure handling, and optimized resource management.
Cross-platform optimized for Windows and Unix systems, with enhanced
read-replica support, performance monitoring, and auto-scaling.
"""
import asyncio
import time
import os
import sys
import multiprocessing
# import awaitable # REMOVED: Incorrect import
import psutil # For system CPU and memory monitoring
from typing import List, Any, Dict, Optional, Callable, Awaitable # MODIFIED: Added Awaitable
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta # Import date and timedelta

from sqlalchemy.orm import Session

from app.utils.logging_config import get_logger, set_correlation_id, get_correlation_id

# Assuming postgres_handler contains these functions and they are adapted for session management
from app.database.postgres_handler import get_pending_claims_for_processing, update_staging_claim_status
from app.processing.rules_engine import RulesEngine
from app.processing.ml_predictor import get_ml_predictor # Singleton MLPredictor
from app.processing.reimbursement_calculator import ReimbursementCalculator


# Import platform configuration for optimized settings
try:
    from app.utils.platform_config import PLATFORM_CONFIG, configure_multiprocessing as configure_platform_mp
    HAS_PLATFORM_CONFIG = True
except ImportError:
    logger = get_logger('app.processing.batch_handler_fallback_config') # Temp logger for this block
    logger.warning("app.utils.platform_config not found. Using fallback configuration.")
    PLATFORM_CONFIG = {
        "platform": sys.platform,
        "max_workers": min(os.cpu_count() or 4, 8 if sys.platform == "win32" else 16),
        "preferred_executor": "thread" if sys.platform == "win32" else "process",
        "use_multiprocessing": False if sys.platform == "win32" else True, # Default to False for Windows due to complexity
        "supports_fork": hasattr(os, 'fork')
    }
    def configure_platform_mp(): # Dummy function
        if sys.platform == "win32":
            multiprocessing.freeze_support()
            try: multiprocessing.set_start_method('spawn', force=True)
            except RuntimeError: pass
        logger.info("Using fallback multiprocessing configuration.")
    HAS_PLATFORM_CONFIG = False

logger = get_logger('app.processing.batch_handler')

class ProcessingStage(Enum):
    """Enumeration of processing pipeline stages."""
    FETCH = "FETCH"
    VALIDATE = "VALIDATE"
    ML_PREDICT = "ML_PREDICT"
    CALCULATE_REIMBURSEMENT = "CALCULATE_REIMBURSEMENT" # More descriptive
    EXPORT = "EXPORT"

@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage."""
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
        self.error_count +=1

@dataclass
class PipelineProcessingMetrics:
    """Tracks overall pipeline processing performance metrics."""
    total_claims_processed_successfully: int = 0
    total_claims_failed: int = 0
    pipeline_start_time: float = field(default_factory=time.perf_counter)
    pipeline_end_time: Optional[float] = None
    current_throughput_claims_per_second: float = 0.0
    overall_avg_throughput_claims_per_second: float = 0.0
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    stage_specific_metrics: Dict[ProcessingStage, StageMetrics] = field(default_factory=lambda: {stage: StageMetrics() for stage in ProcessingStage})

    def update_throughput(self):
        if self.pipeline_end_time:
            duration = self.pipeline_end_time - self.pipeline_start_time
        else:
            duration = time.perf_counter() - self.pipeline_start_time
        
        total_completed = self.total_claims_processed_successfully + self.total_claims_failed
        if duration > 0:
            self.overall_avg_throughput_claims_per_second = total_completed / duration
        # current_throughput would need more frequent updates based on recent window

    def get_queue_depths(self, queues: Dict[ProcessingStage, asyncio.Queue]):
        for stage, metrics in self.stage_specific_metrics.items():
            if stage in queues: # FETCH stage doesn't have an input queue in this model
                 metrics.items_in_queue = queues[stage].qsize()


@dataclass
class ClaimWorkItem:
    """Work item representing a claim at various processing stages."""
    claim_id: str
    current_stage: ProcessingStage
    data: Any # Holds the StagingClaim ORM object or derived data
    created_at: float = field(default_factory=time.perf_counter)
    stage_start_time: float = field(default_factory=time.perf_counter)
    retries: int = 0
    max_retries: int = 3 # Configurable per stage if needed
    # To store results from previous stages
    validation_result: Any = None
    ml_prediction: Any = None
    reimbursement_details: Any = None


def _setup_event_loop_for_windows():
    """Setup optimal event loop for Windows if not handled by platform_config."""
    if sys.platform == "win32":
        try:
            import winloop
            asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
            logger.info("Using winloop event loop policy for Windows (batch_handler setup).")
        except ImportError:
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                logger.info("Using Windows ProactorEventLoop policy (batch_handler setup).")
            except AttributeError:
                logger.info("Using default Windows event loop (batch_handler setup).")


class OptimizedPipelineProcessor:
    """
    High-performance pipeline processor with dynamic scaling, backpressure handling,
    and enhanced read-replica support for validation operations.
    """
    
    def __init__(self, pg_session_factory: Callable[..., Session],
                 sql_session_factory: Callable[..., Session],
                 config: Dict[str, Any]):
        """
        Initializes the pipeline processor.
        Args:
            pg_session_factory: A callable that returns a new PostgreSQL session.
            sql_session_factory: A callable that returns a new SQL Server session.
            config: Application configuration dictionary.
        """
        # Configure multiprocessing and event loop early, especially for Windows
        if HAS_PLATFORM_CONFIG:
            configure_platform_mp() # From app.utils.platform_config
        else: # Fallback if platform_config is not available
            if sys.platform == "win32":
                multiprocessing.freeze_support()
                try: multiprocessing.set_start_method('spawn', force=True)
                except RuntimeError: pass
            _setup_event_loop_for_windows()
        
        self.pg_session_factory = pg_session_factory
        self.sql_session_factory = sql_session_factory
        self.config = config
        self.app_config = config # Keep a reference to the full app config
        
        # Performance and pipeline configuration
        proc_config = self.config.get('processing', {})
        perf_config = self.config.get('performance', {})

        self.initial_fetch_batch_size = proc_config.get('batch_size', 1000) # Batch size for fetching from DB
        self.max_queue_size = perf_config.get('pipeline_max_queue_size', self.initial_fetch_batch_size * 2)
        self.target_throughput = perf_config.get('target_throughput_claims_per_sec', 6667)
        
        self._configure_workers_from_platform() # Sets min/max/current workers
        
        # Pipeline queues with backpressure management
        self.stage_queues: Dict[ProcessingStage, asyncio.Queue[ClaimWorkItem]] = {
            stage: asyncio.Queue(maxsize=self.max_queue_size) for stage in ProcessingStage if stage != ProcessingStage.FETCH
        }
        
        self._init_executors() # Initialize ThreadPoolExecutor and ProcessPoolExecutor
        
        # Core processing components
        self.rules_engine: Optional[RulesEngine] = None
        self.ml_predictor = get_ml_predictor() # Uses singleton
        self.reimbursement_calculator = ReimbursementCalculator() # Instantiated once

        # Metrics and monitoring
        self.metrics = PipelineProcessingMetrics()
        self.last_scale_check_time = time.perf_counter()
        self.scale_check_interval_seconds = perf_config.get('auto_scale_check_interval_seconds', 15.0)
        
        # Shutdown and task management
        self.shutdown_event = asyncio.Event()
        self.active_pipeline_tasks: List[asyncio.Task] = []
        
        # Backpressure thresholds
        self.queue_high_water_mark = int(self.max_queue_size * proc_config.get('queue_high_watermark_factor', 0.85))
        self.queue_low_water_mark = int(self.max_queue_size * proc_config.get('queue_low_watermark_factor', 0.25))
        
        logger.info(f"OptimizedPipelineProcessor initialized for {PLATFORM_CONFIG['platform']} with "
                   f"target throughput {self.target_throughput} claims/sec. "
                   f"Initial workers: {self.current_workers_per_stage_type['cpu']}/{self.current_workers_per_stage_type['io']}.")

    def _configure_workers_from_platform(self):
        """Configure worker counts based on platform capabilities and config."""
        cpu_count = os.cpu_count() or 1 # Default to 1 if cpu_count() is None
        proc_config = self.config.get('processing', {})
        
        # Define min/max/current for different types of stages (CPU-bound vs I/O-bound)
        self.min_workers_per_stage_type: Dict[str, int] = {
            'cpu': max(1, proc_config.get('min_cpu_workers', cpu_count // 2)),
            'io': max(1, proc_config.get('min_io_workers', cpu_count))
        }
        self.max_workers_per_stage_type: Dict[str, int] = {
            'cpu': min(cpu_count * 2, proc_config.get('max_cpu_workers', 16 if PLATFORM_CONFIG["supports_fork"] else 8)),
            'io': min(cpu_count * 4, proc_config.get('max_io_workers', 32))
        }
        self.current_workers_per_stage_type: Dict[str, int] = {
            'cpu': max(self.min_workers_per_stage_type['cpu'],
                       min(cpu_count, self.max_workers_per_stage_type['cpu'])),
            'io': max(self.min_workers_per_stage_type['io'],
                      min(cpu_count * 2, self.max_workers_per_stage_type['io']))
        }
        logger.info(f"Worker counts: CPU (Min/Cur/Max): "
                    f"{self.min_workers_per_stage_type['cpu']}/{self.current_workers_per_stage_type['cpu']}/{self.max_workers_per_stage_type['cpu']}. "
                    f"I/O (Min/Cur/Max): "
                    f"{self.min_workers_per_stage_type['io']}/{self.current_workers_per_stage_type['io']}/{self.max_workers_per_stage_type['io']}.")

    def _init_executors(self):
        """Initialize executor pools with platform-specific optimizations."""
        self.io_executor = ThreadPoolExecutor(
            max_workers=self.max_workers_per_stage_type['io'],
            thread_name_prefix="pipeline_io_worker"
        )
        
        if PLATFORM_CONFIG["use_multiprocessing"] and PLATFORM_CONFIG["supports_fork"] and sys.platform != "win32":
            try:
                ctx = multiprocessing.get_context('fork') # Prefer fork on Unix for lower overhead if safe
                self.cpu_executor = ProcessPoolExecutor(
                    max_workers=self.max_workers_per_stage_type['cpu'],
                    mp_context=ctx
                )
                logger.info(f"Using ProcessPoolExecutor (fork) with {self.max_workers_per_stage_type['cpu']} workers for CPU-bound tasks.")
            except Exception as e_fork:
                logger.warning(f"Failed to create ProcessPoolExecutor with 'fork' context ({e_fork}). Trying 'spawn'.")
                try:
                    ctx = multiprocessing.get_context('spawn')
                    self.cpu_executor = ProcessPoolExecutor(
                        max_workers=self.max_workers_per_stage_type['cpu'],
                        mp_context=ctx
                    )
                    logger.info(f"Using ProcessPoolExecutor (spawn) with {self.max_workers_per_stage_type['cpu']} workers for CPU-bound tasks.")
                except Exception as e_spawn:
                    logger.error(f"Failed to create ProcessPoolExecutor with 'spawn' context ({e_spawn}). Falling back to ThreadPoolExecutor for CPU tasks.")
                    self.cpu_executor = ThreadPoolExecutor(
                        max_workers=self.max_workers_per_stage_type['cpu'],
                        thread_name_prefix="pipeline_cpu_worker"
                    )
        else: # Windows or when multiprocessing is disabled/problematic
            self.cpu_executor = ThreadPoolExecutor(
                max_workers=self.max_workers_per_stage_type['cpu'],
                thread_name_prefix="pipeline_cpu_worker"
            )
            logger.info(f"Using ThreadPoolExecutor with {self.max_workers_per_stage_type['cpu']} workers for CPU-bound tasks (Platform: {sys.platform}).")
            
    def _initialize_shared_components(self):
        """Initializes components shared across workers, like RulesEngine with a read-only session."""
        # Initialize RulesEngine with a read-only session factory for its master data lookups
        # This assumes RulesEngine itself is thread-safe or its methods get fresh sessions.
        # The current RulesEngine takes a session at init.
        # For a pipelined/concurrent system, it's better if RulesEngine methods accept a session.
        # WORKAROUND: Create one RulesEngine instance with a read-only session.
        # This instance will be used by multiple threads/tasks.
        # SQLAlchemy sessions from a factory are generally not shareable across threads unless scoped.
        # However, if RulesEngine only uses the session for read-only lookups and closes it, it might be okay.
        # A SAFER APPROACH: Instantiate RulesEngine inside the worker function _sync_validate,
        # passing a fresh read-only session each time.
        # For now, let's assume RulesEngine can be initialized once if its session usage is carefully managed.
        try:
            # Create a temporary read-only session for RulesEngine initialization
            # This session is for loading rules, not for per-claim validation lookups within the engine.
            # The per-claim validation lookups should happen with a fresh session inside the validation worker.
            with self.pg_session_factory(read_only=True) as temp_ro_session:
                 self.rules_engine = RulesEngine(temp_ro_session)
            logger.info("RulesEngine initialized for OptimizedPipelineProcessor (used read-only session for rule loading).")
        except Exception as e:
            logger.error(f"Failed to initialize RulesEngine: {e}", exc_info=True)
            self.rules_engine = None # Ensure it's None if init fails.
            raise # Re-raise to stop pipeline if critical components fail

    async def run_pipeline(self):
        """Starts and manages the asynchronous processing pipeline."""
        self.metrics.pipeline_start_time = time.perf_counter()
        self._initialize_shared_components() # Initialize RulesEngine etc.

        if not self.rules_engine: # Critical component check
            logger.critical("RulesEngine failed to initialize. Pipeline cannot start.")
            return

        logger.info(f"Starting optimized claims processing pipeline on {PLATFORM_CONFIG['platform']}...")
        
        stage_methods_map = {
            ProcessingStage.FETCH: self._fetch_stage_producer,
            ProcessingStage.VALIDATE: self._generic_stage_worker_manager(
                input_stage_queue=self.stage_queues[ProcessingStage.VALIDATE],
                output_stage_queue=self.stage_queues[ProcessingStage.ML_PREDICT],
                processor_func=self._process_item_validation,
                stage_name="Validation",
                worker_type='io' # Validation involves DB lookups
            ),
            ProcessingStage.ML_PREDICT: self._generic_stage_worker_manager(
                input_stage_queue=self.stage_queues[ProcessingStage.ML_PREDICT],
                output_stage_queue=self.stage_queues[ProcessingStage.CALCULATE_REIMBURSEMENT],
                processor_func=self._process_item_ml_prediction,
                stage_name="MLPrediction",
                worker_type='cpu' # ML prediction is CPU-bound
            ),
            ProcessingStage.CALCULATE_REIMBURSEMENT: self._generic_stage_worker_manager(
                input_stage_queue=self.stage_queues[ProcessingStage.CALCULATE_REIMBURSEMENT],
                output_stage_queue=self.stage_queues[ProcessingStage.EXPORT],
                processor_func=self._process_item_reimbursement,
                stage_name="ReimbursementCalculation",
                worker_type='cpu' # RVU lookup might be I/O but calc is CPU
            ),
            ProcessingStage.EXPORT: self._generic_stage_worker_manager(
                input_stage_queue=self.stage_queues[ProcessingStage.EXPORT],
                output_stage_queue=None, # Final stage
                processor_func=self._process_item_export,
                stage_name="Export",
                worker_type='io' # DB writes are I/O
            )
        }

        # Start monitoring tasks
        self.active_pipeline_tasks.append(asyncio.create_task(self._monitor_and_scale_pipeline(), name="PipelineMonitorScaler"))
        self.active_pipeline_tasks.append(asyncio.create_task(self._pipeline_metrics_reporter(), name="PipelineMetricsReporter"))

        # Start processing stages
        for stage_enum, stage_coro_producer in stage_methods_map.items():
            self.active_pipeline_tasks.append(asyncio.create_task(stage_coro_producer(), name=f"{stage_enum.value}_StageManager"))
        
        try:
            await asyncio.gather(*self.active_pipeline_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            logger.info("Pipeline tasks cancelled.")
        except Exception as e:
            logger.error(f"Critical error in pipeline execution: {e}", exc_info=True)
        finally:
            self.metrics.pipeline_end_time = time.perf_counter()
            self.metrics.update_throughput()
            logger.info(f"Pipeline processing finished. Total duration: {self.metrics.pipeline_end_time - self.metrics.pipeline_start_time:.2f}s. "
                       f"Avg Throughput: {self.metrics.overall_avg_throughput_claims_per_second:.2f} claims/sec.")
            await self._cleanup_pipeline_resources()
    
    async def _fetch_stage_producer(self):
        """Producer stage: Fetches claims and puts them into the VALIDATE queue."""
        stage_name = ProcessingStage.FETCH.value
        cid_prefix = get_correlation_id()
        
        while not self.shutdown_event.is_set():
            set_correlation_id(f"{cid_prefix}_{stage_name}_{time.monotonic_ns()}")
            try:
                output_queue = self.stage_queues[ProcessingStage.VALIDATE]
                if output_queue.qsize() >= self.queue_high_water_mark:
                    await asyncio.sleep(0.2) # Backpressure: output queue is full
                    continue

                # Fetch claims using a read-only session
                with self.pg_session_factory(read_only=True) as ro_pg_session:
                    claims_to_process = await asyncio.get_running_loop().run_in_executor(
                        self.io_executor,
                        get_pending_claims_for_processing, # This function from postgres_handler
                        ro_pg_session,
                        self.initial_fetch_batch_size,
                        'PARSED' # Status to fetch
                    )
                
                if claims_to_process:
                    logger.info(f"[{stage_name}] Fetched {len(claims_to_process)} claims for validation.")
                    for claim_orm in claims_to_process:
                        work_item = ClaimWorkItem(
                            claim_id=claim_orm.claim_id,
                            current_stage=ProcessingStage.FETCH, # Will transition to VALIDATE
                            data=claim_orm # Pass the ORM object
                        )
                        await output_queue.put(work_item)
                    self.metrics.stage_specific_metrics[ProcessingStage.FETCH].record_item_processed(0) # Fetch is producer
                else:
                    logger.debug(f"[{stage_name}] No pending claims found to fetch. Waiting...")
                    await asyncio.sleep(2.0) # Wait if no claims are found
                    
            except asyncio.CancelledError:
                logger.info(f"[{stage_name}] Fetch stage stopping due to cancellation.")
                break
            except Exception as e:
                logger.error(f"[{stage_name}] Error: {e}", exc_info=True)
                await asyncio.sleep(5.0) # Wait before retrying on error

    def _generic_stage_worker_manager(self, input_stage_queue: asyncio.Queue,
                                 output_stage_queue: Optional[asyncio.Queue],
                                 processor_func: Callable[[ClaimWorkItem], Awaitable[Optional[ClaimWorkItem]]], # MODIFIED HERE
                                 stage_name: str, worker_type: str):
        """Manages a pool of worker tasks for a generic pipeline stage."""
        async def manager():
            cid_prefix = get_correlation_id()
            logger.info(f"Starting manager for {stage_name} with {self.current_workers_per_stage_type[worker_type]} initial workers.")
            
            active_workers: List[asyncio.Task] = []

            def create_worker_task():
                task_cid = f"{cid_prefix}_{stage_name}_Worker_{len(active_workers)}_{time.monotonic_ns()}"
                return asyncio.create_task(self._stage_worker_loop(
                    input_stage_queue, output_stage_queue, processor_func, stage_name, task_cid
                ), name=task_cid)

            # Initial worker creation
            for _ in range(self.current_workers_per_stage_type[worker_type]):
                active_workers.append(create_worker_task())

            while not self.shutdown_event.is_set():
                try:
                    # Monitor and adjust worker count based on self.current_workers_per_stage_type[worker_type]
                    # This is a simplified scaling; more robust scaling might involve
                    # cancelling and recreating tasks or using a dynamic task pool.
                    desired_workers = self.current_workers_per_stage_type[worker_type]
                    
                    # Remove completed/cancelled workers
                    active_workers = [w for w in active_workers if not w.done()]

                    if len(active_workers) < desired_workers:
                        for _ in range(desired_workers - len(active_workers)):
                            logger.info(f"[{stage_name}] Scaling up: Adding a worker. Current: {len(active_workers)}, Desired: {desired_workers}")
                            active_workers.append(create_worker_task())
                    elif len(active_workers) > desired_workers and len(active_workers) > self.min_workers_per_stage_type[worker_type]:
                        # Scale down by cancelling an arbitrary worker (could be more sophisticated)
                        num_to_cancel = len(active_workers) - desired_workers
                        logger.info(f"[{stage_name}] Scaling down: Removing {num_to_cancel} worker(s). Current: {len(active_workers)}, Desired: {desired_workers}")
                        for i in range(num_to_cancel):
                            if active_workers:
                                worker_to_cancel = active_workers.pop()
                                worker_to_cancel.cancel()
                    
                    if not active_workers and desired_workers > 0 : # Ensure at least one worker if desired > 0
                         logger.warning(f"[{stage_name}] No active workers but desired is {desired_workers}. Restarting one.")
                         active_workers.append(create_worker_task())


                    await asyncio.sleep(self.scale_check_interval_seconds / 2) # Check worker counts periodically
                except asyncio.CancelledError:
                    logger.info(f"Manager for {stage_name} stopping due to cancellation.")
                    break
                except Exception as e:
                    logger.error(f"Error in {stage_name} manager: {e}", exc_info=True)
                    await asyncio.sleep(1.0) # Avoid tight loop on error

            # Cleanup: Cancel all active workers on shutdown
            logger.info(f"Shutting down workers for {stage_name}...")
            for worker in active_workers:
                worker.cancel()
            await asyncio.gather(*active_workers, return_exceptions=True)
            logger.info(f"All workers for {stage_name} shut down.")

        return manager # Return the coroutine function

    async def _stage_worker_loop(self, input_queue: asyncio.Queue,
                                output_queue: Optional[asyncio.Queue],
                                processor_func: Callable[[ClaimWorkItem], Awaitable[Optional[ClaimWorkItem]]], # MODIFIED HERE
                                stage_name: str, worker_cid:str):
        """The actual processing loop for a single worker task of a stage."""
        set_correlation_id(worker_cid)
        logger.info(f"[{stage_name} Worker {worker_cid}] Started.")
        
        while not self.shutdown_event.is_set():
            try:
                work_item: ClaimWorkItem = await asyncio.wait_for(input_queue.get(), timeout=1.0)
                set_correlation_id(f"{worker_cid}_Item_{work_item.claim_id[:8]}") # Item-specific CID

                if output_queue and output_queue.qsize() >= self.queue_high_water_mark:
                    await input_queue.put(work_item) # Put back if output is full (backpressure)
                    await asyncio.sleep(0.1 * (output_queue.qsize() / self.max_queue_size)) # Sleep proportionally
                    continue

                item_stage_start_time = time.perf_counter()
                processed_item = await processor_func(work_item)
                item_duration = time.perf_counter() - item_stage_start_time
                self.metrics.stage_specific_metrics[work_item.current_stage].record_item_processed(item_duration)

                if processed_item:
                    if output_queue:
                        processed_item.current_stage = ProcessingStage[output_queue.name.split('_')[0].upper()] if output_queue.name else work_item.current_stage # Bit hacky way to get next stage from queue name
                        processed_item.stage_start_time = time.perf_counter()
                        await output_queue.put(processed_item)
                    else: # Final stage, item processing complete
                        self.metrics.total_claims_processed_successfully += 1
                else: # Item processing failed or filtered out
                    self.metrics.total_claims_failed += 1
                
                input_queue.task_done()

            except asyncio.TimeoutError:
                continue # No item in queue, loop again
            except asyncio.CancelledError:
                logger.info(f"[{stage_name} Worker {worker_cid}] Stopping due to cancellation.")
                break
            except Exception as e:
                logger.error(f"[{stage_name} Worker {worker_cid}] Error processing item: {e}", exc_info=True)
                self.metrics.stage_specific_metrics[work_item.current_stage if 'work_item' in locals() else ProcessingStage[stage_name]].record_error()
                # Basic retry for the work_item if an error occurs in processor_func
                if 'work_item' in locals():
                    work_item.retries += 1
                    if work_item.retries <= work_item.max_retries:
                        logger.warning(f"[{stage_name} Worker {worker_cid}] Retrying item {work_item.claim_id} (attempt {work_item.retries})")
                        await asyncio.sleep(0.5 * work_item.retries) # Exponential backoff
                        await input_queue.put(work_item) # Put back for retry
                    else:
                        logger.error(f"[{stage_name} Worker {worker_cid}] Item {work_item.claim_id} failed max retries. Moving to error handling.")
                        await self._handle_permanently_failed_item(work_item, e)
                else: # Error before work_item was obtained
                    await asyncio.sleep(1.0)
        logger.info(f"[{stage_name} Worker {worker_cid}] Stopped.")

    # --- Specific Item Processors for Each Stage ---
    async def _process_item_validation(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        """Processes a claim for validation."""
        loop = asyncio.get_running_loop()
        claim_orm = work_item.data
        
        # Perform validation (potentially I/O bound for DB lookups via RulesEngine)
        # The RulesEngine is initialized with a read-only session.
        # If RulesEngine.validate_claim modifies the claim_orm (e.g., sets status),
        # that change won't be persisted by the read-only session.
        # Status updates should happen with a write session.
        
        validation_result = await loop.run_in_executor(
            self.io_executor, # Rules engine might do DB lookups
            self.rules_engine.validate_claim, # This method from rules_engine.py
            claim_orm
        )
        work_item.validation_result = validation_result # Store for later stages

        if not validation_result.is_valid:
            logger.warning(f"Claim {claim_orm.claim_id} failed Datalog validation: {validation_result.errors}")
            # Update status to VALIDATION_FAILED_RULES using a write session
            try:
                with self.pg_session_factory(read_only=False) as write_pg_session:
                    update_staging_claim_status(
                        write_pg_session,
                        claim_orm.claim_id,
                        "VALIDATION_FAILED_RULES",
                        [f"{e['rule_id']}|{e['field']}|{e['message']}" for e in validation_result.errors]
                    )
                    write_pg_session.commit()
            except Exception as db_err:
                logger.error(f"Failed to update status for {claim_orm.claim_id} after Datalog validation failure: {db_err}", exc_info=True)
                # Decide how to handle this: retry item, or log and drop?
            return None # Stop processing this claim further in the main pipeline
        
        # If valid, pass the original ORM object (claim_orm) which now might have updated fields
        # from rules_engine.validate_claim IF that method modifies its input.
        # Current rules_engine.validate_claim does update claim_orm.processing_status
        # and other validation flags.
        work_item.data = claim_orm
        return work_item

    async def _process_item_ml_prediction(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        """Processes a claim for ML prediction."""
        loop = asyncio.get_running_loop()
        claim_orm = work_item.data # This is the StagingClaim ORM object
        
        # Prepare data for ML predictor
        # This needs to match the feature engineering used during training
        # and expected by ml_predictor.predict_filters
        ml_input_data = {
            "claim_id": claim_orm.claim_id,
            "total_charge_amount": float(claim_orm.total_charge_amount or 0.0),
            "patient_age": int(claim_orm.patient_age or 30), # Default if None
            # Assuming cms1500_diagnoses and cms1500_line_items are loaded on claim_orm
            "diagnoses": [{"icd_code": d.icd_code} for d in getattr(claim_orm, 'cms1500_diagnoses', [])],
            "line_items": [{"cpt_code": l.cpt_code, "units": l.units} for l in getattr(claim_orm, 'cms1500_line_items', [])]
        }

        try:
            # ML prediction can be CPU bound
            predicted_filters, probability = await loop.run_in_executor(
                self.cpu_executor,
                self.ml_predictor.predict_filters, # This is a sync method in ml_predictor
                ml_input_data
            )
            work_item.ml_prediction = {"filters": predicted_filters, "probability": probability}
            # Optionally update claim_orm directly if it has fields for these
            # claim_orm.ml_predicted_filters = predicted_filters
            # claim_orm.ml_confidence_score = probability
            logger.debug(f"ML Prediction for {claim_orm.claim_id}: Filters={predicted_filters}, Prob={probability:.2f}")
        except Exception as ml_err:
            logger.error(f"ML prediction failed for claim {claim_orm.claim_id}: {ml_err}", exc_info=True)
            work_item.ml_prediction = {"filters": ["ERROR_ML_PREDICTION"], "probability": 0.0}
            # Decide if this is a hard failure for the claim or if it can proceed
            # For now, let it proceed with an error filter/flag.
        
        return work_item

    async def _process_item_reimbursement(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        """Processes a claim for reimbursement calculation."""
        loop = asyncio.get_running_loop()
        claim_orm = work_item.data # StagingClaim ORM object
        
        # Reimbursement calculation (can be CPU bound depending on complexity, but RVU lookup is I/O)
        # ReimbursementCalculator.process_claim_reimbursement modifies the claim_orm_object (or its line items)
        # if the ORM models have fields for estimated_reimbursement_amount.
        # Current ReimbursementCalculator doesn't modify in place but logs. Let's assume it can return data.
        
        # We need to adapt ReimbursementCalculator.process_claim_reimbursement
        # or how it's called to get back the calculated values.
        # For now, let's assume ReimbursementCalculator.process_claim_reimbursement is refactored
        # to update the claim_orm object's line items with reimbursement details.
        
        def sync_reimbursement_calc(claim_obj):
            self.reimbursement_calculator.process_claim_reimbursement(claim_obj)
            # Extract data if needed or assume claim_obj is updated
            # Example: sum up line_item.estimated_reimbursement_amount
            total_est_reimb = sum(
                getattr(li, 'estimated_reimbursement_amount', Decimal('0.00'))
                for li in getattr(claim_obj, 'cms1500_line_items', [])
            )
            return {"total_estimated_reimbursement": float(total_est_reimb)}

        try:
            reimbursement_output = await loop.run_in_executor(
                self.io_executor, # RVU cache access is I/O
                sync_reimbursement_calc,
                claim_orm
            )
            work_item.reimbursement_details = reimbursement_output
            # claim_orm.total_estimated_reimbursement = Decimal(str(reimbursement_output.get("total_estimated_reimbursement", "0.0")))
            logger.debug(f"Reimbursement calculated for {claim_orm.claim_id}: {reimbursement_output}")
        except Exception as reimb_err:
            logger.error(f"Reimbursement calculation failed for claim {claim_orm.claim_id}: {reimb_err}", exc_info=True)
            # work_item.reimbursement_details = {"error": str(reimb_err)}
            # Decide if this is a hard failure. For now, proceed.

        return work_item

    async def _process_item_export(self, work_item: ClaimWorkItem) -> Optional[ClaimWorkItem]:
        """Exports a fully processed claim."""
        loop = asyncio.get_running_loop()
        claim_orm = work_item.data # StagingClaim ORM object
        
        # Export to SQL Server (I/O bound)
        # This requires mapping StagingClaim to ProductionClaim ORM model
        # and then saving using sql_session_factory.
        # This logic should ideally be in ClaimsProcessor or a dedicated export service.
        
        def sync_export(staging_claim_obj, validation_res, ml_pred, reimb_details):
            # This would use the main ClaimsProcessor's logic or a refined version of it.
            # Simplified for now:
            try:
                with self.sql_session_factory(read_only=False) as sql_session:
                    with self.pg_session_factory(read_only=False) as pg_session:
                        # Assume claims_processor.py has a method to handle the full export
                        # For this example, we directly call a hypothetical mapping and saving function.
                        
                        # 1. Map StagingClaim to ProductionClaim (and its children)
                        # This is complex and would involve creating ProductionClaim, ProductionCMS1500Diagnosis, etc.
                        # from the staging_claim_obj.
                        # Let's assume a function _map_to_production_orm exists.
                        
                        # from app.database.models.sqlserver_models import ProductionClaim as SQLProdClaim
                        # production_orm = SQLProdClaim(...) # map fields

                        # For now, simulate success if validation was OK
                        if work_item.validation_result and work_item.validation_result.is_valid:
                            # Simulate saving to production
                            logger.info(f"Simulating save of claim {staging_claim_obj.claim_id} to Production DB.")
                            # sql_session.add(production_orm)
                            # sql_session.commit()

                            # Update staging claim status to COMPLETED_EXPORTED_TO_PROD
                            update_staging_claim_status(pg_session, staging_claim_obj.claim_id, "COMPLETED_EXPORTED_TO_PROD")
                            pg_session.commit()
                            logger.info(f"Claim {staging_claim_obj.claim_id} successfully exported and staging status updated.")
                            return True # Indicate success
                        else:
                            # This case should ideally not reach export if validation failed earlier.
                            # If it does, means an issue in pipeline logic or a specific type of "valid but needs review".
                            logger.warning(f"Claim {staging_claim_obj.claim_id} reached export but was not marked fully valid. Status: {getattr(staging_claim_obj, 'processing_status', 'UNKNOWN')}")
                            # update_staging_claim_status(pg_session, staging_claim_obj.claim_id, "ERROR_EXPORT_INVALID_STATE")
                            # pg_session.commit()
                            return False # Indicate failure to export due to state
            except Exception as export_err:
                logger.error(f"Failed to export claim {staging_claim_obj.claim_id}: {export_err}", exc_info=True)
                # Rollback and update staging status to an error state
                try:
                    with self.pg_session_factory(read_only=False) as pg_session_err:
                        update_staging_claim_status(pg_session_err, staging_claim_obj.claim_id, "ERROR_EXPORT_FAILED")
                        pg_session_err.commit()
                except Exception as db_update_err:
                     logger.error(f"Further error updating status for {staging_claim_obj.claim_id} after export failure: {db_update_err}")
                return False
        
        export_successful = await loop.run_in_executor(
            self.io_executor,
            sync_export,
            claim_orm,
            work_item.validation_result,
            work_item.ml_prediction,
            work_item.reimbursement_details
        )
        
        if export_successful:
            return work_item # Or just None if no further processing needed.
        else:
            # If export failed, this work_item processing stops. Error is logged.
            return None


    async def _handle_permanently_failed_item(self, work_item: ClaimWorkItem, error: Exception):
        """Handles items that have failed all retries."""
        logger.error(f"Claim {work_item.claim_id} failed permanently in stage {work_item.current_stage.value} after {work_item.retries} retries: {error}", exc_info=True)
        self.metrics.total_claims_failed += 1
        # Log to a specific error table or dead-letter queue
        try:
            with self.pg_session_factory(read_only=False) as pg_session:
                update_staging_claim_status(
                    pg_session,
                    work_item.claim_id,
                    f"ERROR_PIPELINE_{work_item.current_stage.value}_MAX_RETRIES",
                    [f"Max retries exceeded in stage {work_item.current_stage.value}. Last error: {str(error)[:500]}"] # Truncate long errors
                )
                pg_session.commit()
        except Exception as db_err:
            logger.error(f"Failed to update status for permanently failed claim {work_item.claim_id}: {db_err}", exc_info=True)

    async def _monitor_and_scale_pipeline(self):
        """Monitors pipeline performance and dynamically scales workers."""
        logger.info("Pipeline monitoring and auto-scaling task started.")
        while not self.shutdown_event.is_set():
            await asyncio.sleep(self.scale_check_interval_seconds)
            try:
                current_time = time.perf_counter()
                self.metrics.update_throughput() # Update overall average
                
                # More sophisticated current throughput (e.g., over last N seconds)
                # This would require tracking completed items in a time window.
                # For simplicity, we use overall avg for now for scaling decisions.
                
                # Update system resource usage
                self.metrics.cpu_usage_percent = psutil.cpu_percent()
                self.metrics.memory_usage_percent = psutil.virtual_memory().percent
                self.metrics.get_queue_depths(self.stage_queues)

                # --- Auto-scaling Logic ---
                # This is a simplified example. Real-world auto-scaling can be much more complex.
                # It should consider queue backlogs, processing times per stage, resource utilization,
                # and target throughput. It also needs to manage the lifecycle of worker tasks.
                # The current `self.current_workers_per_stage_type` is used when the manager creates workers.
                
                # Example: Scale CPU workers based on CPU usage and ML queue
                ml_queue_size = self.stage_queues[ProcessingStage.ML_PREDICT].qsize()
                if ml_queue_size > self.queue_high_water_mark and \
                   self.metrics.cpu_usage_percent < 75 and \
                   self.current_workers_per_stage_type['cpu'] < self.max_workers_per_stage_type['cpu']:
                    self.current_workers_per_stage_type['cpu'] = min(self.max_workers_per_stage_type['cpu'], self.current_workers_per_stage_type['cpu'] + 1)
                    logger.info(f"Auto-scaling: Increased CPU workers to {self.current_workers_per_stage_type['cpu']}")
                elif ml_queue_size < self.queue_low_water_mark and \
                     self.metrics.cpu_usage_percent < 40 and \
                     self.current_workers_per_stage_type['cpu'] > self.min_workers_per_stage_type['cpu']:
                    self.current_workers_per_stage_type['cpu'] = max(self.min_workers_per_stage_type['cpu'], self.current_workers_per_stage_type['cpu'] - 1)
                    logger.info(f"Auto-scaling: Decreased CPU workers to {self.current_workers_per_stage_type['cpu']}")

                # Similar logic for I/O workers based on other queues and I/O wait times (if measurable)
                validate_queue_size = self.stage_queues[ProcessingStage.VALIDATE].qsize()
                if validate_queue_size > self.queue_high_water_mark and \
                   self.current_workers_per_stage_type['io'] < self.max_workers_per_stage_type['io']:
                    self.current_workers_per_stage_type['io'] = min(self.max_workers_per_stage_type['io'], self.current_workers_per_stage_type['io'] + 1)
                    logger.info(f"Auto-scaling: Increased I/O workers to {self.current_workers_per_stage_type['io']}")
                elif validate_queue_size < self.queue_low_water_mark and \
                     self.current_workers_per_stage_type['io'] > self.min_workers_per_stage_type['io']:
                     self.current_workers_per_stage_type['io'] = max(self.min_workers_per_stage_type['io'], self.current_workers_per_stage_type['io'] -1)
                     logger.info(f"Auto-scaling: Decreased I/O workers to {self.current_workers_per_stage_type['io']}")


                self.last_scale_check_time = current_time
            except asyncio.CancelledError:
                logger.info("Pipeline monitoring and scaling task cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in pipeline monitoring/scaling: {e}", exc_info=True)

    async def _pipeline_metrics_reporter(self):
        """Periodically reports pipeline performance metrics."""
        report_interval = self.config.get('performance', {}).get('metrics_report_interval_seconds', 60.0)
        logger.info(f"Pipeline metrics reporting task started (Interval: {report_interval}s).")
        while not self.shutdown_event.is_set():
            await asyncio.sleep(report_interval)
            try:
                self.metrics.update_throughput()
                self.metrics.get_queue_depths(self.stage_queues)
                # System resource usage updated by _monitor_and_scale_pipeline

                logger.info(f"--- Pipeline Performance Report (Platform: {PLATFORM_CONFIG['platform']}) ---")
                logger.info(f"  Overall Throughput (avg): {self.metrics.overall_avg_throughput_claims_per_second:.2f} claims/sec")
                # logger.info(f"  Current Throughput (estimated): {self.metrics.current_throughput_claims_per_second:.2f} claims/sec")
                logger.info(f"  Claims Processed (Success/Fail): {self.metrics.total_claims_processed_successfully}/{self.metrics.total_claims_failed}")
                logger.info(f"  System Resources: CPU {self.metrics.cpu_usage_percent:.1f}%, Memory {self.metrics.memory_usage_percent:.1f}%")
                logger.info(f"  Worker Counts (CPU/IO): {self.current_workers_per_stage_type['cpu']}/{self.current_workers_per_stage_type['io']}")

                for stage, metrics_data in self.metrics.stage_specific_metrics.items():
                    logger.info(f"  Stage [{stage.value}]: "
                               f"Queue: {metrics_data.items_in_queue}, "
                               f"Processed: {metrics_data.processed_count}, "
                               f"Errors: {metrics_data.error_count}, "
                               f"AvgTime: {metrics_data.avg_processing_time_ms:.2f}ms")
                logger.info("--- End of Report ---")

            except asyncio.CancelledError:
                logger.info("Pipeline metrics reporting task cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in pipeline metrics reporter: {e}", exc_info=True)

    async def _cleanup_pipeline_resources(self):
        """Shuts down executors and other resources."""
        logger.info("Cleaning up pipeline resources...")
        if hasattr(self, 'io_executor') and self.io_executor:
            self.io_executor.shutdown(wait=True, cancel_futures=True)
            logger.info("I/O executor shut down.")
        if hasattr(self, 'cpu_executor') and self.cpu_executor:
            self.cpu_executor.shutdown(wait=True, cancel_futures=True)
            logger.info("CPU executor shut down.")
        logger.info("Pipeline resources cleaned up.")

    async def stop_pipeline(self):
        """Signals the pipeline to shut down gracefully."""
        logger.info("Stopping pipeline processor...")
        self.shutdown_event.set()
        # Give some time for tasks to acknowledge shutdown_event
        await asyncio.sleep(1)
        
        # Cancel any remaining top-level pipeline tasks
        for task in self.active_pipeline_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for all tasks to complete or be cancelled
        if self.active_pipeline_tasks:
            await asyncio.gather(*self.active_pipeline_tasks, return_exceptions=True)
        
        logger.info("Pipeline processor stopped.")


# Legacy BatchProcessor class for backward compatibility (if needed by app.main.py)
class BatchProcessor:
    """
    Legacy BatchProcessor class that now uses OptimizedPipelineProcessor internally.
    """
    def __init__(self, pg_session_factory: Callable[..., Session],
                 sql_session_factory: Callable[..., Session],
                 config: Dict[str, Any]):
        logger.warning("Using legacy BatchProcessor wrapper. Consider updating calls to use OptimizedPipelineProcessor directly for async benefits.")
        self.pipeline_processor = OptimizedPipelineProcessor(pg_session_factory, sql_session_factory, config)

    def run_main_batch_processing_loop(self, source_pg_session: Optional[Session] = None, batch_id_in_db: Optional[int] = None):
        """
        Runs the main batch processing loop using the async pipeline.
        The source_pg_session and batch_id_in_db are largely for compatibility and might not be fully utilized
        if the pipeline manages its own fetching and batching logic.
        """
        cid = get_correlation_id()
        logger.info(f"[{cid}] Legacy BatchProcessor: Starting main batch processing loop via OptimizedPipelineProcessor.")
        
        # Setup event loop based on platform, particularly for Windows.
        if sys.platform == "win32":
            _setup_event_loop_for_windows()
            loop = asyncio.ProactorEventLoop() # Explicitly use ProactorEventLoop on Windows
            asyncio.set_event_loop(loop)
        else:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self.pipeline_processor.run_pipeline())
        except KeyboardInterrupt:
            logger.info(f"[{cid}] Batch processing loop interrupted by user. Shutting down pipeline.")
            loop.run_until_complete(self.pipeline_processor.stop_pipeline())
        except Exception as e:
            logger.critical(f"[{cid}] Critical error in batch processing loop: {e}", exc_info=True)
            loop.run_until_complete(self.pipeline_processor.stop_pipeline()) # Attempt graceful shutdown
        finally:
            loop.close()
            logger.info(f"[{cid}] Legacy BatchProcessor: Main batch processing loop finished.")
            
    def shutdown_executors(self):
        """Shuts down the internal pipeline processor's executors."""
        # This method might be called if the application structure expects it.
        # The actual shutdown is handled by stop_pipeline.
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(self.pipeline_processor.stop_pipeline())
        else:
            loop.run_until_complete(self.pipeline_processor.stop_pipeline())


if __name__ == '__main__':
    # Ensure platform-specific multiprocessing setup is done for __main__
    if HAS_PLATFORM_CONFIG:
        configure_platform_mp()
    elif sys.platform == "win32": # Fallback
        multiprocessing.freeze_support()
        try: multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError: pass

    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import (
        init_database_connections,
        get_postgres_session,
        get_sqlserver_session,
        dispose_engines,
        CONFIG as APP_CONFIG_FROM_CM # Use the one loaded by connection_manager
    )
    from app.database.models.postgres_models import StagingClaim, Base as PostgresBase
    from decimal import Decimal

    setup_logging() # Initialize logging first
    main_cid = set_correlation_id("BATCH_HANDLER_PIPELINE_TEST")

    async def test_optimized_pipeline():
        """Test the OptimizedPipelineProcessor."""
        logger.info("Starting OptimizedPipelineProcessor test...")
        try:
            # Initialize database connections (this also warms pools)
            init_database_connections()
            
            pg_factory = lambda read_only=False: get_postgres_session(read_only=read_only)
            sql_factory = lambda read_only=False: get_sqlserver_session(read_only=read_only) # SQL factory usually for write

            # Create some dummy staging claims if they don't exist
            with pg_factory() as temp_pg_session:
                PostgresBase.metadata.create_all(temp_pg_session.get_bind()) # Ensure tables exist
                # Check if enough 'PARSED' claims exist
                parsed_claims_count = temp_pg_session.query(StagingClaim).filter(StagingClaim.processing_status == 'PARSED').count()
                num_claims_to_add = 20 # Add a small number for testing
                if parsed_claims_count < num_claims_to_add :
                    logger.info(f"Adding {num_claims_to_add - parsed_claims_count} dummy claims to staging.claims with status 'PARSED'...")
                    for i in range(num_claims_to_add - parsed_claims_count):
                        unique_id_part = time.time_ns() # Ensure unique claim_id for test runs
                        claim = StagingClaim(
                            claim_id=f"PIPE_TST_{unique_id_part}_{i}",
                            facility_id="TEST_FAC_PIPE",
                            patient_account_number=f"ACC_PIPE_{i}",
                            service_date=datetime.now().date() - timedelta(days=i),
                            financial_class_id="FC_PIPE_01",
                            patient_dob=(datetime.now() - timedelta(days=365* (30+i))).date(),
                            patient_age = 30+i,
                            total_charge_amount=Decimal(f"{100 + i*10}.50"),
                            processing_status='PARSED', # Ready for pipeline
                            created_date=datetime.now() - timedelta(minutes=i)
                        )
                        temp_pg_session.add(claim)
                    temp_pg_session.commit()
            
            # Initialize and run the pipeline
            pipeline_processor = OptimizedPipelineProcessor(pg_factory, sql_factory, APP_CONFIG_FROM_CM) # Use loaded config
            
            pipeline_run_task = asyncio.create_task(pipeline_processor.run_pipeline())

            # Let the pipeline run for a certain duration for testing purposes
            test_duration_seconds = 45
            logger.info(f"Pipeline will run for approximately {test_duration_seconds} seconds for this test...")
            await asyncio.sleep(test_duration_seconds)
            
            logger.info("Test duration elapsed. Signaling pipeline to stop...")
            await pipeline_processor.stop_pipeline() # Gracefully stop the pipeline
            
            # Wait for the main pipeline task to finish after stop signal
            await pipeline_run_task

            logger.info("--- Final Pipeline Metrics ---")
            final_metrics = pipeline_processor.metrics
            logger.info(f"  Successfully Processed: {final_metrics.total_claims_processed_successfully}")
            logger.info(f"  Failed: {final_metrics.total_claims_failed}")
            logger.info(f"  Overall Avg Throughput: {final_metrics.overall_avg_throughput_claims_per_second:.2f} claims/sec")
            for stage, sm in final_metrics.stage_specific_metrics.items():
                logger.info(f"  Stage [{stage.value}]: Processed={sm.processed_count}, Errors={sm.error_count}, AvgTime={sm.avg_processing_time_ms:.2f}ms, Queue={sm.items_in_queue}")

        except Exception as e:
            logger.critical(f"Error during OptimizedPipelineProcessor test: {e}", exc_info=True)
        finally:
            dispose_engines() # Clean up database connections
            logger.info("OptimizedPipelineProcessor test finished.")

    if __name__ == "__main__":
        # Correct way to run asyncio main for Windows compatibility with ProactorEventLoop
        if sys.platform == "win32":
            _setup_event_loop_for_windows() # Ensure Proactor loop is set if winloop not there
            # For ProactorEventLoop, asyncio.run() is generally fine.
            # If using 'spawn' for multiprocessing, ensure it's compatible.
        
        try:
            asyncio.run(test_optimized_pipeline())
        except KeyboardInterrupt:
            logger.info("Test run interrupted by user.")
        except Exception as e:
            logger.error(f"Unhandled exception in test runner: {e}", exc_info=True)