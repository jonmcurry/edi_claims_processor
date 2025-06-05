# app/processing/batch_handler.py
"""
High-performance batch processing system with pipeline parallelization,
dynamic scaling, backpressure handling, and optimized resource management.
Cross-platform optimized for Windows and Unix systems.
"""
import asyncio
import time
import os
import sys
import multiprocessing
import psutil
from typing import List, Any, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
import threading
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy.orm import Session

from app.utils.logging_config import get_logger, set_correlation_id, get_correlation_id
from app.utils.error_handler import handle_exception, AppException
from app.database.postgres_handler import get_pending_claims_for_processing, update_staging_claim_status
from app.processing.claims_processor import ClaimsProcessor

# Import platform configuration if available
try:
    from app.utils.platform_config import PLATFORM_CONFIG, configure_multiprocessing
    HAS_PLATFORM_CONFIG = True
except ImportError:
    # Fallback configuration
    PLATFORM_CONFIG = {
        "platform": sys.platform,
        "max_workers": min(os.cpu_count() or 4, 8 if sys.platform == "win32" else 16),
        "preferred_executor": "thread" if sys.platform == "win32" else "process",
        "use_multiprocessing": False if sys.platform == "win32" else True,
        "supports_fork": hasattr(os, 'fork')
    }
    HAS_PLATFORM_CONFIG = False

logger = get_logger('app.processing.batch_handler')

class ProcessingStage(Enum):
    """Enumeration of processing pipeline stages"""
    FETCH = "fetch"
    VALIDATE = "validate" 
    ML_PREDICT = "ml_predict"
    CALCULATE = "calculate"
    EXPORT = "export"

@dataclass
class ProcessingMetrics:
    """Tracks processing performance metrics"""
    claims_processed: int = 0
    claims_failed: int = 0
    stage_timings: Dict[ProcessingStage, float] = field(default_factory=dict)
    queue_sizes: Dict[ProcessingStage, int] = field(default_factory=dict)
    throughput_per_second: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    
@dataclass 
class ClaimWorkItem:
    """Work item representing a claim at various processing stages"""
    claim_id: str
    stage: ProcessingStage
    data: Any
    created_at: float = field(default_factory=time.time)
    stage_start_time: float = field(default_factory=time.time)
    retries: int = 0
    max_retries: int = 3

def _configure_multiprocessing_for_platform():
    """Configure multiprocessing based on platform"""
    if sys.platform == "win32":
        # Windows-specific multiprocessing setup
        multiprocessing.freeze_support()
        
        # Set spawn method for Windows compatibility
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            # Already set
            pass
        
        logger.info("Configured multiprocessing for Windows (spawn method)")
    else:
        # Unix systems can use fork efficiently
        try:
            multiprocessing.set_start_method('fork', force=True)
        except RuntimeError:
            # Already set or not available
            pass
        logger.info("Configured multiprocessing for Unix (fork method)")

def _setup_event_loop_for_windows():
    """Setup optimal event loop for Windows"""
    if sys.platform == "win32":
        # Try to use the best available event loop for Windows
        try:
            # Try winloop if available (high-performance Windows event loop)
            import winloop
            asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
            logger.info("Using winloop event loop policy for Windows")
        except ImportError:
            # Fall back to ProactorEventLoop for better I/O performance on Windows
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                logger.info("Using Windows ProactorEventLoop policy")
            except AttributeError:
                # Very old Python versions
                logger.info("Using default Windows event loop")

class PipelineProcessor:
    """
    High-performance pipeline processor with dynamic scaling and backpressure handling.
    Optimized for cross-platform performance with Windows-specific optimizations.
    """
    
    def __init__(self, pg_session_factory, sql_session_factory, config):
        # Configure multiprocessing early
        if HAS_PLATFORM_CONFIG:
            configure_multiprocessing()
        else:
            _configure_multiprocessing_for_platform()
        
        self.pg_session_factory = pg_session_factory
        self.sql_session_factory = sql_session_factory
        self.config = config
        
        # Performance configuration
        self.batch_size = self.config.get('processing', {}).get('batch_size', 100)
        self.max_queue_size = self.config.get('performance', {}).get('max_queue_size', 1000)
        self.target_throughput = self.config.get('performance', {}).get('target_throughput', 6667)  # records/second
        
        # Platform-specific worker configuration
        self._configure_workers()
        
        # Pipeline queues with backpressure
        self.stage_queues: Dict[ProcessingStage, asyncio.Queue] = {}
        self.init_queues()
        
        # Worker pools with platform optimization
        self._init_executors()
        
        # Metrics and monitoring
        self.metrics = ProcessingMetrics()
        self.processing_start_time = time.time()
        self.last_scale_check = time.time()
        self.scale_check_interval = 10.0  # seconds
        
        # Shutdown control
        self.shutdown_event = asyncio.Event()
        self.active_tasks: List[asyncio.Task] = []
        
        # Backpressure thresholds
        self.high_water_mark = int(self.max_queue_size * 0.8)
        self.low_water_mark = int(self.max_queue_size * 0.3)
        
        logger.info(f"PipelineProcessor initialized for {PLATFORM_CONFIG['platform']} with {self.current_workers} workers")
        
    def _configure_workers(self):
        """Configure worker counts based on platform capabilities"""
        cpu_count = os.cpu_count() or 4
        
        if sys.platform == "win32":
            # Windows: More conservative with workers due to overhead
            self.min_workers = max(1, cpu_count // 4)
            self.max_workers = min(cpu_count * 2, 16)  # Cap at 16 for Windows
            self.current_workers = max(2, cpu_count // 2)
            
            # Windows prefers threading for I/O operations
            self.max_io_workers = min(cpu_count * 4, 32)
            self.max_cpu_workers = min(cpu_count, 8)  # Conservative for Windows
            
        else:
            # Unix: Can handle more workers efficiently
            self.min_workers = max(1, cpu_count // 2)
            self.max_workers = min(cpu_count * 4, 50)
            self.current_workers = cpu_count
            
            self.max_io_workers = min(cpu_count * 6, 64)
            self.max_cpu_workers = cpu_count
        
        logger.info(f"Worker configuration - Min: {self.min_workers}, Max: {self.max_workers}, Current: {self.current_workers}")

    def _init_executors(self):
        """Initialize executor pools with platform-specific optimizations"""
        
        # I/O Executor (always use ThreadPoolExecutor for database operations)
        self.io_executor = ThreadPoolExecutor(
            max_workers=self.max_io_workers,
            thread_name_prefix="pipeline_io"
        )
        
        # CPU Executor - platform dependent
        if PLATFORM_CONFIG["use_multiprocessing"] and PLATFORM_CONFIG["supports_fork"]:
            # Use ProcessPoolExecutor for CPU-bound tasks on Unix
            try:
                if sys.platform != "win32":
                    # Unix systems - use fork context
                    ctx = multiprocessing.get_context('fork')
                else:
                    # Windows - use spawn context
                    ctx = multiprocessing.get_context('spawn')
                
                self.cpu_executor = ProcessPoolExecutor(
                    max_workers=self.max_cpu_workers,
                    mp_context=ctx
                )
                logger.info(f"Using ProcessPoolExecutor with {self.max_cpu_workers} workers")
                
            except Exception as e:
                logger.warning(f"Failed to create ProcessPoolExecutor: {e}. Falling back to ThreadPoolExecutor.")
                self.cpu_executor = ThreadPoolExecutor(
                    max_workers=self.max_cpu_workers,
                    thread_name_prefix="pipeline_cpu"
                )
        else:
            # Use ThreadPoolExecutor for CPU tasks (Windows or when multiprocessing is disabled)
            self.cpu_executor = ThreadPoolExecutor(
                max_workers=self.max_cpu_workers,
                thread_name_prefix="pipeline_cpu"
            )
            logger.info(f"Using ThreadPoolExecutor for CPU tasks with {self.max_cpu_workers} workers")

    def init_queues(self):
        """Initialize pipeline stage queues"""
        for stage in ProcessingStage:
            self.stage_queues[stage] = asyncio.Queue(maxsize=self.max_queue_size)
    
    async def start_pipeline(self):
        """Start all pipeline stages"""
        logger.info(f"Starting pipeline processor with {self.current_workers} workers on {PLATFORM_CONFIG['platform']}")
        
        # Start pipeline stage processors
        tasks = [
            asyncio.create_task(self._fetch_stage(), name="fetch_stage"),
            asyncio.create_task(self._validate_stage(), name="validate_stage"), 
            asyncio.create_task(self._ml_predict_stage(), name="ml_predict_stage"),
            asyncio.create_task(self._calculate_stage(), name="calculate_stage"),
            asyncio.create_task(self._export_stage(), name="export_stage"),
            asyncio.create_task(self._monitor_and_scale(), name="monitor_scale"),
            asyncio.create_task(self._metrics_reporter(), name="metrics_reporter")
        ]
        
        self.active_tasks.extend(tasks)
        
        try:
            # Wait for shutdown signal or task completion
            done, pending = await asyncio.wait(
                tasks + [asyncio.create_task(self.shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
                
            await asyncio.gather(*pending, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def _fetch_stage(self):
        """Stage 1: Fetch claims from database"""
        # Platform-optimized fetch concurrency
        concurrent_fetches = 2 if sys.platform == "win32" else 4
        
        while not self.shutdown_event.is_set():
            try:
                # Check backpressure
                if self.stage_queues[ProcessingStage.VALIDATE].qsize() > self.high_water_mark:
                    await asyncio.sleep(0.1)  # Backpressure delay
                    continue
                
                # Fetch claims in parallel
                fetch_tasks = []
                for _ in range(min(concurrent_fetches, self.current_workers)):
                    task = asyncio.create_task(self._fetch_batch())
                    fetch_tasks.append(task)
                
                if fetch_tasks:
                    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"Fetch error: {result}")
                        elif result:  # Claims fetched
                            for claim in result:
                                work_item = ClaimWorkItem(
                                    claim_id=claim.claim_id,
                                    stage=ProcessingStage.VALIDATE,
                                    data=claim
                                )
                                await self.stage_queues[ProcessingStage.VALIDATE].put(work_item)
                
                # If no claims fetched, wait before next attempt
                if not any(isinstance(r, list) and r for r in results if not isinstance(r, Exception)):
                    await asyncio.sleep(1.0)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Fetch stage error: {e}", exc_info=True)
                await asyncio.sleep(1.0)
    
    async def _fetch_batch(self) -> List[Any]:
        """Fetch a batch of claims"""
        loop = asyncio.get_running_loop()
        
        def _sync_fetch():
            pg_session = None
            try:
                pg_session = self.pg_session_factory()
                return get_pending_claims_for_processing(
                    pg_session, 
                    self.batch_size, 
                    status='PARSED'
                )
            except Exception as e:
                logger.error(f"Database fetch error: {e}")
                return []
            finally:
                if pg_session:
                    try:
                        pg_session.close()
                    except Exception as e:
                        logger.warning(f"Error closing session: {e}")
        
        return await loop.run_in_executor(self.io_executor, _sync_fetch)
    
    async def _validate_stage(self):
        """Stage 2: Validate claims using rules engine"""
        workers = []
        # Platform-specific worker count for validation
        validate_workers = self.current_workers if sys.platform != "win32" else min(self.current_workers, 8)
        
        for i in range(validate_workers):
            worker = asyncio.create_task(
                self._stage_worker(
                    ProcessingStage.VALIDATE,
                    ProcessingStage.ML_PREDICT,
                    self._validate_claim
                ),
                name=f"validate_worker_{i}"
            )
            workers.append(worker)
        
        await asyncio.gather(*workers, return_exceptions=True)
    
    async def _ml_predict_stage(self):
        """Stage 3: ML filter prediction"""
        workers = []
        # Use fewer workers for CPU-intensive ML operations, especially on Windows
        ml_workers = min(self.current_workers, self.max_cpu_workers)
        if sys.platform == "win32":
            ml_workers = min(ml_workers, 4)  # Even more conservative on Windows
            
        for i in range(ml_workers):
            worker = asyncio.create_task(
                self._stage_worker(
                    ProcessingStage.ML_PREDICT,
                    ProcessingStage.CALCULATE,
                    self._ml_predict_claim
                ),
                name=f"ml_worker_{i}"
            )
            workers.append(worker)
        
        await asyncio.gather(*workers, return_exceptions=True)
    
    async def _calculate_stage(self):
        """Stage 4: Calculate reimbursement"""
        workers = []
        for i in range(self.current_workers):
            worker = asyncio.create_task(
                self._stage_worker(
                    ProcessingStage.CALCULATE,
                    ProcessingStage.EXPORT,
                    self._calculate_reimbursement
                ),
                name=f"calc_worker_{i}"
            )
            workers.append(worker)
        
        await asyncio.gather(*workers, return_exceptions=True)
    
    async def _export_stage(self):
        """Stage 5: Export to production database"""
        workers = []
        # Fewer workers for database writes to avoid contention
        # More conservative on Windows
        export_workers = min(self.current_workers // 2, 4)
        if sys.platform == "win32":
            export_workers = min(export_workers, 2)
            
        for i in range(max(1, export_workers)):
            worker = asyncio.create_task(
                self._final_stage_worker(
                    ProcessingStage.EXPORT,
                    self._export_claim
                ),
                name=f"export_worker_{i}"
            )
            workers.append(worker)
        
        await asyncio.gather(*workers, return_exceptions=True)
    
    async def _stage_worker(self, input_stage: ProcessingStage, output_stage: ProcessingStage, processor_func):
        """Generic stage worker that processes items from input queue to output queue"""
        while not self.shutdown_event.is_set():
            try:
                # Get work item with timeout
                work_item = await asyncio.wait_for(
                    self.stage_queues[input_stage].get(),
                    timeout=1.0
                )
                
                # Check backpressure on output queue
                if self.stage_queues[output_stage].qsize() > self.high_water_mark:
                    # Put item back and wait
                    await self.stage_queues[input_stage].put(work_item)
                    await asyncio.sleep(0.1)
                    continue
                
                # Process the item
                start_time = time.time()
                try:
                    result = await processor_func(work_item)
                    
                    if result:
                        # Move to next stage
                        work_item.stage = output_stage
                        work_item.stage_start_time = time.time()
                        work_item.data = result
                        await self.stage_queues[output_stage].put(work_item)
                        
                        # Update metrics
                        processing_time = time.time() - start_time
                        self.metrics.stage_timings[input_stage] = processing_time
                        self.metrics.claims_processed += 1
                    else:
                        # Processing failed
                        self.metrics.claims_failed += 1
                        
                except Exception as e:
                    logger.error(f"Processing error in {input_stage.value}: {e}")
                    work_item.retries += 1
                    
                    if work_item.retries <= work_item.max_retries:
                        # Retry with exponential backoff
                        backoff_time = min(0.5 * (2 ** work_item.retries), 10.0)  # Cap at 10 seconds
                        await asyncio.sleep(backoff_time)
                        await self.stage_queues[input_stage].put(work_item)
                    else:
                        # Failed permanently
                        self.metrics.claims_failed += 1
                        await self._handle_failed_claim(work_item, e)
                
            except asyncio.TimeoutError:
                continue  # No work available, continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stage worker error in {input_stage.value}: {e}", exc_info=True)
                await asyncio.sleep(1.0)
    
    async def _final_stage_worker(self, input_stage: ProcessingStage, processor_func):
        """Final stage worker (no output queue)"""
        while not self.shutdown_event.is_set():
            try:
                work_item = await asyncio.wait_for(
                    self.stage_queues[input_stage].get(),
                    timeout=1.0
                )
                
                start_time = time.time()
                try:
                    await processor_func(work_item)
                    
                    # Update metrics
                    processing_time = time.time() - start_time
                    self.metrics.stage_timings[input_stage] = processing_time
                    
                except Exception as e:
                    logger.error(f"Final stage processing error: {e}")
                    work_item.retries += 1
                    
                    if work_item.retries <= work_item.max_retries:
                        backoff_time = min(0.5 * (2 ** work_item.retries), 10.0)
                        await asyncio.sleep(backoff_time)
                        await self.stage_queues[input_stage].put(work_item)
                    else:
                        self.metrics.claims_failed += 1
                        await self._handle_failed_claim(work_item, e)
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Final stage worker error: {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def _validate_claim(self, work_item: ClaimWorkItem) -> Any:
        """Validate claim using rules engine"""
        loop = asyncio.get_running_loop()
        
        def _sync_validate():
            pg_session = None
            sql_session = None
            try:
                pg_session = self.pg_session_factory()
                sql_session = self.sql_session_factory()
                processor = ClaimsProcessor(pg_session, sql_session, self.config)
                
                # Perform only validation step
                validation_result = processor.rules_engine.validate_claim(work_item.data)
                
                if validation_result.is_valid:
                    return work_item.data  # Pass claim to next stage
                else:
                    # Update claim status to failed
                    update_staging_claim_status(
                        pg_session, 
                        work_item.claim_id, 
                        "VALIDATION_FAILED_RULES",
                        [f"{e['rule_id']}|{e['field']}|{e['message']}" for e in validation_result.errors]
                    )
                    pg_session.commit()
                    return None  # Don't pass to next stage
                    
            except Exception as e:
                logger.error(f"Validation error for claim {work_item.claim_id}: {e}")
                return None
            finally:
                if pg_session:
                    try:
                        pg_session.close()
                    except Exception:
                        pass
                if sql_session:
                    try:
                        sql_session.close()
                    except Exception:
                        pass
        
        return await loop.run_in_executor(self.io_executor, _sync_validate)
    
    async def _ml_predict_claim(self, work_item: ClaimWorkItem) -> Any:
        """Run ML prediction on claim"""
        loop = asyncio.get_running_loop()
        
        def _sync_ml_predict():
            try:
                from app.processing.ml_predictor import MLPredictor
                predictor = MLPredictor()
                
                # Convert claim to format expected by ML model
                claim_data = {
                    "claim_id": work_item.data.claim_id,
                    "total_charge_amount": work_item.data.total_charge_amount,
                    "patient_age": work_item.data.patient_age,
                    "diagnoses": [{"icd_code": d.icd_code} for d in work_item.data.cms1500_diagnoses],
                    "line_items": [{"cpt_code": l.cpt_code, "units": l.units} for l in work_item.data.cms1500_line_items]
                }
                
                predicted_filters, probability = predictor.predict_filters(claim_data)
                
                # Store ML results on claim object
                work_item.data.ml_predicted_filters = predicted_filters
                work_item.data.ml_confidence_score = probability
                
                return work_item.data
                
            except Exception as e:
                logger.error(f"ML prediction failed for claim {work_item.claim_id}: {e}")
                # Continue processing without ML prediction
                return work_item.data
        
        # Use CPU executor for ML operations
        return await loop.run_in_executor(self.cpu_executor, _sync_ml_predict)
    
    async def _calculate_reimbursement(self, work_item: ClaimWorkItem) -> Any:
        """Calculate reimbursement for claim"""
        loop = asyncio.get_running_loop()
        
        def _sync_calculate():
            try:
                from app.processing.reimbursement_calculator import ReimbursementCalculator
                calculator = ReimbursementCalculator()
                calculator.process_claim_reimbursement(work_item.data)
                return work_item.data
            except Exception as e:
                logger.error(f"Reimbursement calculation failed for claim {work_item.claim_id}: {e}")
                return work_item.data  # Continue even if calculation fails
        
        return await loop.run_in_executor(self.io_executor, _sync_calculate)
    
    async def _export_claim(self, work_item: ClaimWorkItem):
        """Export claim to production database"""
        loop = asyncio.get_running_loop()
        
        def _sync_export():
            pg_session = None
            sql_session = None
            try:
                pg_session = self.pg_session_factory()
                sql_session = self.sql_session_factory()
                processor = ClaimsProcessor(pg_session, sql_session, self.config)
                
                # Map staging claim to production claim
                production_claim = processor._map_staging_to_production(
                    work_item.data, 
                    "COMPLETED_PIPELINE"
                )
                
                # Save to production
                sql_session.add(production_claim)
                sql_session.commit()
                
                # Update staging claim
                work_item.data.processing_status = "COMPLETED_EXPORTED_TO_PROD"
                work_item.data.exported_to_production = True
                work_item.data.export_date = datetime.now()
                pg_session.commit()
                
            except Exception as e:
                logger.error(f"Export error for claim {work_item.claim_id}: {e}")
                raise
            finally:
                if pg_session:
                    try:
                        pg_session.close()
                    except Exception:
                        pass
                if sql_session:
                    try:
                        sql_session.close()
                    except Exception:
                        pass
        
        await loop.run_in_executor(self.io_executor, _sync_export)
    
    async def _handle_failed_claim(self, work_item: ClaimWorkItem, error: Exception):
        """Handle permanently failed claim"""
        logger.error(f"Claim {work_item.claim_id} failed permanently after {work_item.retries} retries: {error}")
        
        # Update claim status
        loop = asyncio.get_running_loop()
        
        def _sync_update_failed():
            pg_session = None
            try:
                pg_session = self.pg_session_factory()
                update_staging_claim_status(
                    pg_session,
                    work_item.claim_id,
                    f"ERROR_{work_item.stage.value.upper()}",
                    [str(error)]
                )
                pg_session.commit()
            except Exception as e:
                logger.error(f"Failed to update failed claim status: {e}")
            finally:
                if pg_session:
                    try:
                        pg_session.close()
                    except Exception:
                        pass
        
        await loop.run_in_executor(self.io_executor, _sync_update_failed)
    
    async def _monitor_and_scale(self):
        """Monitor performance and dynamically scale workers"""
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(self.scale_check_interval)
                
                current_time = time.time()
                elapsed = current_time - self.processing_start_time
                
                if elapsed > 0:
                    # Calculate current throughput
                    self.metrics.throughput_per_second = self.metrics.claims_processed / elapsed
                    
                    # Get system metrics (with error handling for Windows)
                    try:
                        self.metrics.cpu_usage = psutil.cpu_percent(interval=1)
                        self.metrics.memory_usage = psutil.virtual_memory().percent
                    except Exception as e:
                        logger.warning(f"Failed to get system metrics: {e}")
                        self.metrics.cpu_usage = 0.0
                        self.metrics.memory_usage = 0.0
                    
                    # Update queue sizes
                    for stage in ProcessingStage:
                        self.metrics.queue_sizes[stage] = self.stage_queues[stage].qsize()
                    
                    # Check if scaling is needed (less aggressive on Windows)
                    if sys.platform != "win32":
                        await self._check_and_scale()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitor error: {e}", exc_info=True)
    
    async def _check_and_scale(self):
        """Check if worker scaling is needed based on performance metrics"""
        try:
            # More conservative scaling on Windows
            scale_factor = 0.7 if sys.platform == "win32" else 0.8
            
            # Scale up conditions
            should_scale_up = (
                self.metrics.throughput_per_second < self.target_throughput * scale_factor and
                self.current_workers < self.max_workers and
                self.metrics.cpu_usage < 70 and  # More conservative CPU threshold
                self.metrics.memory_usage < 80 and  # More conservative memory threshold
                any(queue_size > self.high_water_mark for queue_size in self.metrics.queue_sizes.values())
            )
            
            # Scale down conditions  
            should_scale_down = (
                self.metrics.throughput_per_second > self.target_throughput * 1.3 and
                self.current_workers > self.min_workers and
                all(queue_size < self.low_water_mark for queue_size in self.metrics.queue_sizes.values())
            )
            
            if should_scale_up:
                # More conservative scaling increments on Windows
                increment = 1 if sys.platform == "win32" else 2
                new_workers = min(self.max_workers, self.current_workers + increment)
                logger.info(f"Scaling up workers: {self.current_workers} -> {new_workers}")
                self.current_workers = new_workers
                
            elif should_scale_down:
                new_workers = max(self.min_workers, self.current_workers - 1)
                logger.info(f"Scaling down workers: {self.current_workers} -> {new_workers}")
                self.current_workers = new_workers
                
        except Exception as e:
            logger.error(f"Scaling check error: {e}", exc_info=True)
    
    async def _metrics_reporter(self):
        """Report performance metrics periodically"""
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(30.0)  # Report every 30 seconds
                
                elapsed = time.time() - self.processing_start_time
                
                logger.info(
                    f"Pipeline Metrics ({PLATFORM_CONFIG['platform']}) - "
                    f"Processed: {self.metrics.claims_processed}, "
                    f"Failed: {self.metrics.claims_failed}, "
                    f"Throughput: {self.metrics.throughput_per_second:.2f}/sec, "
                    f"Workers: {self.current_workers}, "
                    f"CPU: {self.metrics.cpu_usage:.1f}%, "
                    f"Memory: {self.metrics.memory_usage:.1f}%, "
                    f"Queue sizes: {dict(self.metrics.queue_sizes)}"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Metrics reporter error: {e}", exc_info=True)
    
    async def shutdown(self):
        """Gracefully shutdown the pipeline"""
        logger.info("Initiating pipeline shutdown...")
        self.shutdown_event.set()
        
        # Wait for active tasks to complete (with timeout)
        if self.active_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.active_tasks, return_exceptions=True),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning("Some tasks did not complete within shutdown timeout")
                # Force cancel remaining tasks
                for task in self.active_tasks:
                    if not task.done():
                        task.cancel()
        
        await self.cleanup()
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            # Shutdown executors with platform-specific handling
            if hasattr(self, 'io_executor'):
                self.io_executor.shutdown(wait=False)
                
            if hasattr(self, 'cpu_executor'):
                # ProcessPoolExecutor cleanup can be tricky on Windows
                if isinstance(self.cpu_executor, ProcessPoolExecutor):
                    try:
                        # Try with timeout parameter (Python 3.9+)
                        self.cpu_executor.shutdown(wait=True, timeout=10.0)
                    except (TypeError, AttributeError):
                        # Older Python versions don't support timeout parameter
                        self.cpu_executor.shutdown(wait=False)
                else:
                    self.cpu_executor.shutdown(wait=False)
            
            logger.info("Pipeline cleanup completed")
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}", exc_info=True)


# Legacy BatchProcessor class for backward compatibility
class BatchProcessor:
    """
    Legacy batch processor - use PipelineProcessor for new implementations
    """
    def __init__(self, pg_session_factory, sql_session_factory, config):
        logger.warning("BatchProcessor is deprecated. Use PipelineProcessor for better performance.")
        self.pipeline = PipelineProcessor(pg_session_factory, sql_session_factory, config)
    
    def run_main_batch_processing_loop(self, source_pg_session: Session, batch_id_in_db: int = None):
        """Legacy method that runs the new pipeline"""
        # Setup event loop properly for Windows
        if sys.platform == "win32":
            # Use ProactorEventLoop on Windows for better I/O performance
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            except AttributeError:
                # Fallback for older Python versions
                pass
        
        asyncio.run(self.pipeline.start_pipeline())
    
    def shutdown_executors(self):
        """Legacy shutdown method"""
        asyncio.run(self.pipeline.shutdown())


if __name__ == '__main__':
    # Windows-specific setup
    if sys.platform == "win32":
        multiprocessing.freeze_support()  # Required for Windows
        
    # Setup optimal event loop
    _setup_event_loop_for_windows()
    
    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import init_database_connections, get_postgres_session, get_sqlserver_session, dispose_engines, CONFIG as APP_CONFIG
    from app.database.models.postgres_models import StagingClaim, Base as PostgresBase
    from datetime import datetime, timedelta
    from decimal import Decimal

    setup_logging()
    main_cid = set_correlation_id("PIPELINE_PROCESSOR_TEST")

    async def test_pipeline():
        """Test the new pipeline processor with Windows optimizations"""
        try:
            init_database_connections()
            pg_session_factory = lambda: get_postgres_session()
            sql_session_factory = lambda: get_sqlserver_session()

            # Setup test data
            pg_sess = pg_session_factory()
            PostgresBase.metadata.create_all(pg_sess.get_bind())
            
            # Add test claims if needed
            existing_claims = pg_sess.query(StagingClaim).filter(StagingClaim.processing_status == 'PARSED').count()
            if existing_claims < 10:
                logger.info("Adding test claims for pipeline test...")
                for i in range(10 - existing_claims):
                    claim = StagingClaim(
                        claim_id=f"PIPELINE_TEST_C{i+1:03}",
                        facility_id="TESTFAC001",
                        patient_account_number=f"ACC_PT_{i}",
                        service_date=datetime.now().date(),
                        financial_class_id="TESTFC01",
                        patient_dob=(datetime.now() - timedelta(days=365*30)).date(),
                        processing_status='PARSED',
                        total_charge_amount=Decimal("150.00") * (i+1),
                        patient_age=25 + (i * 5)
                    )
                    pg_sess.add(claim)
                pg_sess.commit()
            
            pg_sess.close()
            
            # Start pipeline
            pipeline = PipelineProcessor(pg_session_factory, sql_session_factory, APP_CONFIG)
            
            logger.info(f"Starting high-performance pipeline processor on {PLATFORM_CONFIG['platform']}...")
            
            # Run pipeline for a limited time in test
            pipeline_task = asyncio.create_task(pipeline.start_pipeline())
            
            try:
                # Let it run for 30 seconds in test
                await asyncio.wait_for(pipeline_task, timeout=30.0)
            except asyncio.TimeoutError:
                logger.info("Test timeout reached, shutting down pipeline...")
                await pipeline.shutdown()
            
            # Report final metrics
            logger.info(
                f"Pipeline test completed on {PLATFORM_CONFIG['platform']} - "
                f"Processed: {pipeline.metrics.claims_processed}, "
                f"Failed: {pipeline.metrics.claims_failed}, "
                f"Final throughput: {pipeline.metrics.throughput_per_second:.2f}/sec, "
                f"Worker config: {pipeline.current_workers}/{pipeline.max_workers} workers, "
                f"Executor types: IO={type(pipeline.io_executor).__name__}, CPU={type(pipeline.cpu_executor).__name__}"
            )

        except Exception as e:
            logger.critical(f"Pipeline test error: {e}", exc_info=True)
        finally:
            dispose_engines()
            logger.info("Pipeline processor test finished.")

    # Run the async test with proper Windows handling
    try:
        asyncio.run(test_pipeline())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test execution error: {e}", exc_info=True)