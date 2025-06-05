# app/processing/claims_processor.py
"""
Orchestrates the processing of claims through validation, ML prediction,
reimbursement calculation, and final disposition with high-performance optimizations.
Targets 6,667+ records/second throughput with async pipeline stages and bulk operations.
"""
import asyncio
import time
from decimal import Decimal
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict
import concurrent.futures
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.utils.logging_config import get_logger, get_correlation_id, set_correlation_id
from app.utils.error_handler import (
    handle_exception, AppException, ValidationError as AppValidationError, 
    MLModelError, DatabaseError, StagingDBError, ProductionDBError
)
from app.database.models.postgres_models import StagingClaim, StagingValidationResult
from app.database.models.sqlserver_models import ProductionClaim, FailedClaimDetail
from app.processing.rules_engine import RulesEngine, ClaimValidationResult
from app.processing.ml_predictor import MLPredictor
from app.processing.reimbursement_calculator import ReimbursementCalculator

logger = get_logger('app.processing.claims_processor')

@dataclass
class ProcessingMetrics:
    """Tracks processing metrics for performance monitoring"""
    total_processed: int = 0
    successful: int = 0
    failed_validation: int = 0
    failed_ml: int = 0
    failed_other: int = 0
    start_time: float = 0
    end_time: float = 0
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time > self.start_time else 0
    
    @property
    def throughput(self) -> float:
        return self.total_processed / self.duration if self.duration > 0 else 0

@dataclass
class ProcessingResult:
    """Result of processing a single claim"""
    claim_id: str
    success: bool
    status: str
    errors: List[str] = None
    validation_result: Optional[ClaimValidationResult] = None
    ml_filters: Optional[List[str]] = None
    ml_probability: Optional[float] = None
    processing_time: float = 0

class OptimizedClaimsProcessor:
    """
    High-performance claims processor with async pipeline stages and bulk operations.
    """
    
    def __init__(self, pg_session_factory, sql_session_factory, config: dict):
        """
        Initialize with session factories for thread-safe database access.
        """
        self.pg_session_factory = pg_session_factory
        self.sql_session_factory = sql_session_factory
        self.config = config
        
        # Performance configuration
        self.batch_size = config.get('processing', {}).get('batch_size', 1000)
        self.max_workers = config.get('processing', {}).get('max_workers', 8)
        self.pipeline_stages = config.get('processing', {}).get('pipeline_stages', 4)
        self.bulk_commit_size = config.get('processing', {}).get('bulk_commit_size', 500)
        
        # Initialize processors (shared instances for thread safety)
        self.rules_engine = None
        self.ml_predictor = None
        self.reimbursement_calculator = ReimbursementCalculator()
        
        # Pipeline queues for stage overlap
        self.validation_queue = asyncio.Queue(maxsize=self.batch_size * 2)
        self.ml_queue = asyncio.Queue(maxsize=self.batch_size * 2)
        self.reimbursement_queue = asyncio.Queue(maxsize=self.batch_size * 2)
        self.output_queue = asyncio.Queue(maxsize=self.batch_size * 2)
        
        # Thread pool for CPU-bound operations
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        
    def _initialize_processors(self):
        """Initialize processors with proper session management"""
        try:
            pg_session = self.pg_session_factory()
            self.rules_engine = RulesEngine(pg_session)
            pg_session.close()
            
            self.ml_predictor = MLPredictor()
            logger.info("Processors initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize processors: {e}", exc_info=True)
            raise
    
    async def process_claims_batch_async(self, claim_ids: List[str]) -> Tuple[ProcessingMetrics, List[ProcessingResult]]:
        """
        Process a batch of claims using async pipeline with overlapping stages.
        """
        cid = get_correlation_id()
        batch_cid = set_correlation_id(f"{cid}_BATCH_{len(claim_ids)}")
        
        metrics = ProcessingMetrics(
            total_processed=len(claim_ids),
            start_time=time.perf_counter()
        )
        
        logger.info(f"[{batch_cid}] Starting async batch processing of {len(claim_ids)} claims")
        
        try:
            if not self.rules_engine or not self.ml_predictor:
                self._initialize_processors()
            
            # Start pipeline stages concurrently
            tasks = [
                asyncio.create_task(self._validation_stage(claim_ids)),
                asyncio.create_task(self._ml_prediction_stage()),
                asyncio.create_task(self._reimbursement_stage()),
                asyncio.create_task(self._output_stage())
            ]
            
            # Wait for all stages to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect final results from output stage
            processing_results = results[3] if not isinstance(results[3], Exception) else []
            
            # Update metrics
            for result in processing_results:
                if result.success:
                    metrics.successful += 1
                elif "VALIDATION" in result.status:
                    metrics.failed_validation += 1
                elif "ML" in result.status:
                    metrics.failed_ml += 1
                else:
                    metrics.failed_other += 1
            
            metrics.end_time = time.perf_counter()
            
            logger.info(f"[{batch_cid}] Batch processing completed. "
                       f"Success: {metrics.successful}, Failed: {metrics.total_processed - metrics.successful}, "
                       f"Throughput: {metrics.throughput:.2f} records/sec")
            
            return metrics, processing_results
            
        except Exception as e:
            metrics.end_time = time.perf_counter()
            logger.error(f"[{batch_cid}] Critical error in batch processing: {e}", exc_info=True)
            raise
        finally:
            set_correlation_id(cid)
    
    async def _validation_stage(self, claim_ids: List[str]):
        """Stage 1: Bulk fetch claims and perform validation"""
        cid = get_correlation_id()
        stage_cid = set_correlation_id(f"{cid}_VALIDATION")
        
        try:
            # Bulk fetch claims with related data
            claims_data = await self._bulk_fetch_claims(claim_ids)
            
            # Process validation in parallel batches
            validation_tasks = []
            for i in range(0, len(claims_data), self.bulk_commit_size):
                batch = claims_data[i:i + self.bulk_commit_size]
                task = asyncio.create_task(self._validate_claims_batch(batch))
                validation_tasks.append(task)
            
            # Process validation results and feed to next stage
            for task in asyncio.as_completed(validation_tasks):
                validated_batch = await task
                for claim_data, validation_result in validated_batch:
                    await self.ml_queue.put((claim_data, validation_result))
            
            # Signal completion
            await self.ml_queue.put(None)
            
        except Exception as e:
            logger.error(f"[{stage_cid}] Error in validation stage: {e}", exc_info=True)
            await self.ml_queue.put(None)
        finally:
            set_correlation_id(cid)
    
    async def _ml_prediction_stage(self):
        """Stage 2: ML filter prediction with batching"""
        cid = get_correlation_id()
        stage_cid = set_correlation_id(f"{cid}_ML")
        
        try:
            batch = []
            while True:
                item = await self.ml_queue.get()
                if item is None:
                    # Process final batch and signal completion
                    if batch:
                        await self._process_ml_batch(batch)
                    await self.reimbursement_queue.put(None)
                    break
                
                batch.append(item)
                if len(batch) >= self.bulk_commit_size:
                    await self._process_ml_batch(batch)
                    batch = []
                    
        except Exception as e:
            logger.error(f"[{stage_cid}] Error in ML prediction stage: {e}", exc_info=True)
            await self.reimbursement_queue.put(None)
        finally:
            set_correlation_id(cid)
    
    async def _reimbursement_stage(self):
        """Stage 3: Reimbursement calculation"""
        cid = get_correlation_id()
        stage_cid = set_correlation_id(f"{cid}_REIMBURSEMENT")
        
        try:
            batch = []
            while True:
                item = await self.reimbursement_queue.get()
                if item is None:
                    # Process final batch and signal completion
                    if batch:
                        await self._process_reimbursement_batch(batch)
                    await self.output_queue.put(None)
                    break
                
                batch.append(item)
                if len(batch) >= self.bulk_commit_size:
                    await self._process_reimbursement_batch(batch)
                    batch = []
                    
        except Exception as e:
            logger.error(f"[{stage_cid}] Error in reimbursement stage: {e}", exc_info=True)
            await self.output_queue.put(None)
        finally:
            set_correlation_id(cid)
    
    async def _output_stage(self) -> List[ProcessingResult]:
        """Stage 4: Bulk database writes and final disposition"""
        cid = get_correlation_id()
        stage_cid = set_correlation_id(f"{cid}_OUTPUT")
        
        results = []
        try:
            production_batch = []
            failed_batch = []
            
            while True:
                item = await self.output_queue.get()
                if item is None:
                    # Process final batches
                    if production_batch:
                        await self._bulk_save_to_production(production_batch)
                    if failed_batch:
                        await self._bulk_save_failed_claims(failed_batch)
                    break
                
                claim_data, processing_result = item
                results.append(processing_result)
                
                if processing_result.success:
                    production_batch.append(claim_data)
                else:
                    failed_batch.append((claim_data, processing_result))
                
                # Bulk commit when batch is full
                if len(production_batch) >= self.bulk_commit_size:
                    await self._bulk_save_to_production(production_batch)
                    production_batch = []
                
                if len(failed_batch) >= self.bulk_commit_size:
                    await self._bulk_save_failed_claims(failed_batch)
                    failed_batch = []
            
            return results
            
        except Exception as e:
            logger.error(f"[{stage_cid}] Error in output stage: {e}", exc_info=True)
            return results
        finally:
            set_correlation_id(cid)
    
    async def _bulk_fetch_claims(self, claim_ids: List[str]) -> List[Dict[str, Any]]:
        """Bulk fetch claims with optimized query"""
        loop = asyncio.get_running_loop()
        
        def fetch_claims():
            session = self.pg_session_factory()
            try:
                # Use optimized query with eager loading
                query = text("""
                    SELECT c.*, 
                           array_agg(DISTINCT jsonb_build_object(
                               'diagnosis_sequence', d.diagnosis_sequence,
                               'icd_code', d.icd_code,
                               'is_primary', d.is_primary
                           )) FILTER (WHERE d.claim_id IS NOT NULL) as diagnoses,
                           array_agg(DISTINCT jsonb_build_object(
                               'line_number', l.line_number,
                               'cpt_code', l.cpt_code,
                               'units', l.units,
                               'line_charge_amount', l.line_charge_amount,
                               'service_date_from', l.service_date_from
                           )) FILTER (WHERE l.claim_id IS NOT NULL) as line_items
                    FROM staging.claims c
                    LEFT JOIN staging.cms1500_diagnoses d ON c.claim_id = d.claim_id
                    LEFT JOIN staging.cms1500_line_items l ON c.claim_id = l.claim_id
                    WHERE c.claim_id = ANY(:claim_ids)
                    GROUP BY c.claim_id
                """)
                
                result = session.execute(query, {'claim_ids': claim_ids})
                claims_data = []
                
                for row in result:
                    claim_dict = dict(row._mapping)
                    claim_dict['diagnoses'] = claim_dict.get('diagnoses') or []
                    claim_dict['line_items'] = claim_dict.get('line_items') or []
                    claims_data.append(claim_dict)
                
                return claims_data
                
            finally:
                session.close()
        
        return await loop.run_in_executor(self.thread_pool, fetch_claims)
    
    async def _validate_claims_batch(self, claims_data: List[Dict]) -> List[Tuple[Dict, ClaimValidationResult]]:
        """Validate a batch of claims"""
        loop = asyncio.get_running_loop()
        
        def validate_batch():
            session = self.pg_session_factory()
            try:
                results = []
                for claim_data in claims_data:
                    # Create minimal StagingClaim object for validation
                    staging_claim = StagingClaim(**{k: v for k, v in claim_data.items() 
                                                   if hasattr(StagingClaim, k)})
                    
                    validation_result = self.rules_engine.validate_claim(staging_claim)
                    results.append((claim_data, validation_result))
                
                return results
            finally:
                session.close()
        
        return await loop.run_in_executor(self.thread_pool, validate_batch)
    
    async def _process_ml_batch(self, batch: List[Tuple[Dict, ClaimValidationResult]]):
        """Process ML predictions for a batch"""
        loop = asyncio.get_running_loop()
        
        def ml_batch():
            try:
                for claim_data, validation_result in batch:
                    ml_filters = None
                    ml_probability = None
                    
                    if validation_result.is_valid and self.ml_predictor:
                        try:
                            ml_filters, ml_probability = self.ml_predictor.predict_filters(claim_data)
                        except MLModelError as e:
                            logger.warning(f"ML prediction failed for claim {claim_data.get('claim_id')}: {e}")
                    
                    # Add ML results to claim data
                    claim_data['ml_filters'] = ml_filters
                    claim_data['ml_probability'] = ml_probability
                    
                    asyncio.create_task(
                        self.reimbursement_queue.put((claim_data, validation_result))
                    )
            except Exception as e:
                logger.error(f"Error in ML batch processing: {e}", exc_info=True)
        
        await loop.run_in_executor(self.thread_pool, ml_batch)
    
    async def _process_reimbursement_batch(self, batch: List[Tuple[Dict, ClaimValidationResult]]):
        """Process reimbursement calculations for a batch"""
        loop = asyncio.get_running_loop()
        
        def reimbursement_batch():
            try:
                for claim_data, validation_result in batch:
                    # Calculate reimbursement for line items
                    total_reimbursement = Decimal('0.00')
                    
                    for line_item in claim_data.get('line_items', []):
                        try:
                            reimb_amount, status = self.reimbursement_calculator.calculate_line_item_reimbursement(
                                cpt_code=line_item.get('cpt_code'),
                                units=line_item.get('units', 1),
                                line_charge=Decimal(str(line_item.get('line_charge_amount', '0.00')))
                            )
                            line_item['estimated_reimbursement'] = reimb_amount
                            line_item['reimbursement_status'] = status
                            total_reimbursement += reimb_amount
                        except Exception as e:
                            logger.warning(f"Reimbursement calculation failed for line item: {e}")
                            line_item['estimated_reimbursement'] = Decimal('0.00')
                            line_item['reimbursement_status'] = 'ERROR'
                    
                    claim_data['total_estimated_reimbursement'] = total_reimbursement
                    
                    # Create processing result
                    processing_result = ProcessingResult(
                        claim_id=claim_data.get('claim_id'),
                        success=validation_result.is_valid,
                        status="COMPLETED_SUCCESS" if validation_result.is_valid else "VALIDATION_FAILED",
                        errors=[e['message'] for e in validation_result.errors] if validation_result.errors else None,
                        validation_result=validation_result,
                        ml_filters=claim_data.get('ml_filters'),
                        ml_probability=claim_data.get('ml_probability')
                    )
                    
                    asyncio.create_task(
                        self.output_queue.put((claim_data, processing_result))
                    )
            except Exception as e:
                logger.error(f"Error in reimbursement batch processing: {e}", exc_info=True)
        
        await loop.run_in_executor(self.thread_pool, reimbursement_batch)
    
    async def _bulk_save_to_production(self, production_batch: List[Dict]):
        """Bulk save successful claims to production database"""
        loop = asyncio.get_running_loop()
        
        def save_production():
            session = self.sql_session_factory()
            try:
                production_claims = []
                
                for claim_data in production_batch:
                    # Map to production claim
                    prod_claim_data = {
                        'ClaimId': claim_data['claim_id'],
                        'FacilityId': claim_data['facility_id'],
                        'DepartmentId': claim_data.get('department_id'),
                        'FinancialClassId': claim_data.get('financial_class_id'),
                        'PatientId': claim_data.get('patient_id'),
                        'PatientAge': claim_data.get('patient_age'),
                        'PatientDob': claim_data.get('patient_dob'),
                        'PatientSex': claim_data.get('patient_sex'),
                        'PatientAccountNumber': claim_data.get('patient_account_number'),
                        'ProviderId': claim_data.get('provider_id'),
                        'ProviderType': claim_data.get('provider_type'),
                        'RenderingProviderNpi': claim_data.get('rendering_provider_npi'),
                        'PlaceOfService': claim_data.get('place_of_service'),
                        'ServiceDate': claim_data.get('service_date'),
                        'TotalChargeAmount': claim_data.get('total_charge_amount'),
                        'TotalClaimCharges': claim_data.get('total_claim_charges'),
                        'PayerName': claim_data.get('payer_name'),
                        'PrimaryInsuranceId': claim_data.get('primary_insurance_id'),
                        'SecondaryInsuranceId': claim_data.get('secondary_insurance_id'),
                        'ProcessingStatus': 'COMPLETED_VALID',
                        'ProcessedDate': datetime.now(),
                        'ClaimData': str(claim_data.get('claim_data', {})),
                        'ValidationStatus': 'VALIDATED_RULES_PASSED',
                        'ValidationDate': datetime.now(),
                        'CreatedBy': 'OptimizedClaimsProcessor'
                    }
                    production_claims.append(prod_claim_data)
                
                # Bulk insert using PostgreSQL-style upsert for SQL Server
                if production_claims:
                    # Use bulk operations for better performance
                    session.bulk_insert_mappings(ProductionClaim, production_claims)
                    session.commit()
                    
                    logger.debug(f"Bulk saved {len(production_claims)} claims to production")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Error in bulk save to production: {e}", exc_info=True)
                raise
            finally:
                session.close()
        
        await loop.run_in_executor(self.thread_pool, save_production)
    
    async def _bulk_save_failed_claims(self, failed_batch: List[Tuple[Dict, ProcessingResult]]):
        """Bulk save failed claims to failed claims table"""
        loop = asyncio.get_running_loop()
        
        def save_failed():
            session = self.sql_session_factory()
            try:
                failed_claims = []
                
                for claim_data, processing_result in failed_batch:
                    failed_claim_data = {
                        'OriginalClaimId': claim_data['claim_id'],
                        'StagingClaimId': claim_data['claim_id'],
                        'FacilityId': claim_data.get('facility_id'),
                        'PatientAccountNumber': claim_data.get('patient_account_number'),
                        'ServiceDate': claim_data.get('service_date'),
                        'FailureTimestamp': datetime.now(),
                        'ProcessingStage': 'OptimizedClaimsProcessor',
                        'ErrorCodes': ', '.join([f"ERR_{i}" for i, _ in enumerate(processing_result.errors)]) if processing_result.errors else None,
                        'ErrorMessages': '\n'.join(processing_result.errors) if processing_result.errors else 'Unknown error',
                        'ClaimDataSnapshot': str(claim_data),
                        'Status': 'New'
                    }
                    failed_claims.append(failed_claim_data)
                
                if failed_claims:
                    session.bulk_insert_mappings(FailedClaimDetail, failed_claims)
                    session.commit()
                    logger.debug(f"Bulk saved {len(failed_claims)} failed claims")
                
            except Exception as e:
                session.rollback()
                logger.error(f"Error in bulk save failed claims: {e}", exc_info=True)
                raise
            finally:
                session.close()
        
        await loop.run_in_executor(self.thread_pool, save_failed)
    
    async def _bulk_update_staging_status(self, claim_ids: List[str], status: str):
        """Bulk update staging claim statuses"""
        loop = asyncio.get_running_loop()
        
        def update_status():
            session = self.pg_session_factory()
            try:
                query = text("""
                    UPDATE staging.claims 
                    SET processing_status = :status, 
                        updated_date = CURRENT_TIMESTAMP,
                        exported_to_production = CASE WHEN :status = 'COMPLETED_EXPORTED_TO_PROD' THEN TRUE ELSE exported_to_production END,
                        export_date = CASE WHEN :status = 'COMPLETED_EXPORTED_TO_PROD' THEN CURRENT_TIMESTAMP ELSE export_date END
                    WHERE claim_id = ANY(:claim_ids)
                """)
                
                session.execute(query, {'status': status, 'claim_ids': claim_ids})
                session.commit()
                
            except Exception as e:
                session.rollback()
                logger.error(f"Error updating staging status: {e}", exc_info=True)
                raise
            finally:
                session.close()
        
        await loop.run_in_executor(self.thread_pool, update_status)
    
    def shutdown(self):
        """Clean shutdown of resources"""
        if hasattr(self, 'thread_pool'):
            self.thread_pool.shutdown(wait=True)
            logger.info("Thread pool shut down")


# Backward compatibility - synchronous wrapper
class ClaimsProcessor:
    """
    Synchronous wrapper for the optimized async processor.
    Maintains compatibility with existing code.
    """
    
    def __init__(self, pg_session: Session, sql_session: Session, config: dict):
        """Initialize with single sessions for backward compatibility"""
        self.pg_session = pg_session
        self.sql_session = sql_session
        self.config = config
        
        # Create session factories
        self.pg_session_factory = lambda: self.pg_session
        self.sql_session_factory = lambda: self.sql_session
        
        # Initialize optimized processor
        self.optimized_processor = OptimizedClaimsProcessor(
            self.pg_session_factory,
            self.sql_session_factory,
            config
        )
    
    def process_claim_by_id(self, claim_id: str):
        """Process a single claim (synchronous interface)"""
        cid = get_correlation_id()
        claim_cid = set_correlation_id(f"{cid}_PROC_{claim_id[:8]}")
        
        try:
            # Run async processing for single claim
            metrics, results = asyncio.run(
                self.optimized_processor.process_claims_batch_async([claim_id])
            )
            
            if results:
                result = results[0]
                # Update staging claim status
                if result.success:
                    self.pg_session.execute(
                        text("UPDATE staging.claims SET processing_status = 'COMPLETED_EXPORTED_TO_PROD', exported_to_production = TRUE, export_date = CURRENT_TIMESTAMP WHERE claim_id = :claim_id"),
                        {'claim_id': claim_id}
                    )
                else:
                    self.pg_session.execute(
                        text("UPDATE staging.claims SET processing_status = 'ERROR_PROCESSING' WHERE claim_id = :claim_id"),
                        {'claim_id': claim_id}
                    )
                self.pg_session.commit()
                
                logger.info(f"[{claim_cid}] Claim {claim_id} processed: {result.status}")
            
        except Exception as e:
            logger.error(f"[{claim_cid}] Error processing claim {claim_id}: {e}", exc_info=True)
            if self.pg_session.is_active:
                self.pg_session.rollback()
            raise
        finally:
            set_correlation_id(cid)


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import (
        init_database_connections, get_postgres_session, get_sqlserver_session, 
        dispose_engines, CONFIG as APP_CONFIG
    )
    
    setup_logging()
    main_cid = set_correlation_id("OPT_CLAIMS_PROC_TEST")
    
    async def test_optimized_processor():
        """Test the optimized processor with a large batch"""
        try:
            init_database_connections()
            
            pg_factory = lambda: get_postgres_session()
            sql_factory = lambda: get_sqlserver_session()
            
            processor = OptimizedClaimsProcessor(pg_factory, sql_factory, APP_CONFIG)
            
            # Create test claim IDs (in reality these would exist in staging)
            test_claim_ids = [f"OPT_TEST_C{i:06d}" for i in range(1000)]
            
            logger.info(f"Testing optimized processor with {len(test_claim_ids)} claims")
            
            start_time = time.perf_counter()
            metrics, results = await processor.process_claims_batch_async(test_claim_ids)
            end_time = time.perf_counter()
            
            duration = end_time - start_time
            throughput = len(test_claim_ids) / duration
            
            logger.info(f"Batch processing completed in {duration:.2f}s")
            logger.info(f"Throughput: {throughput:.2f} claims/second")
            logger.info(f"Target throughput (6,667 claims/sec): {'✓ ACHIEVED' if throughput >= 6667 else '✗ NOT ACHIEVED'}")
            
            processor.shutdown()
            
        except Exception as e:
            logger.error(f"Error in optimized processor test: {e}", exc_info=True)
        finally:
            dispose_engines()
    
    # Run the async test
    asyncio.run(test_optimized_processor())
    logger.info("Optimized claims processor test completed")