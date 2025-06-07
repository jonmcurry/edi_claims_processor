# app/processing/claims_processor.py
"""
Orchestrates the processing of claims through validation, ML prediction,
reimbursement calculation, and final disposition with high-performance optimizations.
Targets 6,667+ records/second throughput with async pipeline stages and bulk operations.
Enhanced with audit logging, PII/PHI protection considerations, and refined error categorization.
"""
import time
import json # For serializing claim data snapshot
from decimal import Decimal
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta # Import date and timedelta
from dataclasses import dataclass, field # Retained for internal dataclasses
from sqlalchemy.orm import Session

from app.utils.logging_config import get_logger, get_correlation_id, set_correlation_id, log_audit_event
from app.utils.error_handler import (
    handle_exception, ValidationError as AppValidationError, 
    MLModelError
)
from app.utils.security import encrypt_data, mask_phi_field # Placeholder security functions
from app.database.models.postgres_models import StagingClaim, StagingCMS1500Diagnosis, StagingCMS1500LineItem # For type hints
from app.database.models.sqlserver_models import ProductionClaim, ProductionCMS1500Diagnosis, ProductionCMS1500LineItem, FailedClaimDetail
from app.processing.rules_engine import RulesEngine, ClaimValidationResult
from app.processing.ml_predictor import MLPredictor, get_ml_predictor # Use singleton for MLPredictor
from app.processing.reimbursement_calculator import ReimbursementCalculator

logger = get_logger('app.processing.claims_processor')

# Define constants for processing statuses or load from config if more dynamic
STATUS_VALIDATION_FAILED_RULES = "VALIDATION_FAILED_RULES"
STATUS_ML_LOW_CONFIDENCE = "ML_LOW_CONFIDENCE_FALLBACK"
STATUS_ML_PREDICTION_ERROR = "ML_PREDICTION_ERROR"
STATUS_REIMBURSEMENT_ERROR = "REIMBURSEMENT_CALC_ERROR"
STATUS_EXPORT_TO_PROD_FAILED = "ERROR_EXPORT_TO_PROD"
STATUS_COMPLETED_VALID_PROD = "COMPLETED_EXPORTED_TO_PROD"
STATUS_COMPLETED_FAILED_LOGGED = "COMPLETED_FAILED_LOGGED"


@dataclass
class ProcessingMetrics: # Retained from original, might be used by batch_handler
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
class IndividualClaimProcessingResult: # Renamed from ProcessingResult to avoid conflict
    """Result of processing a single claim within the pipeline context"""
    claim_id: str
    is_overall_valid: bool # True if it should go to production, False if to failed_claims
    final_status_staging: str # The status to update in StagingClaim
    errors_for_failed_log: List[Dict[str, Any]] = field(default_factory=list) # Structured errors
    ml_assigned_filters: Optional[List[str]] = None # Filters assigned by ML if confident
    ml_confidence: Optional[float] = None
    # Add other relevant output data if needed for subsequent stages or logging
    # e.g., total_estimated_reimbursement from claim_data if needed by calling code

    def add_error_detail(self, code: str, message: str, component: str, field: Optional[str]=None):
        self.errors_for_failed_log.append({
            "rule_id": code, # Using rule_id to be consistent with rules_engine
            "message": message,
            "component": component,
            "field": field
        })


class OptimizedClaimsProcessor:
    """
    High-performance claims processor, now part of batch_handler.py's pipeline stages.
    This class will now focus on the logic for a single claim's journey through
    validation, ML, reimbursement, and mapping for export.
    The batching and async orchestration is primarily in OptimizedPipelineProcessor (batch_handler.py).
    """
    
    def __init__(self, pg_session_factory, sql_session_factory, config: dict, 
                 rules_engine: RulesEngine, ml_predictor: MLPredictor, reimbursement_calculator: ReimbursementCalculator):
        """
        Initialize with session factories and pre-initialized components.
        """
        self.pg_session_factory = pg_session_factory
        self.sql_session_factory = sql_session_factory
        self.config = config
        self.ml_config = config.get('ml_model', {})
        
        self.rules_engine = rules_engine
        self.ml_predictor = ml_predictor
        self.reimbursement_calculator = reimbursement_calculator

        # ML fallback thresholds from config
        self.ml_confidence_threshold = self.ml_config.get('prediction_confidence_threshold', 0.8)
        self.ml_fallback_on_low_confidence = self.ml_config.get('fallback_rules_on_low_confidence', True)

        logger.info("OptimizedClaimsProcessor components (RulesEngine, MLPredictor, ReimbursementCalculator) received.")
        logger.info(f"ML Confidence Threshold: {self.ml_confidence_threshold}, Fallback on Low Confidence: {self.ml_fallback_on_low_confidence}")


    def process_single_claim_pipeline_style(self, staging_claim_orm: StagingClaim) -> IndividualClaimProcessingResult:
        """
        Processes a single StagingClaim ORM object through all stages.
        This method is designed to be called by a worker in the OptimizedPipelineProcessor.
        It returns a result object indicating success/failure and details.
        Actual database commits for staging/production/failed tables are handled by the caller/pipeline export stage.
        """
        claim_id = staging_claim_orm.claim_id
        cid = get_correlation_id()
        item_processing_cid = set_correlation_id(f"{cid}_ITEM_{claim_id[:8]}")
        logger.info(f"[{item_processing_cid}] Starting processing for StagingClaim ID: {claim_id}")

        original_staging_status = staging_claim_orm.processing_status
        processing_result = IndividualClaimProcessingResult(
            claim_id=claim_id,
            is_overall_valid=False, # Default to not valid
            final_status_staging="" # Will be set
        )

        try:
            # 1. Validation Stage (Rules Engine)
            # The staging_claim_orm passed here has ALREADY been validated by the RulesEngine
            # in the batch_handler's VALIDATE stage. We use its existing validation state.
            
            validation_outcome: Optional[ClaimValidationResult] = getattr(staging_claim_orm, 'pipeline_validation_result', None)
            if validation_outcome is None:
                logger.warning(f"[{item_processing_cid}] Validation outcome not pre-attached to claim {claim_id}. Re-validating.")
                with self.pg_session_factory(read_only=True) as temp_ro_session:
                    validation_outcome = self.rules_engine.validate_claim(temp_ro_session, staging_claim_orm)
            else: 
                logger.info(f"[{item_processing_cid}] Using pre-attached validation outcome for claim {claim_id}.")

            # Primary check: Rule-based validation.
            if not validation_outcome.is_overall_valid:
                processing_result.is_overall_valid = False
                processing_result.final_status_staging = validation_outcome.final_status_staging
                processing_result.errors_for_failed_log = validation_outcome.errors
                
                log_audit_event("claim_validation_failed", resource_id=claim_id, user_id="system_rules_engine",
                                details={"reason": "Rules engine validation failed", "errors": validation_outcome.errors})
                logger.warning(f"[{item_processing_cid}] Claim {claim_id} failed rule validation. Errors: {validation_outcome.errors}")
                return processing_result

            logger.info(f"[{item_processing_cid}] Claim {claim_id} PASSED rule validation.")
            
            # 2. ML Filter Prediction Stage
            ml_filters = None
            ml_confidence = None
            ml_prediction_data = getattr(staging_claim_orm, 'pipeline_ml_prediction', None)

            if ml_prediction_data:
                ml_filters = ml_prediction_data.get("filters")
                ml_confidence = ml_prediction_data.get("probability")
                logger.info(f"[{item_processing_cid}] Using pre-attached ML prediction for {claim_id}: Filters={ml_filters}, Conf={ml_confidence:.2f}")
            elif self.ml_predictor:
                logger.warning(f"[{item_processing_cid}] ML prediction not pre-attached for {claim_id}. Performing prediction now.")
                ml_input_data = self._prepare_ml_input(staging_claim_orm)
                try:
                    ml_filters, ml_confidence = self.ml_predictor.predict_filters(ml_input_data)
                except MLModelError as mle:
                    logger.error(f"[{item_processing_cid}] ML Model Error for claim {claim_id}: {mle.message}")
                    processing_result.add_error_detail(code=mle.error_code, message=mle.message, component="MLPredictor")
                    processing_result.final_status_staging = STATUS_ML_PREDICTION_ERROR
                    ml_filters = ["FILTER_ML_ERROR"]
                    ml_confidence = 0.0

            processing_result.ml_assigned_filters = ml_filters
            processing_result.ml_confidence = ml_confidence

            if self.ml_fallback_on_low_confidence and ml_confidence is not None and ml_confidence < self.ml_confidence_threshold:
                logger.warning(f"[{item_processing_cid}] Claim {claim_id}: ML confidence {ml_confidence:.2f} is below threshold {self.ml_confidence_threshold}. "
                               "Falling back based on rule validation. Original ML filters were: {ml_filters}")
                log_audit_event("ml_prediction_low_confidence", resource_id=claim_id, user_id="system_ml_module",
                                details={"confidence": ml_confidence, "threshold": self.ml_confidence_threshold, "action": "Fallback to rules"})
                
                processing_result.ml_assigned_filters = ["FILTER_ML_LOW_CONFIDENCE_FALLBACK"]
                processing_result.final_status_staging = STATUS_ML_LOW_CONFIDENCE
            elif ml_filters:
                 logger.info(f"[{item_processing_cid}] Claim {claim_id}: ML prediction confident. Filters: {ml_filters}, Conf: {ml_confidence:.2f}")
            
            processing_result.is_overall_valid = True
            if not processing_result.final_status_staging:
                processing_result.final_status_staging = validation_outcome.final_status_staging

            # 3. Reimbursement Calculation Stage
            if processing_result.is_overall_valid:
                logger.info(f"[{item_processing_cid}] Proceeding to reimbursement calculation for claim {claim_id}.")
                try:
                    reimb_details = getattr(staging_claim_orm, 'pipeline_reimbursement_details', None)
                    if reimb_details:
                        logger.info(f"[{item_processing_cid}] Using pre-attached reimbursement details for {claim_id}: {reimb_details}")
                    elif self.reimbursement_calculator:
                        logger.warning(f"[{item_processing_cid}] Reimbursement details not pre-attached for {claim_id}. Calculating now.")
                        self.reimbursement_calculator.process_claim_reimbursement(staging_claim_orm)
                    
                    log_audit_event("reimbursement_calculation_completed", resource_id=claim_id, user_id="system_reimb_calc",
                                    details={"total_estimated": str(getattr(staging_claim_orm, 'total_estimated_reimbursement', 'N/A'))})
                except Exception as e_reimb:
                    logger.error(f"[{item_processing_cid}] Reimbursement calculation error for claim {claim_id}: {e_reimb}", exc_info=True)
                    processing_result.add_error_detail(code="REIMB_CALC_FAIL", message=str(e_reimb), component="ReimbursementCalculator")
                    processing_result.final_status_staging = STATUS_REIMBURSEMENT_ERROR
            
            if processing_result.is_overall_valid and not processing_result.final_status_staging.startswith("ERROR"):
                 processing_result.final_status_staging = STATUS_COMPLETED_VALID_PROD
            
            return processing_result

        except Exception as e_main:
            logger.critical(f"[{item_processing_cid}] Unhandled critical error processing claim {claim_id}: {e_main}", exc_info=True)
            processing_result.is_overall_valid = False
            processing_result.final_status_staging = "ERROR_UNHANDLED_PROCESSOR"
            processing_result.add_error_detail(code="UNHANDLED_PROCESSOR_ERR", message=str(e_main), component="ClaimsProcessor")
            return processing_result
        finally:
            set_correlation_id(cid)

    def _prepare_ml_input(self, claim_orm: StagingClaim) -> Dict[str, Any]:
        """Prepares input data for the ML model from a StagingClaim ORM object."""
        return {
            "claim_id": claim_orm.claim_id,
            "total_charge_amount": float(claim_orm.total_charge_amount or 0.0),
            "patient_age": int(claim_orm.patient_age or 30),
            "diagnoses": [{"icd_code": d.icd_code} for d in getattr(claim_orm, 'cms1500_diagnoses', [])],
            "line_items": [{"cpt_code": l.cpt_code, "units": l.units} for l in getattr(claim_orm, 'cms1500_line_items', [])]
        }

    def _map_staging_to_production_orm(self, staging_claim: StagingClaim, ml_filters: Optional[List[str]]=None) -> ProductionClaim:
        """Maps a validated StagingClaim ORM object to a ProductionClaim ORM object."""
        logger.debug(f"Mapping StagingClaim {staging_claim.claim_id} to ProductionClaim.")
        
        sanitized_claim_data_json = None
        if staging_claim.claim_data:
            try:
                claim_data_dict = json.loads(str(staging_claim.claim_data)) if isinstance(staging_claim.claim_data, str) else staging_claim.claim_data
                sanitized_claim_data_json = encrypt_data(json.dumps(claim_data_dict))
            except Exception as e:
                logger.warning(f"Could not sanitize/encrypt claim_data for {staging_claim.claim_id}: {e}. Storing as is (ensure it's safe).")
                sanitized_claim_data_json = str(staging_claim.claim_data)

        prod_claim = ProductionClaim(
            ClaimId=staging_claim.claim_id,
            FacilityId=staging_claim.facility_id,
            PatientId=mask_phi_field(staging_claim.patient_id, "generic_id") if staging_claim.patient_id else None,
            PatientAge=staging_claim.patient_age,
            PatientDob=staging_claim.patient_dob,
            PatientSex=staging_claim.patient_sex,
            PatientAccountNumber=mask_phi_field(staging_claim.patient_account_number, "account_number") if staging_claim.patient_account_number else None,
            ProviderId=staging_claim.provider_id,
            RenderingProviderNpi=staging_claim.rendering_provider_npi,
            PlaceOfService=staging_claim.place_of_service,
            ServiceDate=staging_claim.service_date,
            TotalChargeAmount=staging_claim.total_charge_amount,
            TotalClaimCharges=staging_claim.total_claim_charges,
            PayerName=staging_claim.payer_name,
            PrimaryInsuranceId=mask_phi_field(staging_claim.primary_insurance_id, "generic_id") if staging_claim.primary_insurance_id else None,
            SecondaryInsuranceId=mask_phi_field(staging_claim.secondary_insurance_id, "generic_id") if staging_claim.secondary_insurance_id else None,
            ProcessingStatus=STATUS_COMPLETED_VALID_PROD,
            ProcessedDate=datetime.utcnow(),
            ClaimData=sanitized_claim_data_json,
            ValidationStatus="VALIDATED_RULES_PASSED",
            CreatedBy="ClaimsProcessorSystem",
        )

        for st_diag in staging_claim.cms1500_diagnoses:
            prod_diag = ProductionCMS1500Diagnosis(
                DiagnosisSequence=st_diag.diagnosis_sequence,
                IcdCode=st_diag.icd_code,
                IcdCodeType=st_diag.icd_code_type,
                DiagnosisDescription=mask_phi_field(st_diag.diagnosis_description, "text_phi") if st_diag.diagnosis_description else None,
                IsPrimary=st_diag.is_primary
            )
            prod_claim.cms1500_diagnoses.append(prod_diag)

        for st_line in staging_claim.cms1500_line_items:
            prod_line = ProductionCMS1500LineItem(
                LineNumber=st_line.line_number,
                ServiceDateFrom=st_line.service_date_from,
                ServiceDateTo=st_line.service_date_to,
                PlaceOfServiceCode=st_line.place_of_service_code,
                EmergencyIndicator=st_line.emergency_indicator,
                CptCode=st_line.cpt_code,
                Modifier1=st_line.modifier_1,
                Modifier2=st_line.modifier_2,
                Modifier3=st_line.modifier_3,
                Modifier4=st_line.modifier_4,
                DiagnosisPointers=st_line.diagnosis_pointers,
                LineChargeAmount=st_line.line_charge_amount,
                Units=st_line.units,
                EpsdtFamilyPlan=st_line.epsdt_family_plan,
                RenderingProviderIdQualifier=st_line.rendering_provider_id_qualifier,
                RenderingProviderId=st_line.rendering_provider_id
            )
            prod_claim.cms1500_line_items.append(prod_line)
            
        return prod_claim

    def _create_failed_claim_detail_orm(self, staging_claim: StagingClaim, processing_result: IndividualClaimProcessingResult) -> FailedClaimDetail:
        """Creates a FailedClaimDetail ORM object from a failed staging claim and processing result."""
        snapshot_data = {
            "claim_id": staging_claim.claim_id,
            "facility_id": staging_claim.facility_id,
            "patient_account_number": mask_phi_field(staging_claim.patient_account_number, "account_number"),
            "service_date": staging_claim.service_date.isoformat() if staging_claim.service_date else None,
            "total_charge": str(staging_claim.total_charge_amount),
            "payer_name": staging_claim.payer_name,
            "diagnoses_codes": [d.icd_code for d in staging_claim.cms1500_diagnoses[:3]],
            "procedure_codes": [l.cpt_code for l in staging_claim.cms1500_line_items[:3]],
            "original_staging_status": getattr(staging_claim, '_original_status_before_pipeline', staging_claim.processing_status),
            "final_pipeline_status": processing_result.final_status_staging,
            "ml_filters_attempted": processing_result.ml_assigned_filters,
            "ml_confidence": processing_result.ml_confidence
        }
        claim_data_snapshot_encrypted = encrypt_data(json.dumps(snapshot_data, default=str))

        error_codes_list = []
        error_messages_list = []
        for err_detail in processing_result.errors_for_failed_log:
            error_codes_list.append(err_detail.get("rule_id", "UNKNOWN_CODE"))
            msg = f"Comp: {err_detail.get('component', 'SYS')}"
            if err_detail.get("field"): msg += f", Field: {err_detail.get('field')}"
            msg += f" - {err_detail.get('message', 'No message provided')}"
            error_messages_list.append(msg)

        return FailedClaimDetail(
            OriginalClaimId=staging_claim.claim_id,
            StagingClaimId=staging_claim.claim_id,
            FacilityId=staging_claim.facility_id,
            PatientAccountNumber=mask_phi_field(staging_claim.patient_account_number, "account_number"),
            ServiceDate=staging_claim.service_date,
            FailureTimestamp=datetime.utcnow(),
            ProcessingStage=processing_result.final_status_staging,
            ErrorCodes=", ".join(error_codes_list) if error_codes_list else "N/A",
            ErrorMessages="\n".join(error_messages_list) if error_messages_list else "No specific error messages captured.",
            ClaimDataSnapshot=claim_data_snapshot_encrypted,
            Status='New'
        )


class ClaimsProcessor:
    """
    Orchestrates the processing of a single claim.
    This is now more of a utility class or a wrapper for individual processing steps
    if called outside the main async pipeline. The OptimizedPipelineProcessor in batch_handler.py
    handles the high-throughput, pipelined processing.
    """
    
    def __init__(self, pg_session: Session, sql_session: Session, config: dict):
        self.pg_session = pg_session
        self.sql_session = sql_session
        self.config = config
        
        self.rules_engine = RulesEngine(self.pg_session)
        self.ml_predictor = get_ml_predictor()
        self.reimbursement_calculator = ReimbursementCalculator()

        ml_config = config.get('ml_model', {})
        self.ml_confidence_threshold = ml_config.get('prediction_confidence_threshold', 0.8)
        self.ml_fallback_on_low_confidence = ml_config.get('fallback_rules_on_low_confidence', True)

    def _prepare_ml_input(self, claim_orm: StagingClaim) -> Dict[str, Any]:
        """Prepares input data for the ML model from a StagingClaim ORM object."""
        return {
            "claim_id": claim_orm.claim_id,
            "total_charge_amount": float(claim_orm.total_charge_amount or 0.0),
            "patient_age": int(claim_orm.patient_age or 30),
            "diagnoses": [{"icd_code": d.icd_code} for d in getattr(claim_orm, 'cms1500_diagnoses', [])],
            "line_items": [{"cpt_code": l.cpt_code, "units": l.units} for l in getattr(claim_orm, 'cms1500_line_items', [])]
        }

    def _map_staging_to_production(self, staging_claim: StagingClaim, ml_filters: Optional[List[str]]=None) -> ProductionClaim:
        """(Copied from OptimizedClaimsProcessor for utility) Maps StagingClaim to ProductionClaim."""
        logger.debug(f"Mapping StagingClaim {staging_claim.claim_id} to ProductionClaim.")
        sanitized_claim_data_json = None
        if staging_claim.claim_data:
            try:
                claim_data_dict = staging_claim.claim_data
                if isinstance(staging_claim.claim_data, str):
                    claim_data_dict = json.loads(staging_claim.claim_data)
                sanitized_claim_data_json = encrypt_data(json.dumps(claim_data_dict, default=str))
            except Exception as e:
                logger.warning(f"Could not sanitize/encrypt claim_data for {staging_claim.claim_id}: {e}. Storing as string.")
                sanitized_claim_data_json = str(staging_claim.claim_data)

        prod_claim = ProductionClaim(
            ClaimId=staging_claim.claim_id, FacilityId=staging_claim.facility_id,
            PatientId=mask_phi_field(staging_claim.patient_id, "generic_id") if staging_claim.patient_id else None,
            PatientAge=staging_claim.patient_age, PatientDob=staging_claim.patient_dob,
            PatientSex=staging_claim.patient_sex,
            PatientAccountNumber=mask_phi_field(staging_claim.patient_account_number, "account_number") if staging_claim.patient_account_number else None,
            ProviderId=staging_claim.provider_id, RenderingProviderNpi=staging_claim.rendering_provider_npi,
            PlaceOfService=staging_claim.place_of_service, ServiceDate=staging_claim.service_date,
            TotalChargeAmount=staging_claim.total_charge_amount, TotalClaimCharges=staging_claim.total_claim_charges,
            PayerName=staging_claim.payer_name,
            PrimaryInsuranceId=mask_phi_field(staging_claim.primary_insurance_id, "generic_id") if staging_claim.primary_insurance_id else None,
            SecondaryInsuranceId=mask_phi_field(staging_claim.secondary_insurance_id, "generic_id") if staging_claim.secondary_insurance_id else None,
            ProcessingStatus=STATUS_COMPLETED_VALID_PROD, ProcessedDate=datetime.utcnow(),
            ClaimData=sanitized_claim_data_json, ValidationStatus="VALIDATED_RULES_PASSED",
            CreatedBy="ClaimsProcessorLegacy"
        )
        for st_diag in getattr(staging_claim, 'cms1500_diagnoses', []):
            prod_claim.cms1500_diagnoses.append(ProductionCMS1500Diagnosis(
                DiagnosisSequence=st_diag.diagnosis_sequence, IcdCode=st_diag.icd_code,
                IcdCodeType=st_diag.icd_code_type, DiagnosisDescription=mask_phi_field(st_diag.diagnosis_description, "text_phi") if st_diag.diagnosis_description else None,
                IsPrimary=st_diag.is_primary
            ))
        for st_line in getattr(staging_claim, 'cms1500_line_items', []):
            prod_claim.cms1500_line_items.append(ProductionCMS1500LineItem(
                LineNumber=st_line.line_number, ServiceDateFrom=st_line.service_date_from, ServiceDateTo=st_line.service_date_to,
                PlaceOfServiceCode=st_line.place_of_service_code, CptCode=st_line.cpt_code,
                Modifier1=st_line.modifier_1, Modifier2=st_line.modifier_2, Modifier3=st_line.modifier_3, Modifier4=st_line.modifier_4,
                DiagnosisPointers=st_line.diagnosis_pointers, LineChargeAmount=st_line.line_charge_amount, Units=st_line.units
            ))
        return prod_claim

    def _create_failed_claim_detail(self, staging_claim: StagingClaim, final_status: str, error_list: List[Dict]) -> FailedClaimDetail:
        """(Copied from OptimizedClaimsProcessor for utility) Creates FailedClaimDetail ORM."""
        snapshot_data = { "claim_id": staging_claim.claim_id, "facility_id": staging_claim.facility_id,
                          "patient_account_number": mask_phi_field(staging_claim.patient_account_number, "account_number"),
                          "service_date": staging_claim.service_date.isoformat() if staging_claim.service_date else None,
                          "ml_filters_attempted": getattr(staging_claim, '_ml_filters_debug', None) }
        claim_data_snapshot_encrypted = encrypt_data(json.dumps(snapshot_data, default=str))
        
        error_codes_list = [err.get("rule_id", "UNKNOWN_RULE") for err in error_list]
        error_messages_list = [ f"Comp: {err.get('component','SYS')}, Field: {err.get('field','N/A')} - {err.get('message','No message')}" for err in error_list]

        return FailedClaimDetail(
            OriginalClaimId=staging_claim.claim_id, StagingClaimId=staging_claim.claim_id,
            FacilityId=staging_claim.facility_id,
            PatientAccountNumber=mask_phi_field(staging_claim.patient_account_number, "account_number"),
            ServiceDate=staging_claim.service_date, FailureTimestamp=datetime.utcnow(),
            ProcessingStage=final_status,
            ErrorCodes=", ".join(error_codes_list) if error_codes_list else "N/A",
            ErrorMessages="\n".join(error_messages_list) if error_messages_list else "No specific error messages.",
            ClaimDataSnapshot=claim_data_snapshot_encrypted, Status='New'
        )

    def process_claim_by_id(self, claim_id: str):
        """
        Processes a single claim identified by its ID.
        This is a synchronous, ad-hoc processing method.
        """
        cid = get_correlation_id()
        item_cid = set_correlation_id(f"{cid}_LEGACY_PROC_{claim_id[:8]}")
        logger.info(f"[{item_cid}] Starting LEGACY processing for StagingClaim ID: {claim_id}")
        
        current_staging_status = "UNKNOWN_INITIAL_STATUS"
        final_errors_for_log: List[Dict[str, Any]] = []

        try:
            staging_claim = self.pg_session.query(StagingClaim).filter_by(claim_id=claim_id).first()
            if not staging_claim:
                logger.error(f"[{item_cid}] Claim {claim_id} not found in staging database.")
                raise AppValidationError(f"Claim {claim_id} not found.", error_code="CLAIM_NOT_FOUND")

            current_staging_status = staging_claim.processing_status
            logger.info(f"[{item_cid}] Fetched claim {claim_id}, current status: {current_staging_status}")
            log_audit_event("claim_processing_started", resource_id=claim_id, user_id="system_claims_processor",
                            details={"initial_status": current_staging_status, "processor_type": "legacy_sync"})

            # 1. Validation
            validation_result = self.rules_engine.validate_claim(self.pg_session, staging_claim)
            
            if not validation_result.is_overall_valid:
                logger.warning(f"[{item_cid}] Claim {claim_id} failed rule validation. Errors: {validation_result.errors}")
                staging_claim.processing_status = validation_result.final_status_staging
                final_errors_for_log.extend(validation_result.errors)
                failed_detail_orm = self._create_failed_claim_detail(staging_claim, validation_result.final_status_staging, final_errors_for_log)
                self.sql_session.add(failed_detail_orm)
                log_audit_event("failed_claim_logged", resource_id=claim_id, user_id="system_claims_processor",
                                details={"reason": validation_result.final_status_staging, "errors_count": len(final_errors_for_log)})
                self.pg_session.commit()
                self.sql_session.commit()
                return

            logger.info(f"[{item_cid}] Claim {claim_id} PASSED rule validation.")
            
            # 2. ML Prediction
            ml_filters = None
            ml_confidence = 0.0
            ml_input_data = self._prepare_ml_input(staging_claim)
            try:
                ml_filters, ml_confidence = self.ml_predictor.predict_filters(ml_input_data)
                setattr(staging_claim, '_ml_filters_debug', ml_filters)
            except MLModelError as mle:
                logger.error(f"[{item_cid}] ML Model Error for claim {claim_id}: {mle.message}")
                final_errors_for_log.append({"rule_id": mle.error_code, "message": mle.message, "component": "MLPredictor", "field": "N/A"})
                staging_claim.processing_status = STATUS_ML_PREDICTION_ERROR
                ml_filters = ["FILTER_ML_ERROR_ENCOUNTERED"]
            
            final_ml_filters_assigned = ml_filters
            if self.ml_fallback_on_low_confidence and ml_confidence < self.ml_confidence_threshold:
                logger.warning(f"[{item_cid}] Claim {claim_id}: ML confidence {ml_confidence:.2f} is low. Fallback. Original ML filters: {ml_filters}")
                log_audit_event("ml_prediction_low_confidence", resource_id=claim_id, user_id="system_ml_module",
                                details={"confidence": ml_confidence, "threshold": self.ml_confidence_threshold, "action": "Fallback to rules outcome"})
                final_ml_filters_assigned = ["FILTER_ML_LOW_CONFIDENCE_APPLIED"]
                staging_claim.processing_status = STATUS_ML_LOW_CONFIDENCE
            elif ml_filters:
                 logger.info(f"[{item_cid}] Claim {claim_id}: ML prediction confident. Filters: {ml_filters}, Conf: {ml_confidence:.2f}")
            
            # 3. Reimbursement Calculation
            try:
                self.reimbursement_calculator.process_claim_reimbursement(staging_claim)
                log_audit_event("reimbursement_calculation_completed", resource_id=claim_id, user_id="system_reimb_calc",
                                details={"total_estimated": str(getattr(staging_claim, 'total_estimated_reimbursement', 'N/A'))})
            except Exception as e_reimb:
                logger.error(f"[{item_cid}] Reimbursement calculation error for claim {claim_id}: {e_reimb}", exc_info=True)
                final_errors_for_log.append({"rule_id": "REIMB_CALC_FAIL", "message": str(e_reimb), "component": "ReimbursementCalculator", "field": "N/A"})
                staging_claim.processing_status = STATUS_REIMBURSEMENT_ERROR

            # 4. Save to Production
            production_orm = self._map_staging_to_production(staging_claim, final_ml_filters_assigned)
            self.sql_session.add(production_orm)
            
            staging_claim.processing_status = STATUS_COMPLETED_VALID_PROD
            staging_claim.exported_to_production = True
            staging_claim.export_date = datetime.utcnow()
            log_audit_event("claim_export_to_production_initiated", resource_id=claim_id, user_id="system_claims_processor",
                            details={"target_table": "dbo.Claims"})

            self.pg_session.commit()
            self.sql_session.commit()
            log_audit_event("claim_export_to_production_success", resource_id=claim_id, user_id="system_claims_processor")
            logger.info(f"[{item_cid}] Claim {claim_id} successfully processed and saved to production.")

        except Exception as e:
            logger.critical(f"[{item_cid}] Unhandled error processing claim {claim_id} in legacy path: {e}", exc_info=True)
            if hasattr(e, 'error_code') and hasattr(e, 'category'):
                 final_errors_for_log.append({"rule_id": e.error_code, "message": str(e), "component": getattr(e, 'component', 'Unknown'), "field": "N/A"})
            else:
                 final_errors_for_log.append({"rule_id": "GENERIC_PROCESSING_ERROR", "message": str(e), "component": "ClaimsProcessorLegacy", "field": "N/A"})

            if 'staging_claim' in locals() and staging_claim and self.pg_session.is_active:
                self.pg_session.rollback()
                try:
                    # Re-fetch and update in a new transaction
                    fresh_session = self.pg_session
                    fresh_session.query(StagingClaim).filter_by(claim_id=claim_id).update({
                        "processing_status": "ERROR_PROCESSING_LEGACY",
                        "validation_errors": StagingClaim.validation_errors.op('||')(f"FATAL_LEGACY_PROC_ERROR|{str(e)[:200]}"),
                        "updated_date": datetime.utcnow()
                    })
                    failed_detail_orm_exc = self._create_failed_claim_detail(staging_claim, "ERROR_PROCESSING_LEGACY", final_errors_for_log)
                    self.sql_session.add(failed_detail_orm_exc)
                    log_audit_event("failed_claim_logged_exception", resource_id=claim_id, user_id="system_claims_processor",
                                    details={"reason": "ERROR_PROCESSING_LEGACY", "exception": str(e)[:200]})
                    fresh_session.commit()
                    self.sql_session.commit()
                except Exception as db_err_on_fail:
                    logger.error(f"[{item_cid}] CRITICAL: Failed to log error status for claim {claim_id} after processing failure: {db_err_on_fail}", exc_info=True)
                    if self.pg_session.is_active: self.pg_session.rollback()
                    if self.sql_session.is_active: self.sql_session.rollback()
            handle_exception(e, context=f"LegacyClaimsProcessing: {claim_id}")
        finally:
            set_correlation_id(cid)

if __name__ == '__main__':
    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import (
        init_database_connections, get_postgres_session, get_sqlserver_session, 
        dispose_engines, CONFIG as APP_CONFIG
    )
    from app.database.models.postgres_models import Base as PostgresBase
    from app.database.models.sqlserver_models import Base as SQLServerBase
    from decimal import Decimal

    setup_logging()
    main_cid = set_correlation_id("CLAIMS_PROC_ENH_TEST")
    
    pg_session = None
    sql_session = None

    try:
        init_database_connections()
        
        pg_session = get_postgres_session()
        sql_session = get_sqlserver_session()

        PostgresBase.metadata.create_all(pg_session.get_bind())
        SQLServerBase.metadata.create_all(sql_session.get_bind())
        pg_session.commit()
        sql_session.commit()

        legacy_processor = ClaimsProcessor(pg_session, sql_session, APP_CONFIG)
        logger.info("--- Testing Legacy ClaimsProcessor (process_claim_by_id) ---")

        test_claim_id_legacy = f"LEGACY_TEST_{time.time_ns()}"
        
        sample_staging_claim = StagingClaim(
            claim_id=test_claim_id_legacy,
            facility_id="FAC_LEGACY01",
            patient_account_number="ACC_LEGACY01",
            service_date=datetime.now().date(),
            patient_dob=(datetime.now() - timedelta(days=365*40)).date(),
            patient_age=40,
            total_charge_amount=Decimal("250.75"),
            processing_status="PARSED",
            claim_data={"raw_edi_segment": "CLM*...~", "source_file": "test.edi"}
        )
        sample_staging_claim.cms1500_line_items.append(
            StagingCMS1500LineItem(line_number=1, cpt_code="99213", line_charge_amount=Decimal("120.00"), units=1)
        )
        sample_staging_claim.cms1500_diagnoses.append(
            StagingCMS1500Diagnosis(diagnosis_sequence=1, icd_code="R51", is_primary=True)
        )
        pg_session.add(sample_staging_claim)
        pg_session.commit()
        log_audit_event("test_claim_created_staging", resource_id=test_claim_id_legacy, details={"status": "PARSED"})

        logger.info(f"Attempting to process claim {test_claim_id_legacy} using legacy processor...")
        legacy_processor.process_claim_by_id(test_claim_id_legacy)
        
        processed_staging_claim = pg_session.query(StagingClaim).filter_by(claim_id=test_claim_id_legacy).first()
        logger.info(f"StagingClaim {test_claim_id_legacy} final status: {processed_staging_claim.processing_status if processed_staging_claim else 'NOT FOUND'}")

        prod_claim_check = sql_session.query(ProductionClaim).filter_by(ClaimId=test_claim_id_legacy).first()
        if prod_claim_check:
            logger.info(f"Claim {test_claim_id_legacy} found in Production DB (dbo.Claims). Status: {prod_claim_check.ProcessingStatus}")
        else:
            failed_claim_check = sql_session.query(FailedClaimDetail).filter_by(OriginalClaimId=test_claim_id_legacy).first()
            if failed_claim_check:
                logger.info(f"Claim {test_claim_id_legacy} found in Failed Claims DB (dbo.FailedClaimDetails). Stage: {failed_claim_check.ProcessingStage}, Errors: {failed_claim_check.ErrorMessages[:100]}...")
                logger.info(f"  Snapshot (Encrypted Placeholder): {failed_claim_check.ClaimDataSnapshot[:100]}...")
            else:
                logger.warning(f"Claim {test_claim_id_legacy} not found in Production or Failed Claims DB after processing.")
        
        logger.info("--- Legacy ClaimsProcessor test finished ---")

    except Exception as e:
        logger.critical(f"Error during ClaimsProcessor test: {e}", exc_info=True)
        if pg_session and pg_session.is_active: pg_session.rollback()
        if sql_session and sql_session.is_active: sql_session.rollback()
    finally:
        if pg_session: pg_session.close()
        if sql_session: sql_session.close()
        dispose_engines()
        set_correlation_id(main_cid)
        logger.info("ClaimsProcessor test script completed.")
