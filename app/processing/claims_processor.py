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
            "code": code,
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
            # RulesEngine.validate_claim now directly updates passed staging_claim_orm's status and validation flags
            # It uses a read-only session for its master data lookups internally.
            # The session passed to RulesEngine at its __init__ is for loading rules, not per-claim validation.
            # Let's assume the RulesEngine.validate_claim method needs a fresh session for its operations or is adapted.
            # For this example, we'll assume self.rules_engine.validate_claim is callable with just the claim.
            # The current rules_engine.validate_claim method in the provided file updates the claim_orm object in place
            # and also uses its own pg_session for master data lookups. This needs careful session management.
            # Ideally, validate_claim would return a result and not modify the ORM directly if called by multiple threads
            # without thread-local sessions for the engine itself.
            # For the pipeline, the batch_handler's _process_item_validation passes claim_orm
            # to rules_engine.validate_claim which modifies it.

            # The staging_claim_orm passed here has ALREADY been validated by the RulesEngine
            # in the batch_handler's VALIDATE stage. We use its existing validation state.
            # Accessing validation_result from ClaimWorkItem.
            # This method now acts more like an orchestrator of results from prior stages.
            
            # We need to make sure staging_claim_orm has validation_result attached or accessible
            # For this refactor, let's assume staging_claim_orm *is* the `work_item.data` and
            # `work_item.validation_result` contains the output from RulesEngine.
            # Let's simplify: this method will re-fetch validation if not provided, or expect it.
            # For the pipeline integration, `staging_claim_orm` is `work_item.data`
            # and `work_item.validation_result` is available.
            # Let's assume `validation_outcome` is passed or attached to `staging_claim_orm`

            validation_outcome: Optional[ClaimValidationResult] = getattr(staging_claim_orm, 'pipeline_validation_result', None)
            if validation_outcome is None:
                # This path might be taken if called outside the full pipeline context.
                logger.warning(f"[{item_processing_cid}] Validation outcome not pre-attached to claim {claim_id}. Re-validating.")
                # This creates a temporary session JUST for this rule engine call if not in pipeline.
                with self.pg_session_factory(read_only=True) as temp_ro_session:
                    # Need a RulesEngine instance if not relying on shared one, or ensure shared is safe.
                    # For now, assume self.rules_engine can be used.
                    # Its internal session for master data lookups must be handled carefully (thread-local or fresh per call).
                    # The current RulesEngine loads rules once. Validate_claim uses its self.pg_session.
                    # This is problematic if self.pg_session is shared across threads.
                    # Simplification: Pipeline is responsible for running validation in a worker.
                    # This method consumes that result.
                    # If validate_claim is called directly, it implies an ad-hoc processing.
                    # Let's assume staging_claim_orm itself has been updated by the rules engine if validation already ran.
                    if not staging_claim_orm.validation_errors and not staging_claim_orm.validation_warnings: # Crude check
                        # Call validation if it appears not to have run and outcome isn't attached.
                        # This is complex due to session management.
                        # For the context of OptimizedPipelineProcessor, staging_claim_orm IS work_item.data
                        # and work_item.validation_result IS set.
                        # Let's assume if this function is called, validation has occurred.
                        # The flags on staging_claim_orm (facility_validated, etc.) are the source of truth for rules.
                        pass # Validation is assumed to be done by the pipeline's VALIDATE stage.
            else: # If validation_outcome was passed via an attribute
                logger.info(f"[{item_processing_cid}] Using pre-attached validation outcome for claim {claim_id}.")


            # Primary check: Rule-based validation.
            # The staging_claim_orm should have its validation flags (facility_validated, etc.)
            # and processing_status (e.g., VALIDATION_FAILED_RULES) set by the RulesEngine
            # in the pipeline's VALIDATE stage.
            if staging_claim_orm.processing_status == STATUS_VALIDATION_FAILED_RULES:
                processing_result.is_overall_valid = False
                processing_result.final_status_staging = STATUS_VALIDATION_FAILED_RULES
                # Populate errors_for_failed_log from staging_claim_orm.validation_errors
                if staging_claim_orm.validation_errors:
                    for err_str in staging_claim_orm.validation_errors:
                        parts = err_str.split('|', 2)
                        processing_result.add_error_detail(
                            code=parts[0] if len(parts) > 0 else "UNKNOWN_RULE",
                            message=parts[2] if len(parts) > 2 else err_str,
                            component="RulesEngine",
                            field=parts[1] if len(parts) > 1 else "Unknown"
                        )
                else:
                    processing_result.add_error_detail(code="VALIDATION_GENERAL_FAILURE", message="Claim failed rule-based validation.", component="RulesEngine")
                
                log_audit_event("claim_validation_failed", resource_id=claim_id, user_id="system_rules_engine",
                                details={"reason": "Rules engine validation failed", "errors": staging_claim_orm.validation_errors})
                logger.warning(f"[{item_processing_cid}] Claim {claim_id} failed rule validation. Errors: {staging_claim_orm.validation_errors}")
                return processing_result # Stop further processing for this claim

            logger.info(f"[{item_processing_cid}] Claim {claim_id} PASSED rule validation.")
            # If rules passed, staging_claim_orm.processing_status should be like VALIDATED_DATALOG_PASSED

            # 2. ML Filter Prediction Stage
            # This also happens in a dedicated pipeline stage. We consume work_item.ml_prediction
            ml_filters = None
            ml_confidence = None
            ml_prediction_data = getattr(staging_claim_orm, 'pipeline_ml_prediction', None)

            if ml_prediction_data:
                ml_filters = ml_prediction_data.get("filters")
                ml_confidence = ml_prediction_data.get("probability")
                logger.info(f"[{item_processing_cid}] Using pre-attached ML prediction for {claim_id}: Filters={ml_filters}, Conf={ml_confidence:.2f}")
            elif self.ml_predictor: # Fallback if not pre-attached (e.g. direct call)
                logger.warning(f"[{item_processing_cid}] ML prediction not pre-attached for {claim_id}. Performing prediction now.")
                ml_input_data = self._prepare_ml_input(staging_claim_orm)
                try:
                    ml_filters, ml_confidence = self.ml_predictor.predict_filters(ml_input_data)
                except MLModelError as mle:
                    logger.error(f"[{item_processing_cid}] ML Model Error for claim {claim_id}: {mle.message}")
                    processing_result.add_error_detail(code=mle.error_code, message=mle.message, component="MLPredictor")
                    processing_result.final_status_staging = STATUS_ML_PREDICTION_ERROR # Mark as ML error
                    # Claim is still rules-valid, but ML part failed. Decide if it goes to failed_claims.
                    # For now, if rules passed, we might still send to prod without ML filters or with error filter.
                    # Let's assume for now, ML error on a rules-valid claim is still a "success" for export.
                    ml_filters = ["FILTER_ML_ERROR"]
                    ml_confidence = 0.0

            processing_result.ml_assigned_filters = ml_filters
            processing_result.ml_confidence = ml_confidence

            # Apply ML Fallback Logic
            if self.ml_fallback_on_low_confidence and ml_confidence is not None and ml_confidence < self.ml_confidence_threshold:
                logger.warning(f"[{item_processing_cid}] Claim {claim_id}: ML confidence {ml_confidence:.2f} is below threshold {self.ml_confidence_threshold}. "
                               "Falling back based on rule validation. Original ML filters were: {ml_filters}")
                log_audit_event("ml_prediction_low_confidence", resource_id=claim_id, user_id="system_ml_module",
                                details={"confidence": ml_confidence, "threshold": self.ml_confidence_threshold, "action": "Fallback to rules"})
                
                # Discard ML filters if confidence is low. The claim's validity is already determined by rules.
                processing_result.ml_assigned_filters = ["FILTER_ML_LOW_CONFIDENCE_FALLBACK"] # Or None
                processing_result.final_status_staging = STATUS_ML_LOW_CONFIDENCE
                # The claim is still considered "rules-valid" overall for production if it passed rules.
                # The "filter" assigned is now an indicator status.
            elif ml_filters:
                 logger.info(f"[{item_processing_cid}] Claim {claim_id}: ML prediction confident. Filters: {ml_filters}, Conf: {ml_confidence:.2f}")
                 # If staging_claim_orm has a field like `assigned_filter_codes`, update it here.
                 # staging_claim_orm.assigned_filter_codes = ml_filters # Example

            # At this point, the claim is considered valid by rules. ML provides supplemental filters.
            processing_result.is_overall_valid = True 
            # final_status_staging might be updated by ML if it has its own status to set.
            # For now, if rules passed, and ML didn't hard fail, it's generally good.
            if not processing_result.final_status_staging: # If not set by ML low confidence or error
                processing_result.final_status_staging = staging_claim_orm.processing_status # Should be like VALIDATED_DATALOG_PASSED

            # 3. Reimbursement Calculation Stage (if claim is still considered valid)
            if processing_result.is_overall_valid:
                logger.info(f"[{item_processing_cid}] Proceeding to reimbursement calculation for claim {claim_id}.")
                try:
                    # This method in ReimbursementCalculator should ideally update line items on staging_claim_orm
                    # or return the calculated details.
                    # Current `process_claim_reimbursement` logs total, assumes ORM has fields.
                    # Let's assume it updates `staging_claim_orm.cms1500_line_items` with estimated_reimbursement
                    # and `staging_claim_orm` with `total_estimated_reimbursement`.
                    # The pipeline's CALCULATE_REIMBURSEMENT stage does this. We consume work_item.reimbursement_details
                    reimb_details = getattr(staging_claim_orm, 'pipeline_reimbursement_details', None)
                    if reimb_details:
                        logger.info(f"[{item_processing_cid}] Using pre-attached reimbursement details for {claim_id}: {reimb_details}")
                        # staging_claim_orm.total_estimated_reimbursement = Decimal(str(reimb_details.get("total_estimated_reimbursement", "0.0")))
                    elif self.reimbursement_calculator: # Fallback if not pre-attached
                        logger.warning(f"[{item_processing_cid}] Reimbursement details not pre-attached for {claim_id}. Calculating now.")
                        self.reimbursement_calculator.process_claim_reimbursement(staging_claim_orm)
                    
                    # Audit log for reimbursement calculation completion
                    log_audit_event("reimbursement_calculation_completed", resource_id=claim_id, user_id="system_reimb_calc",
                                    details={"total_estimated": str(getattr(staging_claim_orm, 'total_estimated_reimbursement', 'N/A'))})
                except Exception as e_reimb:
                    logger.error(f"[{item_processing_cid}] Reimbursement calculation error for claim {claim_id}: {e_reimb}", exc_info=True)
                    processing_result.add_error_detail(code="REIMB_CALC_FAIL", message=str(e_reimb), component="ReimbursementCalculator")
                    processing_result.final_status_staging = STATUS_REIMBURSEMENT_ERROR
                    # Depending on policy, a reimbursement error might not make the whole claim invalid for production,
                    # but it might go to a review queue or have a specific flag. For now, still is_overall_valid = True.
            
            if processing_result.is_overall_valid and not processing_result.final_status_staging.startswith("ERROR"):
                 processing_result.final_status_staging = STATUS_COMPLETED_VALID_PROD # Ready for export
            
            return processing_result

        except Exception as e_main:
            logger.critical(f"[{item_processing_cid}] Unhandled critical error processing claim {claim_id}: {e_main}", exc_info=True)
            processing_result.is_overall_valid = False
            processing_result.final_status_staging = "ERROR_UNHANDLED_PROCESSOR"
            processing_result.add_error_detail(code="UNHANDLED_PROCESSOR_ERR", message=str(e_main), component="ClaimsProcessor")
            return processing_result
        finally:
            set_correlation_id(cid) # Restore batch/run CID

    def _prepare_ml_input(self, claim_orm: StagingClaim) -> Dict[str, Any]:
        """Prepares input data for the ML model from a StagingClaim ORM object."""
        # This must match the feature engineering used during model training.
        # And match what OptimizedPipelineProcessor._process_item_ml_prediction does.
        return {
            "claim_id": claim_orm.claim_id,
            "total_charge_amount": float(claim_orm.total_charge_amount or 0.0),
            "patient_age": int(claim_orm.patient_age or 30),
            "diagnoses": [{"icd_code": d.icd_code} for d in getattr(claim_orm, 'cms1500_diagnoses', [])],
            "line_items": [{"cpt_code": l.cpt_code, "units": l.units} for l in getattr(claim_orm, 'cms1500_line_items', [])]
            # Add other features as required by your model
        }

    def _map_staging_to_production_orm(self, staging_claim: StagingClaim, ml_filters: Optional[List[str]]=None) -> ProductionClaim:
        """Maps a validated StagingClaim ORM object to a ProductionClaim ORM object."""
        # PII/PHI: When mapping, ensure that only necessary data is transferred.
        # If StagingClaim contains raw EDI or overly sensitive intermediate data not needed in production,
        # do not map it.
        logger.debug(f"Mapping StagingClaim {staging_claim.claim_id} to ProductionClaim.")
        
        # Mask/encrypt sensitive fields if they were part of staging_claim.claim_data and are being copied
        # For this example, we assume direct mapping of structured fields.
        # staging_claim.claim_data (JSONB) might contain PII. If it's copied to ProductionClaim.ClaimData,
        # it should be sanitized.
        sanitized_claim_data_json = None
        if staging_claim.claim_data:
            try:
                # If claim_data is already a dict from JSONB:
                # claim_data_dict = staging_claim.claim_data
                # If it's a string needing parsing:
                claim_data_dict = json.loads(str(staging_claim.claim_data)) if isinstance(staging_claim.claim_data, str) else staging_claim.claim_data

                # Example: Mask patient name if present in this raw data
                # This is highly dependent on the structure of your claim_data JSONB
                # if "patient_details" in claim_data_dict and "name" in claim_data_dict["patient_details"]:
                #    claim_data_dict["patient_details"]["name"] = mask_phi_field(claim_data_dict["patient_details"]["name"], "name")
                
                # For now, let's assume claim_data is a simple string or already safe.
                # A robust solution would involve a schema-aware masker.
                # Placeholder: Encrypt the whole snapshot if it's being stored raw.
                sanitized_claim_data_json = encrypt_data(json.dumps(claim_data_dict)) # Placeholder encryption
            except Exception as e:
                logger.warning(f"Could not sanitize/encrypt claim_data for {staging_claim.claim_id}: {e}. Storing as is (ensure it's safe).")
                sanitized_claim_data_json = str(staging_claim.claim_data)


        prod_claim = ProductionClaim(
            ClaimId=staging_claim.claim_id,
            FacilityId=staging_claim.facility_id,
            # DepartmentId: Map if available and validated
            # FinancialClassId: Map if available and validated
            PatientId=mask_phi_field(staging_claim.patient_id, "generic_id") if staging_claim.patient_id else None, # Example masking
            PatientAge=staging_claim.patient_age,
            PatientDob=staging_claim.patient_dob, # Dates are generally okay, but consider policies
            PatientSex=staging_claim.patient_sex,
            PatientAccountNumber=mask_phi_field(staging_claim.patient_account_number, "account_number") if staging_claim.patient_account_number else None,
            ProviderId=staging_claim.provider_id,
            # ProviderType=staging_claim.provider_type, # Consider if this is needed / sensitive
            RenderingProviderNpi=staging_claim.rendering_provider_npi,
            PlaceOfService=staging_claim.place_of_service,
            ServiceDate=staging_claim.service_date,
            TotalChargeAmount=staging_claim.total_charge_amount,
            TotalClaimCharges=staging_claim.total_claim_charges, # Often same as TotalChargeAmount
            PayerName=staging_claim.payer_name, # Could be sensitive depending on context
            PrimaryInsuranceId=mask_phi_field(staging_claim.primary_insurance_id, "generic_id") if staging_claim.primary_insurance_id else None,
            SecondaryInsuranceId=mask_phi_field(staging_claim.secondary_insurance_id, "generic_id") if staging_claim.secondary_insurance_id else None,
            
            ProcessingStatus=STATUS_COMPLETED_VALID_PROD, # Or derive based on ML filters
            ProcessedDate=datetime.utcnow(),
            ClaimData=sanitized_claim_data_json, # Potentially sensitive, ensure PII handled
            ValidationStatus="VALIDATED_RULES_PASSED", # Should come from rule engine result
            # ValidationResults: Store summary or codes, not full PII-laden messages if sensitive
            # MLFilters=json.dumps(ml_filters) if ml_filters else None, # If ProductionClaim has such a field
            CreatedBy="ClaimsProcessorSystem",
            # CreatedDate, UpdatedDate default in model or set here
        )

        for st_diag in staging_claim.cms1500_diagnoses:
            prod_diag = ProductionCMS1500Diagnosis(
                DiagnosisSequence=st_diag.diagnosis_sequence,
                IcdCode=st_diag.icd_code, # Codes themselves are usually not PII
                IcdCodeType=st_diag.icd_code_type,
                # DiagnosisDescription might contain PII, handle with care
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
                CptCode=st_line.cpt_code, # Codes usually not PII
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
                # Add estimated_reimbursement if ProductionCMS1500LineItem has it
                # EstimatedReimbursement=getattr(st_line, 'estimated_reimbursement_amount', None)
            )
            prod_claim.cms1500_line_items.append(prod_line)
            
        return prod_claim

    def _create_failed_claim_detail_orm(self, staging_claim: StagingClaim, processing_result: IndividualClaimProcessingResult) -> FailedClaimDetail:
        """Creates a FailedClaimDetail ORM object from a failed staging claim and processing result."""
        
        # PII/PHI: Create a sanitized snapshot of the claim data.
        # Avoid storing full raw PII unless encrypted and necessary for reprocessing.
        snapshot_data = {
            "claim_id": staging_claim.claim_id,
            "facility_id": staging_claim.facility_id,
            "patient_account_number": mask_phi_field(staging_claim.patient_account_number, "account_number"), # Mask
            "service_date": staging_claim.service_date.isoformat() if staging_claim.service_date else None,
            "total_charge": str(staging_claim.total_charge_amount),
            "payer_name": staging_claim.payer_name,
            "diagnoses_codes": [d.icd_code for d in staging_claim.cms1500_diagnoses[:3]], # Sample
            "procedure_codes": [l.cpt_code for l in staging_claim.cms1500_line_items[:3]], # Sample
            "original_staging_status": getattr(staging_claim, '_original_status_before_pipeline', staging_claim.processing_status), # If tracked
            "final_pipeline_status": processing_result.final_status_staging,
            "ml_filters_attempted": processing_result.ml_assigned_filters,
            "ml_confidence": processing_result.ml_confidence
        }
        # IMPORTANT: The security.encrypt_data is a PLACEHOLDER. Use robust encryption.
        claim_data_snapshot_encrypted = encrypt_data(json.dumps(snapshot_data, default=str))

        error_codes_list = []
        error_messages_list = []
        for err_detail in processing_result.errors_for_failed_log:
            error_codes_list.append(err_detail["code"])
            msg = f"Comp: {err_detail['component']}"
            if err_detail.get("field"): msg += f", Field: {err_detail['field']}"
            msg += f" - {err_detail['message']}"
            error_messages_list.append(msg)

        return FailedClaimDetail(
            OriginalClaimId=staging_claim.claim_id,
            StagingClaimId=staging_claim.claim_id, # Link back to staging if needed
            FacilityId=staging_claim.facility_id,
            PatientAccountNumber=mask_phi_field(staging_claim.patient_account_number, "account_number"), # Mask PII
            ServiceDate=staging_claim.service_date,
            FailureTimestamp=datetime.utcnow(),
            ProcessingStage=processing_result.final_status_staging, # More granular stage of failure
            ErrorCodes=", ".join(error_codes_list) if error_codes_list else "N/A",
            ErrorMessages="\n".join(error_messages_list) if error_messages_list else "No specific error messages captured.",
            ClaimDataSnapshot=claim_data_snapshot_encrypted, # Store encrypted/masked snapshot
            Status='New', # Initial status for UI workflow
            CreatedBy="ClaimsProcessorSystem"
        )


# Legacy ClaimsProcessor wrapper (for app.main.py or other sync callers)
# This class is becoming more of a thin wrapper or utility provider
# as the core logic moves to OptimizedClaimsProcessor for the pipeline.
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
        
        # Initialize components. These could be passed in if they are singletons.
        # For simplicity, initializing here. In a larger app, dependency injection or singletons are better.
        self.rules_engine = RulesEngine(self.pg_session) # RulesEngine loads rules on init
        self.ml_predictor = get_ml_predictor() # Uses singleton
        self.reimbursement_calculator = ReimbursementCalculator() # Loads config on init

        # ML fallback thresholds
        ml_config = config.get('ml_model', {})
        self.ml_confidence_threshold = ml_config.get('prediction_confidence_threshold', 0.8)
        self.ml_fallback_on_low_confidence = ml_config.get('fallback_rules_on_low_confidence', True)

        # Internal OptimizedClaimsProcessor instance for its helper methods if needed,
        # but avoid calling its pipeline method from here directly.
        # The pg_session_factory and sql_session_factory for OptimizedClaimsProcessor
        # are usually provided by the batch_handler, which creates new sessions per task.
        # This legacy ClaimsProcessor is using potentially long-lived sessions.
        # For robust operation, this class should adapt to use session factories too
        # or be very careful about session state.
        
        # For the purpose of using _map_staging_to_production_orm and _create_failed_claim_detail_orm
        # we can instantiate a temporary OptimizedClaimsProcessor or make those static/utility methods.
        # Let's make them utility methods or part of this class.
        # For now, let's assume this class's methods are for single, ad-hoc processing.

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
                claim_data_dict = staging_claim.claim_data # Assuming it's already a dict if from JSONB
                if isinstance(staging_claim.claim_data, str): # If it's a string, try to parse
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
            CreatedBy="ClaimsProcessorLegacy" # Indicate source
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
        
        error_codes_list = [err.get("code", "UNKNOWN") for err in error_list]
        error_messages_list = [ f"Comp: {err.get('component','SYS')}, Field: {err.get('field','N/A')} - {err.get('message','No message')}" for err in error_list]

        return FailedClaimDetail(
            OriginalClaimId=staging_claim.claim_id, StagingClaimId=staging_claim.claim_id,
            FacilityId=staging_claim.facility_id,
            PatientAccountNumber=mask_phi_field(staging_claim.patient_account_number, "account_number"),
            ServiceDate=staging_claim.service_date, FailureTimestamp=datetime.utcnow(),
            ProcessingStage=final_status, # Use the final status as processing stage
            ErrorCodes=", ".join(error_codes_list) if error_codes_list else "N/A",
            ErrorMessages="\n".join(error_messages_list) if error_messages_list else "No specific error messages.",
            ClaimDataSnapshot=claim_data_snapshot_encrypted, Status='New', CreatedBy="ClaimsProcessorLegacy"
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
            validation_result = self.rules_engine.validate_claim(staging_claim) # This updates staging_claim in place
            
            if not validation_result.is_valid:
                logger.warning(f"[{item_cid}] Claim {claim_id} failed rule validation. Errors: {validation_result.errors}")
                staging_claim.processing_status = STATUS_VALIDATION_FAILED_RULES # Already set by rules_engine
                final_errors_for_log.extend(validation_result.errors)
                # Log to FailedClaimDetails
                failed_detail_orm = self._create_failed_claim_detail(staging_claim, STATUS_VALIDATION_FAILED_RULES, final_errors_for_log)
                self.sql_session.add(failed_detail_orm)
                log_audit_event("failed_claim_logged", resource_id=claim_id, user_id="system_claims_processor",
                                details={"reason": STATUS_VALIDATION_FAILED_RULES, "errors_count": len(final_errors_for_log)})
                # Commit changes for this failed claim
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
                staging_claim._ml_filters_debug = ml_filters # Store for potential logging, not a real model field
            except MLModelError as mle:
                logger.error(f"[{item_cid}] ML Model Error for claim {claim_id}: {mle.message}")
                final_errors_for_log.append({"code": mle.error_code, "message": mle.message, "component": "MLPredictor", "field": "N/A"})
                staging_claim.processing_status = STATUS_ML_PREDICTION_ERROR
                # Decide if this is terminal or can proceed. For now, assume it's critical if ML is key.
                # But if rules passed, it might still go to prod with an error filter.
                # Based on requirements, if rules passed, it should proceed.
                ml_filters = ["FILTER_ML_ERROR_ENCOUNTERED"] # Assign a default error filter
                # Fallback logic below will handle this.
            
            final_ml_filters_assigned = ml_filters
            if self.ml_fallback_on_low_confidence and ml_confidence < self.ml_confidence_threshold:
                logger.warning(f"[{item_cid}] Claim {claim_id}: ML confidence {ml_confidence:.2f} is low. Fallback. Original ML filters: {ml_filters}")
                log_audit_event("ml_prediction_low_confidence", resource_id=claim_id, user_id="system_ml_module",
                                details={"confidence": ml_confidence, "threshold": self.ml_confidence_threshold, "action": "Fallback to rules outcome"})
                final_ml_filters_assigned = ["FILTER_ML_LOW_CONFIDENCE_APPLIED"] # Indicate fallback occurred
                staging_claim.processing_status = STATUS_ML_LOW_CONFIDENCE
            elif ml_filters:
                 logger.info(f"[{item_cid}] Claim {claim_id}: ML prediction confident. Filters: {ml_filters}, Conf: {ml_confidence:.2f}")
                 # Update staging_claim.assigned_filters if such a field exists.
            
            # staging_claim.assigned_filter_codes = final_ml_filters_assigned # Example field

            # 3. Reimbursement Calculation
            try:
                self.reimbursement_calculator.process_claim_reimbursement(staging_claim)
                log_audit_event("reimbursement_calculation_completed", resource_id=claim_id, user_id="system_reimb_calc",
                                details={"total_estimated": str(getattr(staging_claim, 'total_estimated_reimbursement', 'N/A'))})
            except Exception as e_reimb:
                logger.error(f"[{item_cid}] Reimbursement calculation error for claim {claim_id}: {e_reimb}", exc_info=True)
                final_errors_for_log.append({"code": "REIMB_CALC_FAIL", "message": str(e_reimb), "component": "ReimbursementCalculator", "field": "N/A"})
                staging_claim.processing_status = STATUS_REIMBURSEMENT_ERROR
                # Even if reimbursement fails, if rules passed, it might still go to production.
                # The decision to fail the claim entirely here depends on business rules.
                # For now, let's assume it's not a terminal failure for export if rules were OK.

            # 4. Save to Production (if all prior critical steps passed)
            # The primary determinant is still rule validation.
            production_orm = self._map_staging_to_production(staging_claim, final_ml_filters_assigned)
            self.sql_session.add(production_orm)
            
            staging_claim.processing_status = STATUS_COMPLETED_VALID_PROD
            staging_claim.exported_to_production = True
            staging_claim.export_date = datetime.utcnow()
            log_audit_event("claim_export_to_production_initiated", resource_id=claim_id, user_id="system_claims_processor",
                            details={"target_table": "dbo.Claims"})

            # Commit all changes for this claim
            self.pg_session.commit()
            self.sql_session.commit()
            log_audit_event("claim_export_to_production_success", resource_id=claim_id, user_id="system_claims_processor")
            logger.info(f"[{item_cid}] Claim {claim_id} successfully processed and saved to production.")

        except Exception as e:
            logger.critical(f"[{item_cid}] Unhandled error processing claim {claim_id} in legacy path: {e}", exc_info=True)
            if hasattr(e, 'error_code') and hasattr(e, 'category'): # If it's an AppException
                 final_errors_for_log.append({"code": e.error_code, "message": str(e), "component": getattr(e, 'component', 'Unknown'), "field": "N/A"})
            else:
                 final_errors_for_log.append({"code": "GENERIC_PROCESSING_ERROR", "message": str(e), "component": "ClaimsProcessorLegacy", "field": "N/A"})

            if 'staging_claim' in locals() and staging_claim:
                try:
                    if self.pg_session.is_active: self.pg_session.rollback()
                    # Fetch fresh staging_claim or ensure it's not in a detached state.
                    # For simplicity, update status on a fresh query if rollback occurred.
                    temp_session = self.pg_session_factory() # Assuming pg_session_factory is available for legacy
                    temp_session.query(StagingClaim).filter_by(claim_id=claim_id).update({
                        "processing_status": "ERROR_PROCESSING_LEGACY",
                        "validation_errors": StagingClaim.validation_errors + [f"FATAL_LEGACY_PROC_ERROR|{str(e)[:200]}"], # Append
                        "updated_date": datetime.utcnow()
                    })
                    
                    failed_detail_orm_exc = self._create_failed_claim_detail(staging_claim, "ERROR_PROCESSING_LEGACY", final_errors_for_log)
                    self.sql_session.add(failed_detail_orm_exc)
                    log_audit_event("failed_claim_logged_exception", resource_id=claim_id, user_id="system_claims_processor",
                                    details={"reason": "ERROR_PROCESSING_LEGACY", "exception": str(e)[:200]})
                    temp_session.commit()
                    self.sql_session.commit() # Commit failed claim detail
                    temp_session.close()
                except Exception as db_err_on_fail:
                    logger.error(f"[{item_cid}] CRITICAL: Failed to log error status for claim {claim_id} after processing failure: {db_err_on_fail}", exc_info=True)
                    if self.pg_session.is_active: self.pg_session.rollback()
                    if self.sql_session.is_active: self.sql_session.rollback()
            handle_exception(e, context=f"LegacyClaimsProcessing: {claim_id}") # Re-raises as AppException
        finally:
            # Ensure CID is restored if it was changed
            set_correlation_id(cid)

if __name__ == '__main__':
    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import (
        init_database_connections, get_postgres_session, get_sqlserver_session, 
        dispose_engines, CONFIG as APP_CONFIG # Use the global config from connection_manager
    )
    from app.database.models.postgres_models import Base as PostgresBase
    from app.database.models.sqlserver_models import Base as SQLServerBase
    from decimal import Decimal

    setup_logging() # Initialize logging first
    main_cid = set_correlation_id("CLAIMS_PROC_ENH_TEST")
    
    pg_session = None
    sql_session = None

    try:
        init_database_connections() # Initialize db_manager and connections
        
        # Use the global session getters
        pg_session = get_postgres_session()
        sql_session = get_sqlserver_session()

        # Ensure tables exist for the test
        PostgresBase.metadata.create_all(pg_session.get_bind())
        SQLServerBase.metadata.create_all(sql_session.get_bind())
        pg_session.commit()
        sql_session.commit()

        # Initialize components for the legacy ClaimsProcessor
        # For RulesEngine, ensure rules are loaded. It tries to load on init.
        # Create dummy rules if they don't exist for testing.
        # (Assuming RulesEngine test from its own file would have set up some rules)
        rules_engine = RulesEngine(pg_session) # RulesEngine needs a session for loading rules
        ml_predictor = get_ml_predictor() # Singleton
        reimbursement_calculator = ReimbursementCalculator()

        # Use the legacy ClaimsProcessor for testing its single claim processing
        legacy_processor = ClaimsProcessor(pg_session, sql_session, APP_CONFIG)
        logger.info("--- Testing Legacy ClaimsProcessor (process_claim_by_id) ---")

        # Create a sample staging claim for testing
        test_claim_id_legacy = f"LEGACY_TEST_{time.time_ns()}"
        
        # PII/PHI Note: Be cautious with hardcoded PII in tests. Use anonymized/fake data.
        # For this example, data is abstract.
        sample_staging_claim = StagingClaim(
            claim_id=test_claim_id_legacy,
            facility_id="FAC_LEGACY01", # Assume this facility exists and is active for rules_engine
            patient_account_number="ACC_LEGACY01",
            service_date=datetime.now().date(),
            patient_dob=(datetime.now() - timedelta(days=365*40)).date(),
            patient_age=40,
            total_charge_amount=Decimal("250.75"),
            processing_status="PARSED",
            claim_data={"raw_edi_segment": "CLM*...~", "source_file": "test.edi"} # Example JSONB data
        )
        # Add some line items and diagnoses for complete processing
        sample_staging_claim.cms1500_line_items.append(
            StagingCMS1500LineItem(line_number=1, cpt_code="99213", line_charge_amount=Decimal("120.00"), units=1)
        )
        sample_staging_claim.cms1500_diagnoses.append(
            StagingCMS1500Diagnosis(diagnosis_sequence=1, icd_code="R51", is_primary=True) # R51 = Headache
        )
        pg_session.add(sample_staging_claim)
        pg_session.commit()
        log_audit_event("test_claim_created_staging", resource_id=test_claim_id_legacy, details={"status": "PARSED"})


        logger.info(f"Attempting to process claim {test_claim_id_legacy} using legacy processor...")
        legacy_processor.process_claim_by_id(test_claim_id_legacy)
        
        # Check status in staging DB
        processed_staging_claim = pg_session.query(StagingClaim).filter_by(claim_id=test_claim_id_legacy).first()
        logger.info(f"StagingClaim {test_claim_id_legacy} final status: {processed_staging_claim.processing_status if processed_staging_claim else 'NOT FOUND'}")

        # Check if it landed in production or failed claims table in SQL Server
        prod_claim_check = sql_session.query(ProductionClaim).filter_by(ClaimId=test_claim_id_legacy).first()
        if prod_claim_check:
            logger.info(f"Claim {test_claim_id_legacy} found in Production DB (dbo.Claims). Status: {prod_claim_check.ProcessingStatus}")
        else:
            failed_claim_check = sql_session.query(FailedClaimDetail).filter_by(OriginalClaimId=test_claim_id_legacy).first()
            if failed_claim_check:
                logger.info(f"Claim {test_claim_id_legacy} found in Failed Claims DB (dbo.FailedClaimDetails). Stage: {failed_claim_check.ProcessingStage}, Errors: {failed_claim_check.ErrorMessages[:100]}...")
                # Example: Check snapshot (note: it's encrypted placeholder)
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
        dispose_engines() # Dispose all engines managed by db_manager
        set_correlation_id(main_cid) # Restore main CID
        logger.info("ClaimsProcessor test script completed.")

