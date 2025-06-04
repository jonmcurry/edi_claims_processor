# app/processing/claims_processor.py
"""
Orchestrates the processing of a single claim through validation, ML prediction,
reimbursement calculation, and final disposition.
"""
import time
from decimal import Decimal
from sqlalchemy.orm import Session

from app.utils.logging_config import get_logger, get_correlation_id, set_correlation_id
from app.utils.error_handler import (
    handle_exception, AppException, ValidationError as AppValidationError, MLModelError, DatabaseError
)
from app.database.models.postgres_models import StagingClaim, StagingValidationResult
from app.database.models.sqlserver_models import ProductionClaim, FailedClaimDetail # And related line/diag models
# from app.database.postgres_handler import update_staging_claim_status, save_validation_results
# from app.database.sqlserver_handler import save_to_production, save_to_failed_claims

from app.processing.rules_engine import RulesEngine, ClaimValidationResult
from app.processing.ml_predictor import MLPredictor
from app.processing.reimbursement_calculator import ReimbursementCalculator

logger = get_logger('app.processing.claims_processor')

class ClaimsProcessor:
    """
    Processes a single claim through all defined stages.
    """
    def __init__(self, pg_session: Session, sql_session: Session, config: dict):
        """
        Initializes the ClaimsProcessor with necessary dependencies.
        Args:
            pg_session: SQLAlchemy session for PostgreSQL.
            sql_session: SQLAlchemy session for SQL Server.
            config: Application configuration dictionary.
        """
        self.pg_session = pg_session
        self.sql_session = sql_session
        self.config = config

        # Initialize sub-processors/services
        self.rules_engine = RulesEngine(self.pg_session)
        try:
            self.ml_predictor = MLPredictor() # Uses singleton model loading
        except (MLModelError, ConfigError) as e:
            logger.error(f"Failed to initialize MLPredictor: {e}. ML predictions will be skipped.")
            self.ml_predictor = None # Allow processing to continue without ML if it fails to load

        self.reimbursement_calculator = ReimbursementCalculator() # Loads its own config

    def _map_staging_to_production(self, staging_claim: StagingClaim, validation_status_str: str) -> ProductionClaim:
        """Maps a StagingClaim ORM object to a ProductionClaim ORM object."""
        logger.debug(f"Mapping StagingClaim {staging_claim.claim_id} to ProductionClaim.")
        # This mapping needs to be comprehensive based on your ProductionClaim model
        prod_claim = ProductionClaim(
            ClaimId=staging_claim.claim_id,
            FacilityId=staging_claim.facility_id,
            DepartmentId=staging_claim.department_id, # Ensure this is correctly mapped/validated
            FinancialClassId=staging_claim.financial_class_id,
            PatientId=staging_claim.patient_id,
            PatientAge=staging_claim.patient_age,
            PatientDob=staging_claim.patient_dob,
            PatientSex=staging_claim.patient_sex,
            PatientAccountNumber=staging_claim.patient_account_number,
            ProviderId=staging_claim.provider_id,
            ProviderType=staging_claim.provider_type,
            RenderingProviderNpi=staging_claim.rendering_provider_npi,
            PlaceOfService=staging_claim.place_of_service,
            ServiceDate=staging_claim.service_date,
            TotalChargeAmount=staging_claim.total_charge_amount,
            TotalClaimCharges=staging_claim.total_claim_charges, # Often same as TotalChargeAmount for CMS1500
            PayerName=staging_claim.payer_name,
            PrimaryInsuranceId=staging_claim.primary_insurance_id,
            SecondaryInsuranceId=staging_claim.secondary_insurance_id,
            ProcessingStatus="COMPLETED_VALID", # Or a status indicating it's now in production
            ProcessedDate=datetime.now(), # Or use timestamp from staging if relevant
            ClaimData=staging_claim.claim_data, # Raw claim data (JSONB in PG, NVARCHAR(MAX) in SQL Server)
            ValidationStatus=validation_status_str, # e.g., "VALIDATED_RULES_PASSED", "ML_FILTER_APPLIED"
            ValidationDate=staging_claim.updated_date, # Or now()
            CreatedBy="ClaimsProcessorSystem", # Or more specific user/process
            # UpdatedBy will be set by trigger or on modification
        )

        # Map diagnoses
        for staging_diag in staging_claim.cms1500_diagnoses:
            prod_diag = ProductionCMS1500Diagnosis(
                DiagnosisSequence=staging_diag.diagnosis_sequence,
                IcdCode=staging_diag.icd_code,
                IcdCodeType=staging_diag.icd_code_type,
                DiagnosisDescription=staging_diag.diagnosis_description,
                IsPrimary=staging_diag.is_primary
            )
            prod_claim.cms1500_diagnoses.append(prod_diag)

        # Map line items
        for staging_line in staging_claim.cms1500_line_items:
            prod_line = ProductionCMS1500LineItem(
                LineNumber=staging_line.line_number,
                ServiceDateFrom=staging_line.service_date_from,
                ServiceDateTo=staging_line.service_date_to,
                PlaceOfServiceCode=staging_line.place_of_service_code,
                EmergencyIndicator=staging_line.emergency_indicator,
                CptCode=staging_line.cpt_code,
                Modifier1=staging_line.modifier_1,
                Modifier2=staging_line.modifier_2,
                Modifier3=staging_line.modifier_3,
                Modifier4=staging_line.modifier_4,
                DiagnosisPointers=staging_line.diagnosis_pointers,
                LineChargeAmount=staging_line.line_charge_amount,
                Units=staging_line.units,
                EpsdtFamilyPlan=staging_line.epsdt_family_plan,
                RenderingProviderIdQualifier=staging_line.rendering_provider_id_qualifier,
                RenderingProviderId=staging_line.rendering_provider_id
                # Add estimated reimbursement if that field exists on ProductionCMS1500LineItem
                # EstimatedReimbursementAmount = getattr(staging_line, 'estimated_reimbursement_amount', None)
            )
            prod_claim.cms1500_line_items.append(prod_line)
        
        return prod_claim

    def _save_claim_to_failed_log(self, staging_claim: StagingClaim, validation_result: ClaimValidationResult = None, exception_info: Exception = None):
        """Saves details of a failed claim to the SQL Server FailedClaimDetails table."""
        logger.warning(f"Claim {staging_claim.claim_id} failed processing. Logging to failed claims table.")
        
        error_messages = []
        error_codes_list = []
        processing_stage = "UnknownFailure"

        if validation_result:
            processing_stage = "ValidationRulesEngine"
            for err in validation_result.errors:
                error_messages.append(f"Field: {err['field']}, Rule: {err['rule_id']}, Msg: {err['message']}")
                error_codes_list.append(err['rule_id'])
            # Include warnings too if desired
            # for warn in validation_result.warnings:
            #    error_messages.append(f"WARN: Field: {warn['field']}, Rule: {warn['rule_id']}, Msg: {warn['message']}")


        elif isinstance(exception_info, MLModelError):
            processing_stage = "MLPrediction"
            error_messages.append(exception_info.message)
            error_codes_list.append(exception_info.error_code)
        elif isinstance(exception_info, AppException):
            processing_stage = exception_info.category or "AppError"
            error_messages.append(exception_info.message)
            error_codes_list.append(exception_info.error_code)
        elif exception_info:
            processing_stage = "UnexpectedErrorInProcessing"
            error_messages.append(str(exception_info))
            error_codes_list.append("UNEXPECTED_PROC_ERR")

        failed_claim_entry = FailedClaimDetail(
            OriginalClaimId=staging_claim.claim_id,
            StagingClaimId=staging_claim.claim_id, # Assuming it's from staging
            FacilityId=staging_claim.facility_id,
            PatientAccountNumber=staging_claim.patient_account_number,
            ServiceDate=staging_claim.service_date,
            FailureTimestamp=datetime.now(),
            ProcessingStage=processing_stage,
            ErrorCodes=", ".join(error_codes_list) if error_codes_list else None,
            ErrorMessages="\n".join(error_messages) if error_messages else "No specific error messages captured.",
            ClaimDataSnapshot=str(staging_claim.claim_data) if staging_claim.claim_data else None, # Or a more structured snapshot
            Status="New" # Initial status for UI review
        )
        try:
            self.sql_session.add(failed_claim_entry)
            self.sql_session.commit()
            logger.info(f"Claim {staging_claim.claim_id} logged to FailedClaimDetails table in SQL Server.")
        except Exception as db_exc:
            self.sql_session.rollback()
            logger.error(f"Failed to save claim {staging_claim.claim_id} to FailedClaimDetails: {db_exc}", exc_info=True)
            # This is a critical failure in logging, might need alternative alerting.
            # For now, just log the error.


    def process_claim_by_id(self, claim_id: str):
        """
        Fetches a claim by ID from staging and processes it.
        This is the main orchestration method for a single claim.
        """
        parent_cid = get_correlation_id()
        claim_cid = set_correlation_id(f"{parent_cid}_PROC_{claim_id[:8]}")
        logger.info(f"[{claim_cid}] Starting processing for StagingClaim ID: {claim_id}")
        start_time = time.perf_counter()

        staging_claim: StagingClaim = None
        validation_result_obj: StagingValidationResult = None # To store detailed validation outcomes

        try:
            # 1. Fetch Claim from Staging DB
            staging_claim = self.pg_session.query(StagingClaim).filter_by(claim_id=claim_id).first()
            if not staging_claim:
                logger.error(f"[{claim_cid}] Claim ID {claim_id} not found in staging database.")
                # This might be an error or expected if called with an ID that was already processed/deleted.
                # For now, assume it's an issue if we are trying to process it.
                raise DatabaseError(f"Claim ID {claim_id} not found for processing.")

            # Initialize or fetch existing StagingValidationResult
            validation_result_obj = self.pg_session.query(StagingValidationResult)\
                                        .filter_by(claim_id=claim_id, validation_type="OVERALL_CLAIM_PROCESSING")\
                                        .first()
            if not validation_result_obj:
                validation_result_obj = StagingValidationResult(
                    claim_id=claim_id, 
                    validation_type="OVERALL_CLAIM_PROCESSING", # Or more specific stages
                    validation_status="PENDING"
                )
                self.pg_session.add(validation_result_obj)
                # self.pg_session.flush() # Get ID if needed immediately

            staging_claim.processing_status = "PROCESSING_STARTED"
            self.pg_session.commit() # Commit status update

            # 2. Rules Engine Validation
            logger.debug(f"[{claim_cid}] Applying rules engine validation for claim {claim_id}.")
            rules_validation_output: ClaimValidationResult = self.rules_engine.validate_claim(staging_claim)
            # staging_claim.processing_status is updated by validate_claim
            # staging_claim.validation_errors/warnings are also updated
            
            # Persist rule validation outcome details
            validation_result_obj.validation_status = "PASSED" if rules_validation_output.is_valid else "FAILED"
            validation_result_obj.validation_details = {
                "rules_engine_errors": rules_validation_output.errors,
                "rules_engine_warnings": rules_validation_output.warnings
            }
            # self.pg_session.commit() # Commit after this stage

            if not rules_validation_output.is_valid:
                logger.warning(f"[{claim_cid}] Claim {claim_id} failed rules validation. Errors: {rules_validation_output.errors}")
                staging_claim.processing_status = "VALIDATION_FAILED_RULES"
                self._save_claim_to_failed_log(staging_claim, validation_result=rules_validation_output)
                self.pg_session.commit()
                return # Stop processing this claim

            logger.info(f"[{claim_cid}] Claim {claim_id} PASSED rules validation.")
            staging_claim.processing_status = "VALIDATION_PASSED_RULES" # Intermediate status
            # self.pg_session.commit()


            # 3. ML Filter Prediction (if rules validation passed)
            ml_predicted_filters = None
            ml_prediction_probability = None
            if self.ml_predictor:
                logger.debug(f"[{claim_cid}] Applying ML filter prediction for claim {claim_id}.")
                try:
                    # Convert StagingClaim ORM to dict for ML predictor if it expects a dict
                    # This needs to be carefully crafted to match ML model's expected features
                    claim_data_for_ml = {
                        "claim_id": staging_claim.claim_id,
                        "total_charge_amount": staging_claim.total_charge_amount,
                        "patient_age": staging_claim.patient_age,
                        "diagnoses": [{"icd_code": d.icd_code} for d in staging_claim.cms1500_diagnoses],
                        "line_items": [{"cpt_code": l.cpt_code, "units": l.units} for l in staging_claim.cms1500_line_items]
                        # Add ALL other features the model was trained on
                    }
                    ml_start_time = time.perf_counter()
                    ml_predicted_filters, ml_prediction_probability = self.ml_predictor.predict_filters(claim_data_for_ml)
                    ml_end_time = time.perf_counter()

                    validation_result_obj.predicted_filters = ml_predicted_filters
                    validation_result_obj.ml_inference_time = Decimal(str(ml_end_time - ml_start_time))
                    validation_result_obj.model_version = self.ml_predictor.get_model_version()
                    
                    # Store predicted filters on staging_claim if model has a field for it
                    # staging_claim.ml_predicted_filter_ids = ml_predicted_filters 
                    # staging_claim.ml_confidence_score = ml_prediction_probability
                    logger.info(f"[{claim_cid}] Claim {claim_id} ML Prediction: Filters={ml_predicted_filters}, Prob={ml_prediction_probability:.4f}")
                    staging_claim.processing_status = "ML_PREDICTION_APPLIED"
                except MLModelError as mle:
                    logger.error(f"[{claim_cid}] ML Model Error for claim {claim_id}: {mle.message}. Proceeding without ML prediction.")
                    staging_claim.processing_status = "ML_PREDICTION_FAILED"
                    validation_result_obj.error_message = f"ML Error: {mle.message}"
                    # Optionally, log this to failed claims table as well, or handle as a softer failure
                    # self._save_claim_to_failed_log(staging_claim, exception_info=mle) # If this is a hard stop
                except Exception as e_ml: # Catch any other error during ML
                    logger.error(f"[{claim_cid}] Unexpected error during ML prediction for claim {claim_id}: {e_ml}", exc_info=True)
                    staging_claim.processing_status = "ML_PREDICTION_ERROR"
                    validation_result_obj.error_message = f"Unexpected ML Error: {str(e_ml)}"
            else:
                logger.warning(f"[{claim_cid}] ML Predictor not available. Skipping ML filter prediction for claim {claim_id}.")
                staging_claim.processing_status = "ML_PREDICTION_SKIPPED"
            # self.pg_session.commit()


            # 4. Reimbursement Calculation
            logger.debug(f"[{claim_cid}] Calculating reimbursement for claim {claim_id}.")
            # The method process_claim_reimbursement might update line items in-memory or return a total.
            # For this example, assume it updates a field on claim_orm_object or its lines if they exist.
            # Let's say it just calculates and the result needs to be stored.
            self.reimbursement_calculator.process_claim_reimbursement(staging_claim) # This logs total internally for now
            # If you need to store total reimbursement on StagingClaim:
            # staging_claim.total_estimated_reimbursement = calculated_total_reimbursement
            staging_claim.processing_status = "REIMBURSEMENT_CALCULATED"
            # self.pg_session.commit()


            # 5. Final Disposition: Save to Production or Failed Log
            # If all critical steps passed (especially rules validation)
            if staging_claim.processing_status not in ["VALIDATION_FAILED_RULES", "ERROR_UNKNOWN"]: # Add other fatal statuses
                logger.info(f"[{claim_cid}] Claim {claim_id} processed successfully through pipeline. Preparing for production.")
                
                # Map to ProductionClaim ORM
                production_claim_orm = self._map_staging_to_production(staging_claim, staging_claim.processing_status)
                
                # Save to SQL Server Production DB
                self.sql_session.add(production_claim_orm)
                self.sql_session.commit() # Commit this single claim to production
                logger.info(f"[{claim_cid}] Claim {claim_id} successfully saved to production database.")

                staging_claim.processing_status = "COMPLETED_EXPORTED_TO_PROD"
                staging_claim.exported_to_production = True
                staging_claim.export_date = datetime.now()
            else:
                # Already handled by _save_claim_to_failed_log if rules validation failed.
                # If other steps lead to a "failed" state that wasn't logged yet, log it here.
                if staging_claim.processing_status != "VALIDATION_FAILED_RULES": # Avoid double logging
                     logger.warning(f"[{claim_cid}] Claim {claim_id} did not pass all processing stages. Final status: {staging_claim.processing_status}")
                     # self._save_claim_to_failed_log(staging_claim, exception_info=...) # If a new failure type

            # Commit final staging claim status
            self.pg_session.commit()

        except DatabaseError as dbe: # Catch DB errors from this processor's operations
            logger.error(f"[{claim_cid}] DatabaseError during processing claim {claim_id}: {dbe.message}", exc_info=True)
            if staging_claim: staging_claim.processing_status = "ERROR_DB"
            # self._save_claim_to_failed_log(staging_claim, exception_info=dbe) # Optional: log to failed if appropriate
            if self.pg_session.is_active: self.pg_session.rollback()
            if self.sql_session.is_active: self.sql_session.rollback()
            # Re-raise or handle as per application policy
            raise 
        except AppException as ae: # Catch our own app exceptions
            logger.error(f"[{claim_cid}] AppException during processing claim {claim_id}: {ae.message}", exc_info=True)
            if staging_claim: staging_claim.processing_status = f"ERROR_{ae.error_code[:15]}" # Truncate if too long
            if not (rules_validation_output and not rules_validation_output.is_valid and ae.category == "Validation"): # Avoid double logging validation rule failures
                self._save_claim_to_failed_log(staging_claim, exception_info=ae)
            if self.pg_session.is_active: self.pg_session.rollback()
            if self.sql_session.is_active: self.sql_session.rollback()
            raise
        except Exception as e: # Catch any other unexpected error
            logger.critical(f"[{claim_cid}] Unexpected critical error processing claim {claim_id}: {e}", exc_info=True)
            if staging_claim: staging_claim.processing_status = "ERROR_UNEXPECTED"
            self._save_claim_to_failed_log(staging_claim, exception_info=e)
            if self.pg_session.is_active: self.pg_session.rollback()
            if self.sql_session.is_active: self.sql_session.rollback()
            # Wrap in AppException or handle_exception utility
            handle_exception(e, context=f"ClaimProcessing:{claim_id}") # This will re-raise as AppException
        finally:
            processing_duration = time.perf_counter() - start_time
            if validation_result_obj: # Update overall processing time
                validation_result_obj.processing_time = Decimal(str(processing_duration))
                validation_result_obj.updated_date = datetime.now()
                if staging_claim and staging_claim.processing_status.startswith("COMPLETED"):
                     validation_result_obj.validation_status = "COMPLETED_SUCCESS" # Or more granular
                elif staging_claim and staging_claim.processing_status.startswith("ERROR") or staging_claim.processing_status.endswith("FAILED"):
                     validation_result_obj.validation_status = "COMPLETED_FAILURE"
                try:
                    self.pg_session.commit()
                except Exception as final_commit_exc:
                    logger.error(f"[{claim_cid}] Error committing final validation result for claim {claim_id}: {final_commit_exc}", exc_info=True)
                    self.pg_session.rollback()

            logger.info(f"[{claim_cid}] Finished processing for StagingClaim ID: {claim_id}. Duration: {processing_duration:.4f}s. Final Status: {staging_claim.processing_status if staging_claim else 'N/A'}")
            set_correlation_id(parent_cid) # Restore parent CID


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import init_database_connections, get_postgres_session, get_sqlserver_session, dispose_engines, CONFIG as APP_CONFIG
    from app.database.models.postgres_models import Base as PostgresBase
    from app.database.models.sqlserver_models import Base as SQLServerBase
    from datetime import timedelta

    setup_logging()
    main_cid = set_correlation_id("CLAIMS_PROC_TEST")

    pg_session = None
    sql_session = None

    try:
        init_database_connections()
        pg_session = get_postgres_session()
        sql_session = get_sqlserver_session()

        # --- Setup: Ensure tables and add a dummy staging claim ---
        PostgresBase.metadata.create_all(pg_session.get_bind())
        SQLServerBase.metadata.create_all(sql_session.get_bind()) # Ensure FailedClaimDetails table exists

        # Add dummy facility and financial class if not present (from rules_engine test)
        test_facility = pg_session.query(EDIFacility).filter_by(facility_id="TESTFAC001").first()
        if not test_facility:
            pg_session.add(EDIFacility(facility_id="TESTFAC001", facility_name="Test Hospital", active=True, facility_code="TF01"))
        test_fc = pg_session.query(EDIFinancialClass).filter_by(financial_class_id="TESTFC01", facility_id="TESTFAC001").first()
        if not test_fc:
            pg_session.add(EDIFinancialClass(financial_class_id="TESTFC01", financial_class_description="Test Payer", facility_id="TESTFAC001", active=True))
        pg_session.commit()


        test_claim_id = "PROC_TEST_C001"
        existing_claim = pg_session.query(StagingClaim).filter_by(claim_id=test_claim_id).first()
        if existing_claim: # Clean up if exists from previous run
            pg_session.delete(existing_claim)
            pg_session.commit()

        claim_to_process = StagingClaim(
            claim_id=test_claim_id,
            facility_id="TESTFAC001", # Valid facility
            patient_account_number="PTACC999",
            service_date=datetime.now().date() - timedelta(days=10),
            financial_class_id="TESTFC01", # Valid FC
            patient_dob=(datetime.now() - timedelta(days=365*25)).date(), # 25 years old
            processing_status='PARSED', # Initial status
            total_charge_amount=Decimal("250.50"),
            payer_name="Test Payer Co",
            claim_data={"raw_edi_snippet": "CLM*PROC_TEST_C001*250.50..."}
        )
        # Add a line item for reimbursement calculation
        line1 = StagingCMS1500LineItem(line_number=1, cpt_code="99213", units=1, line_charge_amount=Decimal("120.00"), service_date_from=claim_to_process.service_date)
        diag1 = StagingCMS1500Diagnosis(diagnosis_sequence=1, icd_code="R51", is_primary=True) # Headache
        claim_to_process.cms1500_line_items.append(line1)
        claim_to_process.cms1500_diagnoses.append(diag1)
        
        pg_session.add(claim_to_process)
        pg_session.commit()
        logger.info(f"Dummy claim {test_claim_id} created for processing.")
        # --- End Setup ---

        processor = ClaimsProcessor(pg_session, sql_session, APP_CONFIG)
        processor.process_claim_by_id(test_claim_id)

        # Verify outcome
        processed_staging_claim = pg_session.query(StagingClaim).filter_by(claim_id=test_claim_id).first()
        if processed_staging_claim:
            logger.info(f"Post-processing status of {test_claim_id}: {processed_staging_claim.processing_status}")
            logger.info(f"  Exported: {processed_staging_claim.exported_to_production}")
            
            if processed_staging_claim.exported_to_production:
                prod_claim_check = sql_session.query(ProductionClaim).filter_by(ClaimId=test_claim_id).first()
                if prod_claim_check:
                    logger.info(f"  Claim {test_claim_id} found in Production DB.")
                else:
                    logger.error(f"  Claim {test_claim_id} NOT found in Production DB despite being marked exported.")
            else: # Check if it's in failed log
                failed_log_entry = sql_session.query(FailedClaimDetail).filter_by(OriginalClaimId=test_claim_id).first()
                if failed_log_entry:
                    logger.info(f"  Claim {test_claim_id} found in FailedClaimDetails: Stage={failed_log_entry.ProcessingStage}, Errors='{failed_log_entry.ErrorMessages[:100]}...'")
        else:
            logger.error(f"Could not find staging claim {test_claim_id} after processing attempt.")


    except Exception as e:
        logger.critical(f"Error in Claims Processor test script: {e}", exc_info=True)
        if pg_session and pg_session.is_active: pg_session.rollback()
        if sql_session and sql_session.is_active: sql_session.rollback()
    finally:
        if pg_session: pg_session.close()
        if sql_session: sql_session.close()
        dispose_engines()
        logger.info("Claims Processor test script finished.")
        set_correlation_id(main_cid)