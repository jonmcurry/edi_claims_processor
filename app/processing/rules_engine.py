# app/processing/rules_engine.py
"""
Implements business logic for claim validation using a combination of direct Python checks
and Datalog rules fetched from the 'edi.filters' table.
"""
import time # For performance timing
from datetime import date # Ensure datetime is imported if used, though not directly in this snippet
from decimal import Decimal
from typing import List, Dict, Any, Set, Optional # Added Optional
import threading # For _datalog_engine_lock

from sqlalchemy.orm import Session
from pyDatalog import pyDatalog # Ensure pyDatalog is installed

from sqlalchemy.exc import SQLAlchemyError

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import ValidationError as AppValidationError, StagingDBError
from app.database.models.postgres_models import (
    StagingClaim,
    Filter as EDIFilter, 
    Facility as EDIFacility,
    FinancialClass as EDIFinancialClass
)
# Assuming this function exists in postgres_handler as previously discussed
from app.database.postgres_handler import perform_direct_staging_claim_validations 

logger = get_logger('app.processing.rules_engine')

# Status constants for internal use and potential logging/reporting
STATUS_PYTHON_VALIDATION_FAILED = "VALIDATION_PYTHON_FAILED"
STATUS_DATALOG_VALIDATION_FAILED = "VALIDATION_DATALOG_FAILED"
STATUS_VALIDATION_FAILED_RULES = "VALIDATION_FAILED_RULES"  # Added missing constant
STATUS_VALIDATION_PASSED = "VALIDATION_PASSED" # Overall pass
STATUS_ERROR_RULES_ENGINE_SETUP = "ERROR_RULES_ENGINE_SETUP"
STATUS_ERROR_RULES_PYTHON_VALIDATION = "ERROR_RULES_PYTHON_VALIDATION"
STATUS_ERROR_RULES_DATALOG = "ERROR_RULES_DATALOG"


class ClaimValidationResult:
    """Holds the result of a claim validation, combining Python and Datalog checks."""
    def __init__(self, claim_id: str):
        self.claim_id = claim_id
        self.is_valid_python: bool = True # Validity based on direct Python checks
        self.is_valid_datalog: bool = True # Validity based on Datalog rules
        self.errors: List[Dict[str, Any]] = [] # Combined errors
        self.warnings: List[Dict[str, Any]] = [] # Combined warnings
        self.datalog_rule_outcomes: Dict[str, Any] = {} # Raw outcomes from Datalog if needed

    @property
    def is_overall_valid(self) -> bool:
        """Determines overall validity based on all checks."""
        return self.is_valid_python and self.is_valid_datalog

    def add_error(self, rule_id: str, field: str, message: str, source: str = "DATALOG", details: Optional[Dict] = None):
        """Adds an error to the result."""
        if source.upper() == "PYTHON":
            self.is_valid_python = False
        elif source.upper() == "DATALOG":
            self.is_valid_datalog = False
        
        error_entry = {
            "rule_id": rule_id, 
            "field": field, 
            "message": message, 
            "source": source.upper(), # 'PYTHON' or 'DATALOG'
            "details": details or {}
        }
        # Avoid duplicate error entries from the same rule/source/field for the same message
        if not any(e['rule_id'] == rule_id and e['field'] == field and e['message'] == message and e['source'] == source.upper() for e in self.errors):
            self.errors.append(error_entry)
        logger.warning(f"Validation Error for Claim {self.claim_id}, Source: {source}, Rule: {rule_id}, Field: {field}: {message}")

    def add_warning(self, rule_id: str, field: str, message: str, source: str = "DATALOG", details: Optional[Dict] = None):
        """Adds a warning to the result."""
        warning_entry = {
            "rule_id": rule_id, 
            "field": field, 
            "message": message, 
            "source": source.upper(),
            "details": details or {}
        }
        if not any(w['rule_id'] == rule_id and w['field'] == field and w['message'] == message and w['source'] == source.upper() for w in self.warnings):
            self.warnings.append(warning_entry)
        logger.info(f"Validation Warning for Claim {self.claim_id}, Source: {source}, Rule: {rule_id}, Field: {field}: {message}")

    def update_from_python_validation(self, python_valid_passed: bool, claim_with_python_errors: StagingClaim):
        """Updates result based on direct Python validation outcomes."""
        self.is_valid_python = python_valid_passed
        if not python_valid_passed:
            # Assuming validation_errors on claim_with_python_errors are strings like "RULE|FIELD|MESSAGE"
            for err_str in getattr(claim_with_python_errors, 'validation_errors', []):
                parts = err_str.split('|', 2)
                rule = parts[0] if len(parts) > 0 else "UNKNOWN_PYTHON_RULE"
                field = parts[1] if len(parts) > 1 else "UnknownField"
                msg = parts[2] if len(parts) > 2 else err_str
                # Ensure this error isn't already added if perform_direct_staging_claim_validations was called elsewhere
                # and this method is just for syncing. For now, it adds directly.
                self.add_error(rule_id=rule, field=field, message=msg, source="PYTHON")


class RulesEngine:
    """
    Validates claims using a combination of direct Python checks and Datalog rules.
    Datalog rules are fetched from the 'edi.filters' table.
    """
    _datalog_rules_loaded: bool = False
    _loaded_datalog_rule_ids: Set[str] = set() # Tracks IDs of loaded Datalog rule definitions
    _datalog_engine_lock = threading.Lock() # Lock for loading/clearing global Datalog state

    def __init__(self, pg_session: Session):
        """
        Initializes the RulesEngine.
        pg_session: An active SQLAlchemy session, primarily used here for loading Datalog rules
                    and passed to validation functions for master data lookups.
                    Ensure this session is managed appropriately by the caller, especially for concurrency.
                    For rule loading, it's a one-time operation. For per-claim validation, a read-only session
                    is preferred if the RulesEngine instance is shared.
        """
        self.pg_session_for_rule_loading = pg_session # Store for initial rule load
        # For performance, consider pre-loading master data for Datalog if it's static and fits in memory,
        # or ensure efficient querying during fact assertion.
        
        with RulesEngine._datalog_engine_lock:
            if not RulesEngine._datalog_rules_loaded:
                self._load_datalog_rules_from_db()
                # After initial load, general master data facts could be asserted here if they are static
                # self._assert_global_master_data_facts() 
                RulesEngine._datalog_rules_loaded = True

    def _load_datalog_rules_from_db(self):
        """Loads active Datalog rules from the 'edi.filters' table into pyDatalog."""
        # This method modifies global pyDatalog state, ensure locking if called concurrently (handled by _datalog_engine_lock).
        cid = get_correlation_id()
        logger.info(f"[{cid}] Attempting to load Datalog rules from database.")
        try:
            active_rules_orm: List[EDIFilter] = self.pg_session_for_rule_loading.query(EDIFilter)\
                .filter(EDIFilter.rule_type == 'DATALOG', EDIFilter.is_active == True, EDIFilter.is_latest_version == True)\
                .all()

            if not active_rules_orm:
                logger.warning(f"[{cid}] No active Datalog rules found in edi.filters table.")
                return

            loaded_count = 0
            newly_loaded_ids = set()
            for rule_orm_obj in active_rules_orm:
                rule_id_for_tracking = f"{rule_orm_obj.filter_name}_v{rule_orm_obj.version}"
                
                # Check if this specific rule definition (by ID) has already been processed and loaded.
                if rule_id_for_tracking in RulesEngine._loaded_datalog_rule_ids:
                    logger.debug(f"Datalog rule definition '{rule_id_for_tracking}' already loaded. Skipping.")
                    continue
                
                try:
                    logger.debug(f"Loading Datalog rule: {rule_orm_obj.filter_name} (Version {rule_orm_obj.version})")
                    datalog_statements = [
                        line.strip() for line in rule_orm_obj.rule_definition.splitlines() 
                        if line.strip() and not line.strip().startswith('%') # Ignore comments and empty lines
                    ]
                    if datalog_statements:
                        for stmt in datalog_statements:
                            pyDatalog.load(stmt) # Load statements into the global pyDatalog context
                        newly_loaded_ids.add(rule_id_for_tracking)
                        loaded_count +=1
                    else:
                        logger.warning(f"Rule {rule_orm_obj.filter_name} v{rule_orm_obj.version} has no valid Datalog statements.")
                except Exception as e_load: 
                    logger.error(f"[{cid}] Failed to load Datalog rule '{rule_orm_obj.filter_name}' (v{rule_orm_obj.version}): {e_load}", exc_info=True)
            
            RulesEngine._loaded_datalog_rule_ids.update(newly_loaded_ids)
            if loaded_count > 0:
                logger.info(f"[{cid}] Successfully loaded {loaded_count} new Datalog rules into pyDatalog engine.")
            else:
                logger.info(f"[{cid}] No new Datalog rules were loaded (all active rules might be already in engine).")

        except SQLAlchemyError as e_db:
            logger.error(f"[{cid}] Database error loading Datalog rules: {e_db}", exc_info=True)
            raise StagingDBError(f"Failed to load Datalog rules from DB: {e_db}", original_exception=e_db)
        except Exception as e_gen:
            logger.error(f"[{cid}] Unexpected error loading Datalog rules: {e_gen}", exc_info=True)
            raise AppValidationError(f"Unexpected error during Datalog rule loading: {e_gen}")

    def _assert_claim_facts(self, pg_read_session: Session, claim: StagingClaim):
        """Asserts facts about the given claim and relevant master data into the Datalog engine."""
        # This method should use a read-only session for master data lookups.
        cid = get_correlation_id()
        claim_id_str = str(claim.claim_id)
        logger.debug(f"[{cid}] Asserting Datalog facts for claim: {claim_id_str}")

        # Basic claim facts
        pyDatalog.assert_fact('claim', claim_id_str)
        if claim.facility_id: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'facility_id', str(claim.facility_id))
        if claim.patient_account_number: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'patient_account_number', str(claim.patient_account_number))
        if claim.service_date: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'service_date', claim.service_date.strftime('%Y-%m-%d'))
        if claim.financial_class_id: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'financial_class_id', str(claim.financial_class_id))
        if claim.patient_dob: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'patient_dob', claim.patient_dob.strftime('%Y-%m-%d'))
        if claim.total_charge_amount is not None: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'total_charge_amount', float(claim.total_charge_amount))

        # Line Item Facts
        for i, line in enumerate(getattr(claim, 'cms1500_line_items', [])):
            line_num = line.line_number if line.line_number is not None else (i + 1)
            pyDatalog.assert_fact('claim_line_item', claim_id_str, line_num)
            if line.service_date_from: pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'service_date_from', line.service_date_from.strftime('%Y-%m-%d'))
            if line.service_date_to: pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'service_date_to', line.service_date_to.strftime('%Y-%m-%d'))
            if line.cpt_code: pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'cpt_code', str(line.cpt_code))
            if line.line_charge_amount is not None: pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'line_charge', float(line.line_charge_amount))
            if line.units is not None: pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'units', int(line.units))


        # Diagnosis Facts
        for i, diag in enumerate(getattr(claim, 'cms1500_diagnoses', [])):
            diag_seq = diag.diagnosis_sequence if diag.diagnosis_sequence is not None else (i + 1)
            pyDatalog.assert_fact('claim_diagnosis', claim_id_str, diag_seq)
            if diag.icd_code: pyDatalog.assert_fact('diagnosis_attribute', claim_id_str, diag_seq, 'icd_code', str(diag.icd_code))
            if diag.is_primary: pyDatalog.assert_fact('diagnosis_attribute', claim_id_str, diag_seq, 'is_primary', True)

        # Master Data Facts (fetch using the provided read-only session)
        if claim.facility_id:
            facility_master = pg_read_session.query(EDIFacility.facility_id, EDIFacility.active)\
                .filter(EDIFacility.facility_id == claim.facility_id).first()
            if facility_master:
                pyDatalog.assert_fact('master_facility_exists', str(facility_master.facility_id))
                if facility_master.active: pyDatalog.assert_fact('master_facility_active', str(facility_master.facility_id))
        
        if claim.financial_class_id: # Assert even if facility_id is missing for FC-specific rules
            fc_master = pg_read_session.query(EDIFinancialClass.financial_class_id, EDIFinancialClass.facility_id, EDIFinancialClass.active)\
                .filter(EDIFinancialClass.financial_class_id == claim.financial_class_id).all() # Get all matching FCs
            for fc_entry in fc_master:
                facility_for_fc = str(fc_entry.facility_id) if fc_entry.facility_id else 'GLOBAL_FC'
                pyDatalog.assert_fact('master_financial_class_definition', str(fc_entry.financial_class_id), facility_for_fc, bool(fc_entry.active))
                # For the specific combination on the claim:
                if (claim.facility_id and fc_entry.facility_id == claim.facility_id) or (fc_entry.facility_id is None):
                     if fc_entry.active:
                        pyDatalog.assert_fact('master_financial_class_is_valid_for_claim_context', str(fc_entry.financial_class_id), str(claim.facility_id or 'GLOBAL_CONTEXT'))


        logger.debug(f"[{cid}] Asserted Datalog facts for claim {claim_id_str} using its specific read session.")

    def _retract_claim_facts(self, claim: StagingClaim):
        """Retracts facts specific to this claim after processing to keep Datalog engine clean."""
        cid = get_correlation_id()
        claim_id_str = str(claim.claim_id)
        logger.debug(f"[{cid}] Retracting Datalog facts for claim: {claim_id_str}")

        # Use pyDatalog.Var for wildcard retraction where appropriate
        V = pyDatalog.Var
        pyDatalog.retract_fact('claim', claim_id_str)
        pyDatalog.retract_fact('claim_attribute', claim_id_str, V, V)
        
        for i, line in enumerate(getattr(claim, 'cms1500_line_items', [])):
            line_num = line.line_number if line.line_number is not None else (i + 1)
            pyDatalog.retract_fact('claim_line_item', claim_id_str, line_num)
            pyDatalog.retract_fact('line_item_attribute', claim_id_str, line_num, V, V)

        for i, diag in enumerate(getattr(claim, 'cms1500_diagnoses', [])):
            diag_seq = diag.diagnosis_sequence if diag.diagnosis_sequence is not None else (i + 1)
            pyDatalog.retract_fact('claim_diagnosis', claim_id_str, diag_seq)
            pyDatalog.retract_fact('diagnosis_attribute', claim_id_str, diag_seq, V, V)

        # Retract master data facts that were specific to this claim's context during assertion
        if claim.facility_id:
            pyDatalog.retract_fact('master_facility_exists', str(claim.facility_id))
            pyDatalog.retract_fact('master_facility_active', str(claim.facility_id))
        
        if claim.financial_class_id:
            # This retraction needs to be as specific as the assertion
            # Retracting all 'master_financial_class_definition' for this FC_ID might be too broad if other claims use it.
            # The current assertion for 'master_financial_class_definition' is general.
            # 'master_financial_class_is_valid_for_claim_context' is specific and should be retracted.
            pyDatalog.retract_fact('master_financial_class_is_valid_for_claim_context', str(claim.financial_class_id), str(claim.facility_id or 'GLOBAL_CONTEXT'))
            # Careful with retracting 'master_financial_class_definition' unless it's asserted per claim. If asserted globally once, don't retract here.
            # Assuming it was asserted specifically for this claim's context:
            # pyDatalog.retract_fact('master_financial_class_definition', str(claim.financial_class_id), V, V)


        logger.debug(f"[{cid}] Retracted Datalog facts for claim {claim_id_str}.")

    def validate_claim(self, pg_validation_session: Session, claim_orm: StagingClaim) -> ClaimValidationResult:
        """
        Validates a single claim object using direct Python checks and then Datalog rules.
        The pg_validation_session is used for any database lookups needed during these validation steps.
        It is expected to be a read-only session if this RulesEngine instance is shared.
        The claim_orm object's validation_errors and flags will be updated.
        """
        cid = get_correlation_id()
        claim_id_str = str(claim_orm.claim_id)
        logger.info(f"[{cid}] Starting comprehensive validation for Claim ID: {claim_id_str}")
        
        # Initialize validation result object for this claim
        # This object will accumulate results from both Python and Datalog validations.
        validation_result = ClaimValidationResult(claim_id=claim_id_str)
        
        # Ensure claim's error list is initialized
        if claim_orm.validation_errors is None: claim_orm.validation_errors = []

        # --- Step 1: Direct Python Validations ---
        python_validation_start_time = time.perf_counter()
        try:
            # perform_direct_staging_claim_validations updates claim_orm.validation_errors and flags
            python_rules_passed = perform_direct_staging_claim_validations(pg_validation_session, claim_orm)
            validation_result.update_from_python_validation(python_rules_passed, claim_orm)
            python_validation_duration = (time.perf_counter() - python_validation_start_time) * 1000
            logger.info(f"[{cid}] Direct Python validations for claim {claim_id_str} completed in {python_validation_duration:.2f}ms. Passed: {python_rules_passed}.")
        except Exception as e_py_val:
            logger.error(f"[{cid}] Error during direct Python validation for claim {claim_id_str}: {e_py_val}", exc_info=True)
            validation_result.add_error("PYTHON_VALIDATION_ERR", "System", f"Error in Python validation phase: {e_py_val}", source="PYTHON")
            claim_orm.processing_status = STATUS_ERROR_RULES_PYTHON_VALIDATION
            # No commit here; caller manages transactions.
            return validation_result # Stop if critical Python validation phase errors out

        # --- Step 2: Datalog Rule Validations ---
        # Datalog rules might be skipped if critical Python validations have already failed majorly,
        # or they can run to accumulate all possible errors. Current logic runs Datalog regardless.
        datalog_validation_start_time = time.perf_counter()
        with RulesEngine._datalog_engine_lock: # Ensure thread-safe access to global Datalog state
            if not RulesEngine._datalog_rules_loaded:
                logger.warning(f"[{cid}] Datalog rules not loaded prior to Datalog validation for {claim_id_str}. This should not happen if init was successful.")
                # Attempting a load here might be risky if multiple threads hit this. Init should handle it.
                # For safety, if rules are not loaded, we can't proceed with Datalog part.
                validation_result.add_error("DATALOG_SETUP_ERROR", "DatalogRules", "Datalog rules are not loaded. Datalog validation skipped.", source="SYSTEM")
                # This is a system setup issue.
                if claim_orm.processing_status != STATUS_PYTHON_VALIDATION_FAILED : # If not already failed by python
                     claim_orm.processing_status = STATUS_ERROR_RULES_ENGINE_SETUP
                return validation_result # Cannot proceed with Datalog

            try:
                self._assert_claim_facts(pg_validation_session, claim_orm)

                X, R, F, M = pyDatalog.Var(), pyDatalog.Var(), pyDatalog.Var(), pyDatalog.Var()
                
                # Query for Datalog validation errors
                datalog_errors_found = pyDatalog.ask(f'validation_error("{claim_id_str}", R, F, M)')
                if datalog_errors_found:
                    for rule_id, field, message in datalog_errors_found:
                        validation_result.add_error(str(rule_id), str(field), str(message), source="DATALOG")
                
                # Query for Datalog validation warnings
                datalog_warnings_found = pyDatalog.ask(f'validation_warning("{claim_id_str}", R, F, M)')
                if datalog_warnings_found:
                    for rule_id, field, message in datalog_warnings_found:
                        validation_result.add_warning(str(rule_id), str(field), str(message), source="DATALOG")

                validation_result.datalog_rule_outcomes = {
                    "errors_found": datalog_errors_found if datalog_errors_found else [],
                    "warnings_found": datalog_warnings_found if datalog_warnings_found else []
                }
                # claim_orm.datalog_rule_outcomes = validation_result.datalog_rule_outcomes # If ORM has this field
            
            except Exception as e_datalog:
                logger.error(f"[{cid}] Error during Datalog rule execution for claim {claim_id_str}: {e_datalog}", exc_info=True)
                validation_result.add_error("DATALOG_ENGINE_ERROR", "DatalogExecution", f"Datalog engine error: {e_datalog}", source="SYSTEM")
                # claim_orm.processing_status might be updated by caller based on overall result
            finally:
                self._retract_claim_facts(claim_orm) # CRITICAL: Clean up Datalog facts for this claim

        datalog_validation_duration = (time.perf_counter() - datalog_validation_start_time) * 1000
        logger.info(f"[{cid}] Datalog validations for claim {claim_id_str} completed in {datalog_validation_duration:.2f}ms. Passed: {validation_result.is_valid_datalog}.")

        # --- Finalize claim status based on overall validation ---
        # The claim_orm.validation_errors list has been populated by direct Python checks (via update_from_python_validation)
        # and Datalog checks (via validation_result.add_error).
        # Now, update claim_orm.validation_errors with Datalog errors if not already there from Python part.
        for err_entry in validation_result.errors:
            if err_entry["source"] == "DATALOG":
                err_str = f"{err_entry['rule_id']}|{err_entry['field']}|{err_entry['message']}"
                if err_str not in claim_orm.validation_errors:
                    claim_orm.validation_errors.append(err_str)
        
        # Similar for warnings if claim_orm has a validation_warnings field
        # claim_orm.validation_warnings = [f"{w['rule_id']}|{w['field']}|{w['message']}" for w in validation_result.warnings]


        if validation_result.is_overall_valid():
            # If python checks passed and datalog checks passed
            claim_orm.processing_status = STATUS_VALIDATION_PASSED
            logger.info(f"[{cid}] Claim ID: {claim_id_str} PASSED all validations with {len(validation_result.warnings)} warnings.")
        else:
            # Determine primary reason for failure if needed
            if not validation_result.is_valid_python and validation_result.is_valid_datalog:
                claim_orm.processing_status = STATUS_PYTHON_VALIDATION_FAILED
            elif validation_result.is_valid_python and not validation_result.is_valid_datalog:
                claim_orm.processing_status = STATUS_DATALOG_VALIDATION_FAILED
            else: # Both failed or Datalog failed after Python failed
                 claim_orm.processing_status = STATUS_VALIDATION_FAILED_RULES # General failure
            logger.warning(f"[{cid}] Claim ID: {claim_id_str} FAILED overall validation. Python Valid: {validation_result.is_valid_python}, Datalog Valid: {validation_result.is_valid_datalog}. Errors: {len(validation_result.errors)}")

        # Store the detailed validation_result object on the claim_orm if needed by later pipeline stages
        # This is useful for OptimizedPipelineProcessor to access detailed results.
        setattr(claim_orm, 'pipeline_validation_result', validation_result)

        return validation_result


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, get_postgres_session, dispose_engines
    from app.database.models.postgres_models import Base as PostgresBase, StagingCMS1500LineItem # Import StagingCMS1500LineItem
    from sqlalchemy.orm import selectinload
    from decimal import Decimal # Import Decimal for test data

    setup_logging()
    set_correlation_id("RULES_ENGINE_ENHANCED_TEST")

    pg_session_main = None
    try:
        init_database_connections()
        pg_session_main = get_postgres_session() # This session is for setup and rule loading by RulesEngine

        PostgresBase.metadata.create_all(pg_session_main.get_bind())

        # --- Setup: Add dummy master data and Datalog rules to edi.filters ---
        test_facility_id = "ENHFAC001"
        facility = pg_session_main.query(EDIFacility).filter_by(facility_id=test_facility_id).first()
        if not facility:
            pg_session_main.add(EDIFacility(facility_id=test_facility_id, facility_name="Enhanced Test Hospital", active=True, facility_code="ENHTF"))
        
        test_fc_id = "ENHFC01"
        fc = pg_session_main.query(EDIFinancialClass).filter_by(financial_class_id=test_fc_id, facility_id=test_facility_id).first()
        if not fc:
            pg_session_main.add(EDIFinancialClass(financial_class_id=test_fc_id, financial_class_description="Enhanced Test Payer",
                                        facility_id=test_facility_id, active=True))

        # Datalog rule for a specific check, e.g., charge amount consistency
        charge_rule_name = "DLG_ChargeAmountPositive"
        if not pg_session_main.query(EDIFilter).filter_by(filter_name=charge_rule_name, version=1).first():
            pg_session_main.add(EDIFilter(
                filter_name=charge_rule_name, version=1, rule_type='DATALOG', is_active=True, is_latest_version=True,
                rule_definition="""
                    % Rule: Total charge amount must be positive
                    validation_error(ClaimID, "DLG_CHG_001", "total_charge_amount", "Total charge amount must be positive.") :-
                        claim_attribute(ClaimID, "total_charge_amount", Amount),
                        Amount <= 0.
                """
            ))
        pg_session_main.commit()
        # --- End Setup ---

        # Initialize RulesEngine - this will load Datalog rules using pg_session_main
        pyDatalog.clear() 
        RulesEngine._datalog_rules_loaded = False 
        RulesEngine._loaded_datalog_rule_ids.clear()
        engine = RulesEngine(pg_session_main) # RulesEngine initialized with the main session for loading rules


        # Test Case 1: Claim that passes Python direct validations but fails Datalog
        claim_fails_datalog = StagingClaim(
            claim_id="ENH_FAIL_DLG_001", facility_id=test_facility_id, patient_account_number="ACC_DLG_001",
            service_date=date(2023, 5, 5), financial_class_id=test_fc_id,
            patient_dob=date(1980, 1, 1), total_charge_amount=Decimal("-50.00") # Fails DLG_CHG_001
        )
        pg_session_main.add(claim_fails_datalog)
        pg_session_main.commit()
        
        # For validation, use a fresh (or read-only from factory) session
        with get_postgres_session(read_only=True) as validation_pg_session: # Simulating pipeline worker session
            claim_to_validate_1 = validation_pg_session.query(StagingClaim).options(
                selectinload(StagingClaim.cms1500_line_items), # Eager load for perform_direct_staging_claim_validations
                selectinload(StagingClaim.cms1500_diagnoses)
            ).filter_by(claim_id="ENH_FAIL_DLG_001").one()
            
            result1 = engine.validate_claim(validation_pg_session, claim_to_validate_1)
        
        logger.info(f"Validation for {claim_to_validate_1.claim_id}: OverallValid={result1.is_overall_valid()}, PythonValid={result1.is_valid_python}, DatalogValid={result1.is_valid_datalog}")
        logger.info(f"  Errors: {result1.errors}")
        logger.info(f"  Claim ORM status: {claim_to_validate_1.processing_status}, Errors on ORM: {claim_to_validate_1.validation_errors}")
        assert not result1.is_overall_valid() and result1.is_valid_python and not result1.is_valid_datalog
        assert any(e['rule_id'] == "DLG_CHG_001" for e in result1.errors)

        # Test Case 2: Claim that fails Python direct validations (e.g., missing facility)
        claim_fails_python = StagingClaim(
            claim_id="ENH_FAIL_PY_002", facility_id=None, patient_account_number="ACC_PY_002",
            service_date=date(2023, 5, 6), financial_class_id=test_fc_id,
            patient_dob=date(1981, 1, 1), total_charge_amount=Decimal("100.00")
        )
        pg_session_main.add(claim_fails_python)
        pg_session_main.commit()

        with get_postgres_session(read_only=True) as validation_pg_session:
            claim_to_validate_2 = validation_pg_session.query(StagingClaim).options(
                selectinload(StagingClaim.cms1500_line_items),
                selectinload(StagingClaim.cms1500_diagnoses)
            ).filter_by(claim_id="ENH_FAIL_PY_002").one()
            
            result2 = engine.validate_claim(validation_pg_session, claim_to_validate_2)

        logger.info(f"Validation for {claim_to_validate_2.claim_id}: OverallValid={result2.is_overall_valid()}, PythonValid={result2.is_valid_python}, DatalogValid={result2.is_valid_datalog}")
        logger.info(f"  Errors: {result2.errors}")
        logger.info(f"  Claim ORM status: {claim_to_validate_2.processing_status}, Errors on ORM: {claim_to_validate_2.validation_errors}")
        assert not result2.is_overall_valid() and not result2.is_valid_python
        assert any(e['rule_id'] == "CORE_FAC_01" and e['source'] == "PYTHON" for e in result2.errors) # Check for Python validation error

        # Commit status changes made by engine.validate_claim to the ORM objects (if any)
        # The `validate_claim` itself updates the ORM object status and errors.
        # The session used by the caller of `validate_claim` needs to commit these.
        pg_session_main.commit()


    except Exception as e:
        logger.critical(f"Error in RulesEngine enhanced test script: {e}", exc_info=True)
        if pg_session_main and pg_session_main.is_active:
            pg_session_main.rollback()
    finally:
        if pg_session_main:
            pg_session_main.close()
        dispose_engines()
        pyDatalog.clear() 
        logger.info("RulesEngine enhanced test script finished.")

