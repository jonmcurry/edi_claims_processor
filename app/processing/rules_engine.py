# app/processing/rules_engine.py
"""
Implements business logic for claim validation using pyDatalog.
Rules are fetched from the 'edi.filters' table in the database.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import List, Tuple, Dict, Any, Set

from sqlalchemy.orm import Session
from pyDatalog import pyDatalog

from sqlalchemy.exc import SQLAlchemyError

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import ValidationError as AppValidationError, StagingDBError
from app.database.models.postgres_models import (
    StagingClaim, StagingCMS1500LineItem, StagingCMS1500Diagnosis,
    Filter as EDIFilter, # For loading Datalog rules
    Facility as EDIFacility,
    FinancialClass as EDIFinancialClass
)
# from app.database.postgres_handler import get_active_datalog_rules # This function would be in postgres_handler.py

logger = get_logger('app.processing.rules_engine')

class ClaimValidationResult:
    """Holds the result of a claim validation."""
    def __init__(self, claim_id: str):
        self.claim_id = claim_id
        self.is_valid = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.datalog_rule_outcomes: Dict[str, Any] = {} # To store raw outcomes if needed

    def add_error(self, rule_id: str, field: str, message: str, details: Dict = None):
        self.is_valid = False
        error_entry = {"rule_id": rule_id, "field": field, "message": message, "details": details or {}}
        if error_entry not in self.errors: # Avoid duplicate errors from same rule/fact
            self.errors.append(error_entry)
        logger.warning(f"Validation Error for Claim {self.claim_id}, Rule {rule_id}, Field {field}: {message}")

    def add_warning(self, rule_id: str, field: str, message: str, details: Dict = None):
        warning_entry = {"rule_id": rule_id, "field": field, "message": message, "details": details or {}}
        if warning_entry not in self.warnings:
            self.warnings.append(warning_entry)
        logger.info(f"Validation Warning for Claim {self.claim_id}, Rule {rule_id}, Field {field}: {message}")


class RulesEngine:
    """
    Validates claims against a set of Datalog rules fetched from the database.
    """
    _datalog_rules_loaded = False
    _loaded_rule_ids: Set[str] = set()

    def __init__(self, pg_session: Session):
        self.pg_session = pg_session
        if not RulesEngine._datalog_rules_loaded:
            self._load_datalog_rules_from_db()
            RulesEngine._datalog_rules_loaded = True

    def _clear_datalog_facts_for_claim(self, claim_id: str):
        """Clears facts related to a specific claim to avoid interference between validations."""
        # pyDatalog does not have a direct "delete_facts_matching_pattern"
        # We need to retract specific facts asserted for this claim.
        # This requires knowing which predicates were used.
        # A common pattern is to include claim_id in all asserted facts.
        
        # Example predicates that might have been asserted:
        predicates_to_clear = [
            'claim_attribute', 'claim_line_item_attribute', 'claim_diagnosis_attribute',
            'master_facility_active', 'master_financial_class_valid_for_facility'
            # Add any other predicates you assert on a per-claim basis
        ]
        
        # Iterate and retract. This is not very efficient.
        # A better way is to use pyDatalog.clear() before processing each claim,
        # then re-load common rules and re-assert common facts if any.
        # For now, let's assume a full clear and reload of rules per claim for simplicity,
        # or manage facts carefully.
        
        # Simplest for now: clear all facts and re-assert rules and facts.
        # This is safe but might be slow if rules are numerous.
        # pyDatalog.clear()
        # RulesEngine._datalog_rules_loaded = False # Force reload of rules
        # self._load_datalog_rules_from_db()
        # RulesEngine._datalog_rules_loaded = True

        # More targeted retraction (if you track asserted facts or use specific terms):
        # For example, if all claim-specific facts use a term like claim_fact(ClaimID, ...):
        # pyDatalog.retract_fact('claim_fact', claim_id, pyDatalog.Var()) # Retract all matching
        # This is highly dependent on how you structure your facts.
        # For this example, we'll rely on asserting facts with unique claim_ids and rules
        # designed to work with specific claim_ids. If rules are general, facts need careful namespacing or clearing.
        
        # A common approach:
        # 1. Load general rules once.
        # 2. For each claim:
        #    a. Assert claim-specific facts.
        #    b. Query.
        #    c. Retract claim-specific facts.
        # This requires knowing exactly which facts were asserted for the claim.
        logger.debug(f"Preparing Datalog environment for claim {claim_id}. (Fact clearing strategy depends on rule design)")


    def _load_datalog_rules_from_db(self):
        """
        Loads active Datalog rules from the 'edi.filters' table.
        """
        cid = get_correlation_id()
        logger.info(f"[{cid}] Loading Datalog rules from database.")
        try:
            # This would ideally be a function in postgres_handler.py
            # active_rules_orm = get_active_datalog_rules(self.pg_session)
            active_rules_orm: List[EDIFilter] = self.pg_session.query(EDIFilter)\
                .filter(EDIFilter.rule_type == 'DATALOG', EDIFilter.is_active == True, EDIFilter.is_latest_version == True)\
                .all()

            if not active_rules_orm:
                logger.warning(f"[{cid}] No active Datalog rules found in edi.filters table.")
                return

            loaded_count = 0
            for rule_orm_obj in active_rules_orm:
                rule_id = f"{rule_orm_obj.filter_name}_v{rule_orm_obj.version}"
                if rule_id in RulesEngine._loaded_rule_ids and pyDatalog.Program._loaded_modules.get(rule_id): # Avoid reloading same rule string
                    logger.debug(f"Rule '{rule_id}' already loaded. Skipping.")
                    continue
                try:
                    logger.debug(f"Loading Datalog rule: {rule_orm_obj.filter_name} (Version {rule_orm_obj.version})")
                    # pyDatalog.load_program expects a list of strings (Datalog statements)
                    # The rule_definition should contain valid Datalog syntax.
                    datalog_statements = [line for line in rule_orm_obj.rule_definition.splitlines() if line.strip() and not line.strip().startswith('%')] # Ignore comments
                    if datalog_statements:
                        # Create a new program context for each rule to avoid name clashes if rules are not designed carefully
                        # Or ensure rule predicates are unique across all loaded rules.
                        # For simplicity, we load into the global pyDatalog context.
                        # pyDatalog.create_program(rule_orm_obj.rule_definition) # This might not be how load_program works
                        
                        # pyDatalog.load_program takes a list of strings
                        for stmt in datalog_statements:
                            pyDatalog.load(stmt) # Load statements one by one
                        
                        RulesEngine._loaded_rule_ids.add(rule_id)
                        loaded_count +=1
                    else:
                        logger.warning(f"Rule {rule_orm_obj.filter_name} v{rule_orm_obj.version} has no valid statements.")
                except Exception as e_load: # pyDatalog might raise various errors on bad syntax
                    logger.error(f"[{cid}] Failed to load Datalog rule '{rule_orm_obj.filter_name}' (Version {rule_orm_obj.version}): {e_load}", exc_info=True)
            
            logger.info(f"[{cid}] Successfully loaded {loaded_count} new Datalog rules into pyDatalog engine.")
            RulesEngine._datalog_rules_loaded = True

        except SQLAlchemyError as e_db:
            logger.error(f"[{cid}] Database error loading Datalog rules: {e_db}", exc_info=True)
            raise StagingDBError(f"Failed to load Datalog rules from DB: {e_db}", original_exception=e_db)
        except Exception as e_gen:
            logger.error(f"[{cid}] Unexpected error loading Datalog rules: {e_gen}", exc_info=True)
            raise AppValidationError(f"Unexpected error during Datalog rule loading: {e_gen}")


    def _assert_claim_facts(self, claim: StagingClaim):
        """
        Asserts facts about the given claim into the Datalog engine.
        Also asserts relevant master data facts.
        """
        cid = get_correlation_id()
        claim_id_str = str(claim.claim_id) # Ensure it's a string for Datalog
        logger.debug(f"[{cid}] Asserting facts for claim: {claim_id_str}")

        # Clear any previous facts for this specific claim_id if not clearing globally
        # This depends on how facts are named. Example:
        # pyDatalog.retract_fact('claim_attribute', claim_id_str, pyDatalog.Var(), pyDatalog.Var())
        # pyDatalog.retract_fact('line_item_attribute', claim_id_str, pyDatalog.Var(), pyDatalog.Var(), pyDatalog.Var())
        # pyDatalog.retract_fact('diagnosis_attribute', claim_id_str, pyDatalog.Var(), pyDatalog.Var(), pyDatalog.Var())
        # This is crucial to prevent data from one claim affecting another if facts are not namespaced.
        # For this example, we assume rules are written to expect facts like `claim_attribute(ClaimID, Attribute, Value)`.

        # --- Claim Level Facts ---
        pyDatalog.assert_fact('claim', claim_id_str)
        if claim.facility_id:
            pyDatalog.assert_fact('claim_attribute', claim_id_str, 'facility_id', str(claim.facility_id))
        if claim.patient_account_number:
            pyDatalog.assert_fact('claim_attribute', claim_id_str, 'patient_account_number', str(claim.patient_account_number))
        if claim.service_date:
            pyDatalog.assert_fact('claim_attribute', claim_id_str, 'service_date', claim.service_date.strftime('%Y-%m-%d'))
        if claim.financial_class_id:
            pyDatalog.assert_fact('claim_attribute', claim_id_str, 'financial_class_id', str(claim.financial_class_id))
        if claim.patient_dob:
            pyDatalog.assert_fact('claim_attribute', claim_id_str, 'patient_dob', claim.patient_dob.strftime('%Y-%m-%d'))
        if claim.total_charge_amount is not None:
             pyDatalog.assert_fact('claim_attribute', claim_id_str, 'total_charge_amount', float(claim.total_charge_amount)) # Datalog typically prefers floats/ints

        # --- Line Item Facts ---
        for i, line in enumerate(claim.cms1500_line_items):
            line_num = line.line_number or (i + 1)
            pyDatalog.assert_fact('claim_line_item', claim_id_str, line_num)
            if line.service_date_from:
                pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'service_date_from', line.service_date_from.strftime('%Y-%m-%d'))
            if line.service_date_to:
                pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'service_date_to', line.service_date_to.strftime('%Y-%m-%d'))
            if line.cpt_code:
                pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'cpt_code', str(line.cpt_code))
            if line.line_charge_amount is not None:
                pyDatalog.assert_fact('line_item_attribute', claim_id_str, line_num, 'line_charge', float(line.line_charge_amount))


        # --- Diagnosis Facts ---
        for i, diag in enumerate(claim.cms1500_diagnoses):
            diag_seq = diag.diagnosis_sequence or (i + 1)
            pyDatalog.assert_fact('claim_diagnosis', claim_id_str, diag_seq)
            if diag.icd_code:
                pyDatalog.assert_fact('diagnosis_attribute', claim_id_str, diag_seq, 'icd_code', str(diag.icd_code))
            if diag.is_primary:
                pyDatalog.assert_fact('diagnosis_attribute', claim_id_str, diag_seq, 'is_primary', True)


        # --- Master Data Facts (relevant to this claim) ---
        # This is where it gets tricky: assert ALL master data, or only relevant?
        # For performance, assert only relevant master data.
        if claim.facility_id:
            facility_master = self.pg_session.query(EDIFacility.facility_id, EDIFacility.active)\
                .filter(EDIFacility.facility_id == claim.facility_id).first()
            if facility_master:
                pyDatalog.assert_fact('master_facility_exists', str(facility_master.facility_id))
                if facility_master.active:
                    pyDatalog.assert_fact('master_facility_active', str(facility_master.facility_id))
        
        if claim.financial_class_id and claim.facility_id:
            fc_master = self.pg_session.query(EDIFinancialClass.financial_class_id, EDIFinancialClass.facility_id, EDIFinancialClass.active)\
                .filter(EDIFinancialClass.financial_class_id == claim.financial_class_id,
                        EDIFinancialClass.facility_id == claim.facility_id).first()
            if fc_master:
                pyDatalog.assert_fact('master_financial_class_exists_for_facility', str(fc_master.financial_class_id), str(fc_master.facility_id))
                if fc_master.active:
                    pyDatalog.assert_fact('master_financial_class_active_for_facility', str(fc_master.financial_class_id), str(fc_master.facility_id))
        
        logger.debug(f"[{cid}] Asserted facts for claim {claim_id_str} into Datalog engine.")

    def _retract_claim_facts(self, claim: StagingClaim):
        """Retracts facts specific to this claim after processing."""
        cid = get_correlation_id()
        claim_id_str = str(claim.claim_id)
        logger.debug(f"[{cid}] Retracting facts for claim: {claim_id_str}")

        # Must match the predicates and arity used in _assert_claim_facts
        pyDatalog.retract_fact('claim', claim_id_str)
        # Use pyDatalog.Var() for "any value" in other positions
        pyDatalog.retract_fact('claim_attribute', claim_id_str, pyDatalog.Var(), pyDatalog.Var())
        
        for i, line in enumerate(claim.cms1500_line_items):
            line_num = line.line_number or (i + 1)
            pyDatalog.retract_fact('claim_line_item', claim_id_str, line_num)
            pyDatalog.retract_fact('line_item_attribute', claim_id_str, line_num, pyDatalog.Var(), pyDatalog.Var())

        for i, diag in enumerate(claim.cms1500_diagnoses):
            diag_seq = diag.diagnosis_sequence or (i + 1)
            pyDatalog.retract_fact('claim_diagnosis', claim_id_str, diag_seq)
            pyDatalog.retract_fact('diagnosis_attribute', claim_id_str, diag_seq, pyDatalog.Var(), pyDatalog.Var())

        # Retract relevant master data facts asserted for this claim
        if claim.facility_id:
            pyDatalog.retract_fact('master_facility_exists', str(claim.facility_id))
            pyDatalog.retract_fact('master_facility_active', str(claim.facility_id))
        
        if claim.financial_class_id and claim.facility_id:
            pyDatalog.retract_fact('master_financial_class_exists_for_facility', str(claim.financial_class_id), str(claim.facility_id))
            pyDatalog.retract_fact('master_financial_class_active_for_facility', str(claim.financial_class_id), str(claim.facility_id))
            
        logger.debug(f"[{cid}] Retracted facts for claim {claim_id_str}.")


    def validate_claim(self, claim: StagingClaim) -> ClaimValidationResult:
        """
        Validates a single claim object using Datalog rules.
        """
        cid = get_correlation_id()
        claim_id_str = str(claim.claim_id)
        logger.debug(f"[{cid}] Starting Datalog validation for Claim ID: {claim_id_str}")
        result = ClaimValidationResult(claim_id=claim_id_str)

        if not RulesEngine._datalog_rules_loaded:
            logger.warning(f"[{cid}] Datalog rules not loaded. Attempting to load now.")
            self._load_datalog_rules_from_db() # Attempt to load if not already
            if not RulesEngine._datalog_rules_loaded:
                result.add_error("SYSTEM_ERROR", "DatalogRules", "Datalog rules could not be loaded. Validation skipped.")
                claim.processing_status = "ERROR_RULES_ENGINE_SETUP"
                return result
        
        # Prepare Datalog environment for this claim
        # self._clear_datalog_facts_for_claim(claim_id_str) # Or use global clear and reload if simpler
        
        # For this example, let's assume facts are namespaced by claim_id or rules are specific enough.
        # A robust solution requires careful fact management.
        # We will assert facts for the current claim, then retract them after querying.

        try:
            self._assert_claim_facts(claim)

            # Query for errors
            # Assumes Datalog rules define: validation_error(ClaimID, RuleID, Field, Message)
            X, R, F, M = pyDatalog.Var(), pyDatalog.Var(), pyDatalog.Var(), pyDatalog.Var()
            # Query for errors specific to this claim_id
            errors_found = pyDatalog.ask(f'validation_error("{claim_id_str}", R, F, M)')
            if errors_found:
                for rule_id, field, message in errors_found:
                    result.add_error(str(rule_id), str(field), str(message))
            
            # Query for warnings
            # Assumes Datalog rules define: validation_warning(ClaimID, RuleID, Field, Message)
            warnings_found = pyDatalog.ask(f'validation_warning("{claim_id_str}", R, F, M)')
            if warnings_found:
                for rule_id, field, message in warnings_found:
                    result.add_warning(str(rule_id), str(field), str(message))

            # Example: Query for a specific validation status if rules define it
            # is_claim_structurally_valid = pyDatalog.ask(f'is_claim_structurally_valid("{claim_id_str}")')
            # if is_claim_structurally_valid:
            #    claim.facility_validated = True # Example, depends on Datalog rule output

            # Store raw Datalog outcomes if needed for debugging or detailed logging
            result.datalog_rule_outcomes = {
                "errors_found": errors_found if errors_found else [],
                "warnings_found": warnings_found if warnings_found else []
            }
            claim.datalog_rule_outcomes = result.datalog_rule_outcomes # Store on ORM if field exists

        except Exception as e_datalog:
            logger.error(f"[{cid}] Error during Datalog processing for claim {claim_id_str}: {e_datalog}", exc_info=True)
            result.add_error("DATALOG_ENGINE_ERROR", "ClaimValidation", f"Datalog engine error: {e_datalog}")
            claim.processing_status = "ERROR_RULES_DATALOG"
        finally:
            self._retract_claim_facts(claim) # CRUCIAL: Clean up facts for this claim

        # Update StagingClaim based on Datalog validation outcome
        claim.validation_errors = [f"{e['rule_id']}|{e['field']}|{e['message']}" for e in result.errors]
        claim.validation_warnings = [f"{w['rule_id']}|{w['field']}|{w['message']}" for w in result.warnings]
        
        if result.is_valid:
            logger.info(f"[{cid}] Claim ID: {claim_id_str} passed Datalog validations with {len(result.warnings)} warnings.")
            claim.processing_status = "VALIDATED_DATALOG_PASSED"
            # Set specific flags like facility_validated based on Datalog output if designed that way
            # For example, if you have a Datalog fact `facility_validation_status(ClaimID, Status)`
            # facility_status = pyDatalog.ask(f'facility_validation_status("{claim_id_str}", S)')
            # if facility_status and facility_status[0][0] == 'VALID': claim.facility_validated = True
            # else: claim.facility_validated = False
            # This requires careful rule design. For now, we only set overall status.
            claim.facility_validated = True # Assume if no facility errors, it's valid for this example
            claim.financial_class_validated = True # Assume if no FC errors, it's valid

        else:
            logger.warning(f"[{cid}] Claim ID: {claim_id_str} failed Datalog validation with {len(result.errors)} errors.")
            claim.processing_status = "VALIDATION_DATALOG_FAILED"
            # Determine specific flag failures based on error fields/rule_ids if needed
            if any(e['field'] == 'facility_id' for e in result.errors):
                claim.facility_validated = False
            if any(e['field'] == 'financial_class_id' for e in result.errors):
                claim.financial_class_validated = False
        
        return result


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, get_postgres_session, dispose_engines
    from app.database.models.postgres_models import Base as PostgresBase 

    setup_logging()
    set_correlation_id("RULES_ENGINE_DATALOG_TEST")

    pg_session = None
    try:
        init_database_connections()
        pg_session = get_postgres_session()

        PostgresBase.metadata.create_all(pg_session.get_bind())

        # --- Setup: Add dummy master data and Datalog rules to edi.filters ---
        # 1. Dummy Facility
        test_facility_id = "DLG_FAC001"
        test_facility = pg_session.query(EDIFacility).filter_by(facility_id=test_facility_id).first()
        if not test_facility:
            pg_session.add(EDIFacility(facility_id=test_facility_id, facility_name="Datalog Test Hospital", active=True, facility_code="DLGTF"))
        
        # 2. Dummy Financial Class for Test Facility
        test_fc_id = "DLG_FC01"
        test_fc = pg_session.query(EDIFinancialClass).filter_by(financial_class_id=test_fc_id, facility_id=test_facility_id).first()
        if not test_fc:
            pg_session.add(EDIFinancialClass(financial_class_id=test_fc_id, financial_class_description="Datalog Test Payer",
                                        facility_id=test_facility_id, active=True))

        # 3. Dummy Datalog Rules in edi.filters
        rule_facility_exists_name = "DLG_FacilityExists"
        rule_facility_exists = pg_session.query(EDIFilter).filter_by(filter_name=rule_facility_exists_name, version=1).first()
        if not rule_facility_exists:
            pg_session.add(EDIFilter(
                filter_name=rule_facility_exists_name, version=1, rule_type='DATALOG', is_active=True, is_latest_version=True,
                rule_definition="""
                    % Rule: Facility ID on claim must exist in master_facility_exists
                    validation_error(ClaimID, "DLG_FAC_001", "facility_id", "Facility ID on claim does not exist in master data.") :-
                        claim_attribute(ClaimID, "facility_id", FacID),
                        +master_facility_exists(FacID).
                    % Rule: Facility ID on claim must be active
                    validation_warning(ClaimID, "DLG_FAC_002", "facility_id", "Facility ID on claim is inactive.") :-
                        claim_attribute(ClaimID, "facility_id", FacID),
                        master_facility_exists(FacID),
                        +master_facility_active(FacID).
                """
            ))
        
        rule_pat_acc_num_name = "DLG_PatientAccountNonEmpty"
        rule_pat_acc_num = pg_session.query(EDIFilter).filter_by(filter_name=rule_pat_acc_num_name, version=1).first()
        if not rule_pat_acc_num:
            pg_session.add(EDIFilter(
                filter_name=rule_pat_acc_num_name, version=1, rule_type='DATALOG', is_active=True, is_latest_version=True,
                rule_definition="""
                    % Rule: Patient Account Number must exist
                    validation_error(ClaimID, "DLG_PAT_001", "patient_account_number", "Patient Account Number is missing.") :-
                        claim(ClaimID),
                        +claim_attribute(ClaimID, "patient_account_number", _).
                """
            ))
        pg_session.commit()
        # --- End Setup ---

        # Initialize RulesEngine - this will load rules
        pyDatalog.clear() # Clear any state from previous runs or other modules
        RulesEngine._datalog_rules_loaded = False # Force reload for test
        RulesEngine._loaded_rule_ids.clear()
        engine = RulesEngine(pg_session)


        # Test Case 1: Valid Claim by Datalog standards
        valid_claim = StagingClaim(
            claim_id="DLG_VALID_C001", facility_id=test_facility_id, patient_account_number="ACC123DLG",
            service_date=date(2023, 1, 15), financial_class_id=test_fc_id,
            patient_dob=date(1990, 5, 20)
        )
        pg_session.add(valid_claim) # Add to session for potential updates by engine

        result1 = engine.validate_claim(valid_claim)
        logger.info(f"Datalog Validation for {valid_claim.claim_id}: Valid={result1.is_valid}, Errors={len(result1.errors)}, Warnings={len(result1.warnings)}")
        logger.info(f"  Errors: {result1.errors}")
        logger.info(f"  Warnings: {result1.warnings}")
        logger.info(f"  Claim status: {valid_claim.processing_status}, Facility Validated: {valid_claim.facility_validated}")


        # Test Case 2: Invalid Facility ID (not in master_facility_exists)
        invalid_fac_claim = StagingClaim(
            claim_id="DLG_INV_FAC_C002", facility_id="NONEXISTFAC_DLG", patient_account_number="ACC456DLG",
            service_date=date(2023, 2, 10), patient_dob=date(1985, 3, 10)
        )
        pg_session.add(invalid_fac_claim)
        result2 = engine.validate_claim(invalid_fac_claim)
        logger.info(f"Datalog Validation for {invalid_fac_claim.claim_id}: Valid={result2.is_valid}, Errors={len(result2.errors)}")
        logger.info(f"  Errors: {result2.errors}")
        logger.info(f"  Claim status: {invalid_fac_claim.processing_status}, Facility Validated: {invalid_fac_claim.facility_validated}")

        # Test Case 3: Missing Patient Account Number
        missing_acc_claim = StagingClaim(
            claim_id="DLG_MISS_ACC_C003", facility_id=test_facility_id, # No patient_account_number
            service_date=date(2023, 3, 5), patient_dob=date(1970, 1, 1)
        )
        pg_session.add(missing_acc_claim)
        result3 = engine.validate_claim(missing_acc_claim)
        logger.info(f"Datalog Validation for {missing_acc_claim.claim_id}: Valid={result3.is_valid}, Errors={len(result3.errors)}")
        logger.info(f"  Errors: {result3.errors}")

        pg_session.commit() # Commit changes to claim statuses

    except Exception as e:
        logger.critical(f"Error in Datalog Rules Engine test script: {e}", exc_info=True)
        if pg_session and pg_session.is_active:
            pg_session.rollback()
    finally:
        if pg_session:
            pg_session.close()
        dispose_engines()
        pyDatalog.clear() # Clean up pyDatalog state after tests
        logger.info("Datalog Rules Engine test script finished.")

