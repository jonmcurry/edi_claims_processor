# app/processing/rules_engine.py
"""
Implements business logic for claim validation using a combination of direct Python checks
and a pre-compiled Datalog ruleset for high performance.
"""
from __future__ import annotations
import time
from typing import List, Dict, Any, Optional
import threading

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
# Corrected pyDatalog imports for thread-safe operation
from pyDatalog import pyDatalog, Logic

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import StagingDBError
from app.database.models.postgres_models import (
    StagingClaim,
    Filter as EDIFilter,
    Facility as EDIFacility,
)
from app.database.postgres_handler import perform_direct_staging_claim_validations

logger = get_logger('app.processing.rules_engine')

# Status constants
STATUS_PYTHON_VALIDATION_FAILED = "VALIDATION_PYTHON_FAILED"
STATUS_DATALOG_VALIDATION_FAILED = "VALIDATION_DATALOG_FAILED"
STATUS_VALIDATION_FAILED_RULES = "VALIDATION_FAILED_RULES"
STATUS_VALIDATION_PASSED = "VALIDATION_PASSED"
STATUS_ERROR_RULES_ENGINE_SETUP = "ERROR_RULES_ENGINE_SETUP"
STATUS_ERROR_RULES_PYTHON_VALIDATION = "ERROR_RULES_PYTHON_VALIDATION"
STATUS_ERROR_RULES_DATALOG = "ERROR_RULES_DATALOG"

class ClaimValidationResult:
    """Holds the result of a claim validation."""
    def __init__(self, claim_id: str):
        self.claim_id = claim_id
        self.is_valid_python: bool = True
        self.is_valid_datalog: bool = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.datalog_rule_outcomes: Dict[str, Any] = {}
        self.final_status_staging: str = ""

    @property
    def is_overall_valid(self) -> bool:
        return self.is_valid_python and self.is_valid_datalog

    def add_error(self, rule_id: str, field: str, message: str, source: str = "DATALOG", details: Optional[Dict] = None):
        if source.upper() == "PYTHON":
            self.is_valid_python = False
        elif source.upper() == "DATALOG":
            self.is_valid_datalog = False
        
        error_entry = {
            "rule_id": rule_id, "field": field, "message": message,
            "source": source.upper(), "details": details or {}
        }
        if not any(e['rule_id'] == rule_id and e['field'] == field for e in self.errors):
            self.errors.append(error_entry)
        logger.warning(f"Validation Error for Claim {self.claim_id}, Source: {source}, Rule: {rule_id}, Field: {field}: {message}")

    def add_warning(self, rule_id: str, field: str, message: str, source: str = "DATALOG", details: Optional[Dict] = None):
        warning_entry = {
            "rule_id": rule_id, "field": field, "message": message,
            "source": source.upper(), "details": details or {}
        }
        if not any(w['rule_id'] == rule_id and w['field'] == field for w in self.warnings):
            self.warnings.append(warning_entry)
        logger.info(f"Validation Warning for Claim {self.claim_id}, Source: {source}, Rule: {rule_id}, Field: {field}: {message}")

    def update_from_python_validation(self, python_valid_passed: bool, claim_with_python_errors: StagingClaim):
        self.is_valid_python = python_valid_passed
        if not python_valid_passed:
            for err_str in getattr(claim_with_python_errors, 'validation_errors', []):
                parts = err_str.split('|', 2)
                rule = parts[0] if len(parts) > 0 else "UNKNOWN_PYTHON_RULE"
                field = parts[1] if len(parts) > 1 else "UnknownField"
                msg = parts[2] if len(parts) > 2 else err_str
                self.add_error(rule_id=rule, field=field, message=msg, source="PYTHON")

class RulesEngine:
    """
    Validates claims using Python checks and a pre-compiled Datalog ruleset
    for high performance.
    """
    _datalog_logic_instance = None
    _engine_init_lock = threading.Lock()

    def __init__(self, pg_session: Session):
        """
        Initializes the RulesEngine.
        It loads and compiles Datalog rules once and stores them in a class-level variable
        to avoid reloading for every instance, which is critical for performance.
        """
        with RulesEngine._engine_init_lock:
            if RulesEngine._datalog_logic_instance is None:
                self._load_and_compile_datalog_rules(pg_session)
    
    def _load_and_compile_datalog_rules(self, pg_session: Session):
        """
        Loads Datalog rules from the DB and compiles them into a reusable Logic object.
        This is a one-time operation.
        """
        cid = get_correlation_id()
        logger.info(f"[{cid}] First-time initialization: Loading and compiling Datalog rules...")
        try:
            active_rules_orm: List[EDIFilter] = pg_session.query(EDIFilter).filter(
                EDIFilter.rule_type == 'DATALOG', EDIFilter.is_active == True, EDIFilter.is_latest_version == True
            ).all()

            if not active_rules_orm:
                logger.warning(f"[{cid}] No active Datalog rules found in edi.filters table. Datalog validation will be skipped.")
                RulesEngine._datalog_logic_instance = Logic() # Create an empty logic instance
                return

            logic = Logic()
            loaded_definitions = 0
            for rule_orm_obj in active_rules_orm:
                # *** CORRECTED LINE ***
                # Call load on the pyDatalog module, passing the logic instance as the target.
                pyDatalog.load(rule_orm_obj.rule_definition, logic=logic)
                loaded_definitions += 1
            
            RulesEngine._datalog_logic_instance = logic
            logger.info(f"[{cid}] Successfully compiled {loaded_definitions} Datalog rule definitions into a reusable logic instance.")

        except SQLAlchemyError as e_db:
            logger.error(f"[{cid}] Database error loading Datalog rules: {e_db}", exc_info=True)
            raise StagingDBError(f"Failed to load Datalog rules from DB: {e_db}", original_exception=e_db)

    def _assert_claim_facts(self, claim: StagingClaim, pg_read_session: Session, temp_logic):
        """
        Asserts facts about a single claim into a temporary, thread-safe Datalog logic instance.
        """
        claim_id_str = str(claim.claim_id)
        
        # Assert claim facts into the temporary logic instance
        temp_logic.assert_fact('claim', claim_id_str)
        if claim.facility_id: temp_logic.assert_fact('claim_attribute', claim_id_str, 'facility_id', str(claim.facility_id))
        if claim.financial_class_id: temp_logic.assert_fact('claim_attribute', claim_id_str, 'financial_class_id', str(claim.financial_class_id))
        if claim.total_charge_amount is not None: temp_logic.assert_fact('claim_attribute', claim_id_str, 'total_charge_amount', float(claim.total_charge_amount))
        
        # Assert master data facts from DB
        if claim.facility_id:
            facility_master = pg_read_session.query(EDIFacility.active).filter(EDIFacility.facility_id == claim.facility_id).first()
            if facility_master:
                temp_logic.assert_fact('master_facility_exists', str(claim.facility_id))
                if facility_master.active: temp_logic.assert_fact('master_facility_active', str(claim.facility_id))

    def validate_claim(self, pg_validation_session: Session, claim_orm: StagingClaim) -> ClaimValidationResult:
        """
        Validates a single claim using Python checks and the pre-compiled Datalog ruleset.
        """
        cid = get_correlation_id()
        claim_id_str = str(claim_orm.claim_id)
        logger.info(f"[{cid}] Starting comprehensive validation for Claim ID: {claim_id_str}")
        
        validation_result = ClaimValidationResult(claim_id=claim_id_str)
        
        if claim_orm.validation_errors is None:
            claim_orm.validation_errors = []

        # --- Python-based validation ---
        try:
            python_rules_passed = perform_direct_staging_claim_validations(pg_validation_session, claim_orm)
            validation_result.update_from_python_validation(python_rules_passed, claim_orm)
        except Exception as e:
            logger.error(f"[{cid}] Unhandled error in Python validation phase for {claim_id_str}: {e}", exc_info=True)
            validation_result.add_error("PYTHON_VALIDATION_ERR", "System", str(e), "PYTHON")
        
        # --- Datalog-based validation ---
        if RulesEngine._datalog_logic_instance is None:
            logger.error(f"[{cid}] Datalog logic instance not available. Skipping Datalog validation.")
            validation_result.is_valid_datalog = True # Assume valid if engine fails
        else:
            try:
                # Use a temporary logic instance derived from the main one to ensure thread safety.
                # This inherits all the compiled rules but keeps facts isolated.
                with RulesEngine._datalog_logic_instance.new_logic() as temp_logic:
                    # Assert facts for the current claim into the temporary logic instance.
                    self._assert_claim_facts(claim_orm, pg_validation_session, temp_logic)
                    
                    # Query for validation errors using the temporary logic.
                    datalog_errors = temp_logic.ask(f'validation_error("{claim_id_str}", R, F, M)')
                    
                    if datalog_errors and hasattr(datalog_errors, 'data'):
                        for rule_id, field, message in datalog_errors.data:
                            validation_result.add_error(str(rule_id), str(field), str(message), "DATALOG")
                    else:
                        validation_result.is_valid_datalog = True

            except Exception as e_datalog:
                logger.error(f"[{cid}] Error during Datalog rule execution for claim {claim_id_str}: {e_datalog}", exc_info=True)
                validation_result.add_error("DATALOG_ENGINE_ERROR", "DatalogExecution", f"Datalog engine error: {e_datalog}", "SYSTEM")
        
        # --- Finalize Status ---
        if validation_result.is_overall_valid:
            validation_result.final_status_staging = STATUS_VALIDATION_PASSED
            logger.info(f"[{cid}] Claim ID: {claim_id_str} PASSED all validations.")
        else:
            validation_result.final_status_staging = STATUS_VALIDATION_FAILED_RULES
            logger.warning(f"[{cid}] Claim ID: {claim_id_str} FAILED validation.")
        
        return validation_result
