# app/processing/rules_engine.py
"""
Implements business logic for claim validation using a combination of direct Python checks
and Datalog rules fetched from the 'edi.filters' table.
"""
import time
from datetime import date
from decimal import Decimal
from typing import List, Dict, Any, Set, Optional
import threading

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
# Corrected pyDatalog imports to be thread-safe and to get Var correctly
from pyDatalog import pyDatalog, Logic
from pyDatalog.pyDatalog import Var as pyDatalogVar

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import ValidationError as AppValidationError, StagingDBError
from app.database.models.postgres_models import (
    StagingClaim,
    Filter as EDIFilter,
    Facility as EDIFacility,
    FinancialClass as EDIFinancialClass
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
        # Added missing attribute
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
    """Validates claims using Python checks and Datalog rules."""
    _datalog_rules_loaded_main_thread: bool = False
    _loaded_datalog_definitions: List[str] = []
    _engine_init_lock = threading.Lock()

    def __init__(self, pg_session: Session):
        with RulesEngine._engine_init_lock:
            if not RulesEngine._datalog_rules_loaded_main_thread:
                self._load_and_store_datalog_rules(pg_session)
                RulesEngine._datalog_rules_loaded_main_thread = True
    
    def _initialize_datalog_for_thread(self):
        """Initializes pyDatalog for the current thread if not already done."""
        # pyDatalog uses thread-local storage. We need to initialize it in each new thread.
        if not hasattr(Logic.tl, 'logic'):
             pyDatalog.create_terms('Logic')
             Logic.create_default_logic()
             # Load the globally stored rule definitions into the new thread's logic context
             if RulesEngine._loaded_datalog_definitions:
                 for rule_def in RulesEngine._loaded_datalog_definitions:
                     pyDatalog.load(rule_def)
                 logger.debug(f"Initialized pyDatalog for thread {threading.get_ident()} and loaded {len(RulesEngine._loaded_datalog_definitions)} rules.")

    def _load_and_store_datalog_rules(self, pg_session: Session):
        """Loads Datalog rules from DB and stores definitions globally."""
        cid = get_correlation_id()
        logger.info(f"[{cid}] Loading and storing Datalog rule definitions from database.")
        try:
            active_rules_orm: List[EDIFilter] = pg_session.query(EDIFilter).filter(
                EDIFilter.rule_type == 'DATALOG', EDIFilter.is_active == True, EDIFilter.is_latest_version == True
            ).all()

            if not active_rules_orm:
                logger.warning(f"[{cid}] No active Datalog rules found in edi.filters table.")
                return

            # Store rule definitions in the class variable to be loaded by each thread
            RulesEngine._loaded_datalog_definitions = []
            for rule_orm_obj in active_rules_orm:
                datalog_statements = [
                    line.strip() for line in rule_orm_obj.rule_definition.splitlines() 
                    if line.strip() and not line.strip().startswith('%')
                ]
                RulesEngine._loaded_datalog_definitions.extend(datalog_statements)
            
            logger.info(f"[{cid}] Stored {len(RulesEngine._loaded_datalog_definitions)} Datalog rule statements to be used by worker threads.")

        except SQLAlchemyError as e_db:
            logger.error(f"[{cid}] Database error loading Datalog rules: {e_db}", exc_info=True)
            raise StagingDBError(f"Failed to load Datalog rules from DB: {e_db}", original_exception=e_db)

    def _assert_claim_facts(self, claim: StagingClaim, pg_read_session: Session):
        """Asserts facts about a claim into the Datalog engine for the current thread."""
        cid = get_correlation_id()
        claim_id_str = str(claim.claim_id)
        
        pyDatalog.assert_fact('claim', claim_id_str)
        if claim.facility_id: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'facility_id', str(claim.facility_id))
        if claim.financial_class_id: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'financial_class_id', str(claim.financial_class_id))
        if claim.total_charge_amount is not None: pyDatalog.assert_fact('claim_attribute', claim_id_str, 'total_charge_amount', float(claim.total_charge_amount))
        # Add other assertions...
        
        if claim.facility_id:
            facility_master = pg_read_session.query(EDIFacility.active).filter(EDIFacility.facility_id == claim.facility_id).first()
            if facility_master:
                pyDatalog.assert_fact('master_facility_exists', str(claim.facility_id))
                if facility_master.active: pyDatalog.assert_fact('master_facility_active', str(claim.facility_id))

    def _retract_claim_facts(self, claim: StagingClaim):
        """Retracts facts specific to a claim to clean the Datalog engine for the current thread."""
        V = pyDatalogVar() # Correctly use the imported Var
        claim_id_str = str(claim.claim_id)
        
        pyDatalog.retract_fact('claim', claim_id_str)
        pyDatalog.retract_fact('claim_attribute', claim_id_str, V, V)
        pyDatalog.retract_fact('master_facility_exists', claim_id_str)
        pyDatalog.retract_fact('master_facility_active', claim_id_str)
        # Add other retractions...

    def validate_claim(self, pg_validation_session: Session, claim_orm: StagingClaim) -> ClaimValidationResult:
        """Validates a single claim using both Python checks and Datalog rules."""
        cid = get_correlation_id()
        claim_id_str = str(claim_orm.claim_id)
        logger.info(f"[{cid}] Starting comprehensive validation for Claim ID: {claim_id_str}")
        
        validation_result = ClaimValidationResult(claim_id=claim_id_str)
        
        if claim_orm.validation_errors is None:
            claim_orm.validation_errors = []

        try:
            python_rules_passed = perform_direct_staging_claim_validations(pg_validation_session, claim_orm)
            validation_result.update_from_python_validation(python_rules_passed, claim_orm)
        except Exception as e:
            logger.error(f"[{cid}] Unhandled error in Python validation phase for {claim_id_str}: {e}", exc_info=True)
            validation_result.add_error("PYTHON_VALIDATION_ERR", "System", str(e), "PYTHON")
        
        # Datalog Validation
        try:
            # Initialize Datalog engine for the current thread context
            self._initialize_datalog_for_thread()
            
            self._assert_claim_facts(claim_orm, pg_validation_session)

            X, R, F, M = pyDatalogVar(), pyDatalogVar(), pyDatalogVar(), pyDatalogVar()
            datalog_errors = pyDatalog.ask(f'validation_error("{claim_id_str}", R, F, M)')
            if datalog_errors:
                for rule_id, field, message in datalog_errors:
                    validation_result.add_error(str(rule_id), str(field), str(message), "DATALOG")
            
            self._retract_claim_facts(claim_orm)
        except Exception as e_datalog:
            logger.error(f"[{cid}] Error during Datalog rule execution for claim {claim_id_str}: {e_datalog}", exc_info=True)
            validation_result.add_error("DATALOG_ENGINE_ERROR", "DatalogExecution", f"Datalog engine error: {e_datalog}", "SYSTEM")

        # Finalize status
        if validation_result.is_overall_valid:
            validation_result.final_status_staging = STATUS_VALIDATION_PASSED
            logger.info(f"[{cid}] Claim ID: {claim_id_str} PASSED all validations.")
        else:
            validation_result.final_status_staging = STATUS_VALIDATION_FAILED_RULES
            logger.warning(f"[{cid}] Claim ID: {claim_id_str} FAILED validation.")
        
        return validation_result
