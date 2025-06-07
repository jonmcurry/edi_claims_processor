# app/database/postgres_handler.py
"""
Handles all database interactions with the PostgreSQL staging and metrics database.
Uses SQLAlchemy ORM and sessions from connection_manager.py.
Includes direct validation logic for staging claims.
"""
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, date, timedelta
from sqlalchemy import func, update, text
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import StagingDBError
from app.database.models.postgres_models import (
    StagingClaim, StagingCMS1500Diagnosis, StagingCMS1500LineItem,
    StagingValidationResult, StagingProcessingBatch, StagingClaimBatch,
    StagingFacilitiesCache, StagingDepartmentsCache, StagingFinancialClassesCache, StagingStandardPayersCache,
    Facility as EDIFacility, FinancialClass as EDIFinancialClass,
    Organization as EDIOrganization, Region as EDIRegion, StandardPayer as EDIStandardPayer,
    ClinicalDepartment as EDIClinicalDepartment, Filter as EDIFilter
)

logger = get_logger('app.database.postgres_handler')

# --- Claim Ingestion and Staging ---

def add_parsed_claims_to_staging(pg_session: Session, orm_claims: List[StagingClaim]) -> Tuple[int, List[str]]:
    """
    Adds a list of parsed StagingClaim ORM objects to the PostgreSQL staging database.
    """
    if not orm_claims:
        return 0, []

    cid = get_correlation_id()
    logger.info(f"[{cid}] Attempting to add {len(orm_claims)} claims to staging.")
    
    inserted_count = 0
    skipped_claim_ids = []
    
    for claim_orm in orm_claims:
        try:
            pg_session.add(claim_orm)
            inserted_count += 1
        except SQLAlchemyError as e:
            if "duplicate key value" in str(e).lower():
                logger.warning(f"[{cid}] Claim ID {claim_orm.claim_id} already exists in staging. Skipping.")
                skipped_claim_ids.append(claim_orm.claim_id)
                pg_session.rollback() # Rollback the failed add
            else:
                logger.error(f"[{cid}] SQLAlchemyError adding claim {claim_orm.claim_id}: {e}", exc_info=True)
                skipped_claim_ids.append(claim_orm.claim_id)
                pg_session.rollback()
        except Exception as e_gen:
            logger.error(f"[{cid}] Unexpected error adding claim {claim_orm.claim_id}: {e_gen}", exc_info=True)
            skipped_claim_ids.append(claim_orm.claim_id)
            pg_session.rollback()

    if inserted_count > 0:
        try:
            pg_session.commit()
        except SQLAlchemyError:
             logger.error(f"[{cid}] Commit failed after adding claims. Some claims might not be saved.", exc_info=True)
             pg_session.rollback()

    return inserted_count, skipped_claim_ids


def get_pending_claims_for_processing(pg_session: Session, batch_size: int, status: str = 'PENDING') -> List[StagingClaim]:
    """
    Fetches a batch of claims from staging that are pending processing.
    This function is now atomic and safe for concurrent workers. It selects claims,
    locks them, and updates their status to 'PROCESSING' in a single transaction.
    """
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Attempting to fetch and lock up to {batch_size} claims with status '{status}'.")
    
    try:
        # Step 1: Select and lock claim_ids to be processed.
        # FOR UPDATE SKIP LOCKED ensures that different workers don't grab the same rows.
        claim_ids_to_process_stmt = text(f"""
            SELECT claim_id FROM staging.claims
            WHERE processing_status = :status
            ORDER BY created_date
            LIMIT :batch_size
            FOR UPDATE SKIP LOCKED
        """)
        
        # This needs to be in a transaction
        with pg_session.begin_nested():
            result_proxy = pg_session.execute(
                claim_ids_to_process_stmt,
                {"status": status, "batch_size": batch_size}
            )
            claim_ids = [row[0] for row in result_proxy]

        if not claim_ids:
            logger.debug(f"[{cid}] No claims with status '{status}' found to process at this moment.")
            return []

        # Step 2: Update the status of these selected claims to 'PROCESSING'.
        update_stmt = (
            update(StagingClaim)
            .where(StagingClaim.claim_id.in_(claim_ids))
            .values(processing_status='PROCESSING', updated_date=datetime.utcnow())
            .execution_options(synchronize_session=False) # Important for performance
        )
        pg_session.execute(update_stmt)

        # Step 3: Fetch the full ORM objects for the claims that were just updated.
        claims = (
            pg_session.query(StagingClaim)
            .options(
                selectinload(StagingClaim.cms1500_diagnoses),
                selectinload(StagingClaim.cms1500_line_items)
            )
            .filter(StagingClaim.claim_id.in_(claim_ids))
            .all()
        )
        
        # The main transaction will be committed by the calling worker, making the status change permanent.
        logger.info(f"[{cid}] Fetched and locked {len(claims)} claims for processing.")
        return claims

    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error fetching pending claims: {e}", exc_info=True)
        # The session's transaction will be rolled back by the context manager in the calling function
        raise StagingDBError(f"Failed to fetch pending claims: {e}", original_exception=e)


def update_staging_claim_status(pg_session: Session, claim_id: str, new_status: str, error_messages: Optional[List[str]] = None):
    """Updates the processing status of a claim in the staging table."""
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Updating status for claim {claim_id} to '{new_status}'.")
    try:
        stmt_values = {
            "processing_status": new_status,
            "updated_date": datetime.utcnow()
        }
        if error_messages is not None:
            stmt_values["validation_errors"] = error_messages
        
        if new_status == "COMPLETED_EXPORTED_TO_PROD":
            stmt_values["exported_to_production"] = True
            stmt_values["export_date"] = datetime.utcnow()

        stmt = update(StagingClaim).where(StagingClaim.claim_id == claim_id).values(**stmt_values)
        result = pg_session.execute(stmt)
        
        if result.rowcount == 0:
            logger.warning(f"[{cid}] Claim {claim_id} not found for status update to '{new_status}'.")
            return False
        logger.info(f"[{cid}] Status for claim {claim_id} updated to '{new_status}'.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error updating status for claim {claim_id}: {e}", exc_info=True)
        raise StagingDBError(f"Failed to update status for claim {claim_id}: {e}", original_exception=e)

def save_validation_results(pg_session: Session, validation_result_orm: StagingValidationResult):
    """Saves or updates a StagingValidationResult entry."""
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Saving validation results for claim {validation_result_orm.claim_id}.")
    try:
        pg_session.merge(validation_result_orm)
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error saving validation results for claim {validation_result_orm.claim_id}: {e}", exc_info=True)
        raise StagingDBError(f"Failed to save validation results for claim {validation_result_orm.claim_id}: {e}", original_exception=e)

# --- Direct Staging Claim Validations ---
def perform_direct_staging_claim_validations(pg_session: Session, claim: StagingClaim) -> bool:
    """
    Performs a set of direct validation rules on a StagingClaim object.
    """
    cid = get_correlation_id()
    claim_id = claim.claim_id
    logger.info(f"[{cid}] Performing direct validations for claim: {claim_id}")
    
    errors_found = []
    is_overall_valid = True

    if claim.validation_errors is None:
        claim.validation_errors = []

    # 1. Facility ID
    claim.facility_validated = False
    if not claim.facility_id:
        errors_found.append({"rule": "CORE_FAC_01", "field": "facility_id", "message": "Facility ID is missing."})
        is_overall_valid = False
    else:
        facility = pg_session.query(EDIFacility).filter(EDIFacility.facility_id == claim.facility_id, EDIFacility.active == True).first()
        if not facility:
            errors_found.append({"rule": "CORE_FAC_02", "field": "facility_id", "message": f"Facility ID '{claim.facility_id}' not found or is inactive."})
            is_overall_valid = False
        else:
            claim.facility_validated = True

    # ... (other validation rules remain the same) ...
    # 2. Patient Account Number validation
    if not claim.patient_account_number or not claim.patient_account_number.strip():
        errors_found.append({"rule": "CORE_PAT_01", "field": "patient_account_number", "message": "Patient Account Number is missing or empty."})
        is_overall_valid = False

    # 3. Service Date validation
    if not claim.service_date:
        errors_found.append({"rule": "CORE_DATE_01", "field": "service_date", "message": "Claim service_date is missing."})
        is_overall_valid = False
    elif claim.service_date > date.today():
        errors_found.append({"rule": "CORE_DATE_02", "field": "service_date", "message": "Claim service_date cannot be in the future."})
        is_overall_valid = False

    # 4. Financial Class validation
    claim.financial_class_validated = False
    if not claim.financial_class_id:
        errors_found.append({"rule": "CORE_FC_01", "field": "financial_class_id", "message": "Financial Class ID is missing."})
        is_overall_valid = False
    elif claim.facility_validated: # Only check if facility is valid
        financial_class = pg_session.query(EDIFinancialClass).filter(
            EDIFinancialClass.financial_class_id == claim.financial_class_id,
            EDIFinancialClass.active == True,
            (EDIFinancialClass.facility_id.in_([claim.facility_id, None]))
        ).first()
        if not financial_class:
            errors_found.append({"rule": "CORE_FC_03", "field": "financial_class_id", 
                                 "message": f"Financial Class '{claim.financial_class_id}' not valid for facility '{claim.facility_id}'."})
            is_overall_valid = False
        else:
            claim.financial_class_validated = True
    
    for err in errors_found:
        error_string = f"{err['rule']}|{err['field']}|{err['message']}"
        if error_string not in claim.validation_errors:
            claim.validation_errors.append(error_string)
            
    if not is_overall_valid:
        logger.warning(f"[{cid}] Claim {claim_id} failed direct validations: {len(errors_found)} errors.")

    return is_overall_valid


# --- Batch Processing Records ---
def create_db_processing_batch(pg_session: Session, batch_name: str, total_claims: int = 0) -> int:
    cid = get_correlation_id()
    logger.info(f"[{cid}] Creating new processing batch record: {batch_name}")
    try:
        new_batch = StagingProcessingBatch(batch_name=batch_name, status='PENDING', total_claims=total_claims, start_time=datetime.utcnow())
        pg_session.add(new_batch)
        pg_session.flush()
        return new_batch.batch_id
    except SQLAlchemyError as e:
        raise StagingDBError(f"Failed to create processing batch: {e}", original_exception=e)

# Other functions (assign_claims_to_db_batch, update_db_batch_processing_status, etc.) remain unchanged.

