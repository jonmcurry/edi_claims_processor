# app/database/postgres_handler.py
"""
Handles all database interactions with the PostgreSQL staging and metrics database.
Uses SQLAlchemy ORM and sessions from connection_manager.py.
"""
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime
from sqlalchemy import func, update, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert as pg_insert # For ON CONFLICT DO UPDATE/NOTHING

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import StagingDBError, handle_exception
from app.database.models.postgres_models import (
    StagingClaim, StagingCMS1500Diagnosis, StagingCMS1500LineItem,
    StagingValidationResult, StagingProcessingBatch, StagingClaimBatch,
    StagingFacilitiesCache, StagingDepartmentsCache, StagingFinancialClassesCache, StagingStandardPayersCache,
    Facility as EDIFacility, FinancialClass as EDIFinancialClass, # From edi schema
    Organization as EDIOrganization, Region as EDIRegion, StandardPayer as EDIStandardPayer,
    ClinicalDepartment as EDIClinicalDepartment, Filter as EDIFilter
)

logger = get_logger('app.database.postgres_handler')

# --- Claim Ingestion and Staging ---

def add_parsed_claims_to_staging(pg_session: Session, orm_claims: List[StagingClaim]) -> Tuple[int, List[str]]:
    """
    Adds a list of parsed StagingClaim ORM objects to the PostgreSQL staging database.
    Uses ON CONFLICT DO NOTHING to avoid errors if a claim_id already exists (idempotency).
    
    Args:
        pg_session: SQLAlchemy session for PostgreSQL.
        orm_claims: A list of StagingClaim ORM objects.

    Returns:
        Tuple containing (number of claims successfully inserted, list of duplicate/skipped claim_ids).
    """
    if not orm_claims:
        return 0, []

    cid = get_correlation_id()
    logger.info(f"[{cid}] Attempting to add/update {len(orm_claims)} claims to staging.")
    
    inserted_count = 0
    skipped_claim_ids = []
    
    # For PostgreSQL, ON CONFLICT DO NOTHING is efficient for bulk inserts.
    # However, SQLAlchemy's ORM add_all doesn't directly support ON CONFLICT for the main object
    # in a simple way that also handles cascades for related objects (diagnoses, line_items).
    # A common approach is to check existence or use a more direct SQL approach for the main claim,
    # or handle IntegrityError.

    # Approach 1: Check existence for each claim (less efficient for very large batches but ORM-friendly)
    # Approach 2: Use pg_insert for StagingClaim, then add related objects (more complex with cascades)
    # Approach 3: Try adding and catch IntegrityError (can be slow if many duplicates)

    # Let's go with a hybrid: pg_insert for main claim, then handle related objects.
    # This is more complex than simple add_all if relationships are deeply nested and need to be updated.

    # Simpler approach for now: try to add and log duplicates.
    # For high performance, raw SQL with psycopg2's execute_values and ON CONFLICT might be better
    # for the StagingClaim table itself, then handling child tables.

    for claim_orm in orm_claims:
        try:
            # Check if claim_id already exists
            existing_claim = pg_session.query(StagingClaim.claim_id).filter_by(claim_id=claim_orm.claim_id).scalar()
            if existing_claim:
                logger.warning(f"[{cid}] Claim ID {claim_orm.claim_id} already exists in staging. Skipping.")
                skipped_claim_ids.append(claim_orm.claim_id)
                # Optionally, update the existing claim if re-parsing is intended to overwrite
                # For now, we skip.
                continue

            pg_session.add(claim_orm)
            # We will commit in a batch outside this loop or by the caller.
            inserted_count += 1
        except SQLAlchemyError as e:
            # This specific error handling might be too granular here if the caller handles commit
            logger.error(f"[{cid}] SQLAlchemyError adding claim {claim_orm.claim_id}: {e}", exc_info=True)
            skipped_claim_ids.append(claim_orm.claim_id) # Treat as skipped due to error
            # No rollback here, caller should manage transaction
        except Exception as e_gen:
            logger.error(f"[{cid}] Unexpected error adding claim {claim_orm.claim_id}: {e_gen}", exc_info=True)
            skipped_claim_ids.append(claim_orm.claim_id)


    # Caller should handle commit. If this function is responsible for commit:
    # try:
    #     pg_session.commit()
    #     logger.info(f"[{cid}] Committed {inserted_count} new claims to staging.")
    # except Exception as e_commit:
    #     pg_session.rollback()
    #     logger.error(f"[{cid}] Error committing claims to staging: {e_commit}", exc_info=True)
    #     # All claims in this batch would effectively be skipped if commit fails
    #     return 0, [c.claim_id for c in orm_claims]

    if skipped_claim_ids:
         logger.warning(f"[{cid}] Skipped {len(skipped_claim_ids)} claims due to existence or error: {skipped_claim_ids[:5]}...") # Log first 5

    return inserted_count, skipped_claim_ids


def get_pending_claims_for_processing(pg_session: Session, batch_size: int, status: str = 'PARSED') -> List[StagingClaim]:
    """
    Fetches a batch of claims from staging that are pending processing.
    Optionally, could also lock these rows (SELECT ... FOR UPDATE SKIP LOCKED) for concurrent processing.
    """
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching up to {batch_size} claims with status '{status}' for processing.")
    try:
        claims = pg_session.query(StagingClaim)\
            .filter(StagingClaim.processing_status == status)\
            .order_by(StagingClaim.created_date) # Process older claims first
            .limit(batch_size)\
            .all()
            # For row locking in a concurrent environment:
            # .with_for_update(skip_locked=True)\ 
        logger.info(f"[{cid}] Fetched {len(claims)} claims for processing.")
        return claims
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error fetching pending claims: {e}", exc_info=True)
        raise StagingDBError(f"Failed to fetch pending claims: {e}", original_exception=e)


def update_staging_claim_status(pg_session: Session, claim_id: str, new_status: str, error_messages: Optional[List[str]] = None):
    """Updates the processing status of a claim in the staging table."""
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Updating status for claim {claim_id} to '{new_status}'.")
    try:
        stmt = update(StagingClaim).where(StagingClaim.claim_id == claim_id).values(
            processing_status=new_status,
            updated_date=datetime.utcnow() # func.now() for database time
        )
        if error_messages: # Assuming StagingClaim has a field for validation_errors (it does as ARRAY(Text))
            stmt = stmt.values(validation_errors=error_messages) # This overwrites existing errors
            # To append, you'd need more complex logic or raw SQL if using ARRAY type directly.
            # For JSONB errors: stmt = stmt.values(validation_errors_json=jsonable_encoder(error_messages))

        result = pg_session.execute(stmt)
        # pg_session.commit() # Caller should commit
        if result.rowcount == 0:
            logger.warning(f"[{cid}] Claim {claim_id} not found for status update to '{new_status}'.")
            return False
        logger.info(f"[{cid}] Status for claim {claim_id} updated to '{new_status}'.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error updating status for claim {claim_id}: {e}", exc_info=True)
        # pg_session.rollback() # Caller should manage transaction
        raise StagingDBError(f"Failed to update status for claim {claim_id}: {e}", original_exception=e)

def save_validation_results(pg_session: Session, validation_result_orm: StagingValidationResult):
    """Saves or updates a StagingValidationResult entry."""
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Saving validation results for claim {validation_result_orm.claim_id}.")
    try:
        # Upsert logic: if exists, update; otherwise, insert.
        # Using merge for simplicity, though pg_insert with on_conflict_do_update is more explicit for PG.
        pg_session.merge(validation_result_orm)
        # pg_session.commit() # Caller should commit
        logger.info(f"[{cid}] Validation results saved for claim {validation_result_orm.claim_id}.")
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error saving validation results for claim {validation_result_orm.claim_id}: {e}", exc_info=True)
        # pg_session.rollback()
        raise StagingDBError(f"Failed to save validation results for claim {validation_result_orm.claim_id}: {e}", original_exception=e)

# --- Batch Processing Records ---

def create_db_processing_batch(pg_session: Session, batch_name: str, total_claims: int = 0) -> int:
    """Creates a new record in staging.processing_batches."""
    cid = get_correlation_id()
    logger.info(f"[{cid}] Creating new processing batch record: {batch_name}")
    try:
        new_batch = StagingProcessingBatch(
            batch_name=batch_name,
            status='PENDING',
            total_claims=total_claims,
            start_time=datetime.utcnow()
        )
        pg_session.add(new_batch)
        pg_session.flush() # To get the batch_id
        # pg_session.commit() # Caller should commit
        logger.info(f"[{cid}] Created processing batch '{batch_name}' with ID {new_batch.batch_id}.")
        return new_batch.batch_id
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error creating processing batch record: {e}", exc_info=True)
        # pg_session.rollback()
        raise StagingDBError(f"Failed to create processing batch: {e}", original_exception=e)

def assign_claims_to_db_batch(pg_session: Session, batch_id: int, claim_ids: List[str]):
    """Assigns a list of claim_ids to a specific batch_id in staging.claim_batches."""
    cid = get_correlation_id()
    if not claim_ids:
        logger.info(f"[{cid}] No claim IDs provided to assign to batch {batch_id}.")
        return
    logger.info(f"[{cid}] Assigning {len(claim_ids)} claims to batch {batch_id}.")
    try:
        assignments = [StagingClaimBatch(claim_id=cid_val, batch_id=batch_id) for cid_val in claim_ids]
        pg_session.bulk_save_objects(assignments)
        # pg_session.commit() # Caller should commit
        logger.info(f"[{cid}] Successfully assigned {len(claim_ids)} claims to batch {batch_id}.")
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error assigning claims to batch {batch_id}: {e}", exc_info=True)
        # pg_session.rollback()
        raise StagingDBError(f"Failed to assign claims to batch {batch_id}: {e}", original_exception=e)

def update_db_batch_processing_status(pg_session: Session, batch_id: int, status: str, processed_claims: int = None, successful_claims: int = None, failed_claims: int = None):
    """Updates the status and counts of a processing batch."""
    cid = get_correlation_id()
    logger.info(f"[{cid}] Updating batch {batch_id} to status '{status}'.")
    try:
        values_to_update = {"status": status, "updated_date": datetime.utcnow()} # Assuming StagingProcessingBatch has updated_date
        if status.upper() == 'COMPLETED' or status.upper() == 'FAILED':
            values_to_update["end_time"] = datetime.utcnow()
        if processed_claims is not None:
            values_to_update["processed_claims"] = processed_claims
        if successful_claims is not None:
            values_to_update["successful_claims"] = successful_claims
        if failed_claims is not None:
            values_to_update["failed_claims"] = failed_claims
            
        stmt = update(StagingProcessingBatch).where(StagingProcessingBatch.batch_id == batch_id).values(**values_to_update)
        result = pg_session.execute(stmt)
        # pg_session.commit() # Caller should commit
        if result.rowcount == 0:
            logger.warning(f"[{cid}] Batch {batch_id} not found for status update.")
        else:
            logger.info(f"[{cid}] Batch {batch_id} status updated to '{status}'.")
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error updating batch {batch_id} status: {e}", exc_info=True)
        # pg_session.rollback()
        raise StagingDBError(f"Failed to update batch {batch_id} status: {e}", original_exception=e)


# --- Reference Data Cache Management (Example) ---
def refresh_facilities_cache(pg_session: Session, facilities_data: List[Dict]):
    """
    Refreshes the staging.facilities_cache table.
    Deletes existing data and inserts new data.
    Args:
        pg_session: SQLAlchemy session.
        facilities_data: List of dictionaries, each representing a facility.
                         Keys should match StagingFacilitiesCache columns.
    """
    cid = get_correlation_id()
    logger.info(f"[{cid}] Refreshing facilities_cache with {len(facilities_data)} entries.")
    try:
        # Delete existing data
        pg_session.execute(text("DELETE FROM staging.facilities_cache")) # Or StagingFacilitiesCache.__table__.delete()
        
        # Bulk insert new data
        if facilities_data:
            # pg_session.bulk_insert_mappings(StagingFacilitiesCache, facilities_data)
            # More robust: handle potential key errors if dicts don't match model perfectly
            cache_objects = []
            for data in facilities_data:
                cache_objects.append(StagingFacilitiesCache(
                    facility_id=data.get('facility_id'),
                    facility_name=data.get('facility_name'),
                    facility_type=data.get('facility_type'),
                    critical_access=data.get('critical_access'),
                    organization_id=data.get('organization_id'),
                    region_id=data.get('region_id'),
                    active=data.get('active', True), # Default to True if missing
                    last_updated=datetime.utcnow()
                ))
            pg_session.add_all(cache_objects)

        # pg_session.commit() # Caller should commit
        logger.info(f"[{cid}] facilities_cache refreshed successfully.")
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error refreshing facilities_cache: {e}", exc_info=True)
        # pg_session.rollback()
        raise StagingDBError(f"Failed to refresh facilities_cache: {e}", original_exception=e)
    # Similar functions for departments_cache, financial_classes_cache, standard_payers_cache

# --- Master Data Access (edi schema) ---
def get_edi_facility_details(pg_session: Session, facility_id: str) -> Optional[EDIFacility]:
    """Fetches facility details from edi.facilities."""
    try:
        return pg_session.query(EDIFacility).filter_by(facility_id=facility_id).first()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching EDI facility {facility_id}: {e}", exc_info=True)
        raise StagingDBError(f"Failed to fetch EDI facility {facility_id}: {e}", original_exception=e)

def get_edi_financial_class_details(pg_session: Session, financial_class_id: str, facility_id: str) -> Optional[EDIFinancialClass]:
    """Fetches financial class details from edi.financial_classes for a specific facility."""
    try:
        return pg_session.query(EDIFinancialClass)\
            .filter_by(financial_class_id=financial_class_id, facility_id=facility_id)\
            .first()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching EDI financial class {financial_class_id} for facility {facility_id}: {e}", exc_info=True)
        raise StagingDBError(f"Failed to fetch EDI financial class: {e}", original_exception=e)

if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, get_postgres_session, dispose_engines
    from app.database.models.postgres_models import Base as PostgresBase

    setup_logging()
    set_correlation_id("PG_HANDLER_TEST")
    pg_session = None
    try:
        init_database_connections()
        pg_session = get_postgres_session()

        # Ensure tables exist
        PostgresBase.metadata.create_all(pg_session.get_bind())
        pg_session.commit()

        logger.info("--- Testing PostgreSQL Handler ---")

        # Test add_parsed_claims_to_staging
        mock_claim1_data = {
            "claim_id": "PG_TEST_C001", "facility_id": "FAC001", "patient_account_number": "ACC001",
            "service_date": datetime.now().date(), "total_charge_amount": Decimal("100.00"),
            "diagnoses": [{"diagnosis_sequence": 1, "icd_code": "R51", "is_primary": True}],
            "line_items": [{"line_number": 1, "cpt_code": "99213", "line_charge_amount": Decimal("100.00")}]
        }
        # Need to import map_parsed_data_to_orm from edi_parser or replicate its logic here for test
        # For simplicity, creating ORM object directly
        from app.processing.edi_parser import map_parsed_data_to_orm # Assuming it's available
        orm_claim1 = map_parsed_data_to_orm(mock_claim1_data)
        
        inserted, skipped = add_parsed_claims_to_staging(pg_session, [orm_claim1])
        pg_session.commit() # Commit this test batch
        logger.info(f"Add claims test: Inserted={inserted}, Skipped={len(skipped)}")
        if inserted > 0:
            logger.info(f"Claim {orm_claim1.claim_id} should be in staging.")
        
        # Test get_pending_claims_for_processing
        pending_claims = get_pending_claims_for_processing(pg_session, 5, status='PARSED')
        logger.info(f"Found {len(pending_claims)} pending claims with status PARSED.")
        if pending_claims:
            test_claim_id_for_update = pending_claims[0].claim_id
            # Test update_staging_claim_status
            update_staging_claim_status(pg_session, test_claim_id_for_update, "PROCESSING_STARTED")
            pg_session.commit()
            updated_claim = pg_session.query(StagingClaim).filter_by(claim_id=test_claim_id_for_update).first()
            logger.info(f"Claim {test_claim_id_for_update} status after update: {updated_claim.processing_status if updated_claim else 'Not Found'}")

        # Test batch record creation
        batch_id = create_db_processing_batch(pg_session, "Test Batch 001", total_claims=len(pending_claims))
        pg_session.commit()
        if pending_claims:
            assign_claims_to_db_batch(pg_session, batch_id, [c.claim_id for c in pending_claims])
            pg_session.commit()
        update_db_batch_processing_status(pg_session, batch_id, "COMPLETED", processed_claims=len(pending_claims), successful_claims=len(pending_claims))
        pg_session.commit()
        logger.info(f"Batch operations test completed for batch ID {batch_id}.")

    except Exception as e:
        logger.critical(f"Error in PostgreSQL Handler test script: {e}", exc_info=True)
        if pg_session and pg_session.is_active:
            pg_session.rollback()
    finally:
        if pg_session:
            pg_session.close()
        dispose_engines()
        logger.info("PostgreSQL Handler test script finished.")