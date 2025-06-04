# app/database/sqlserver_handler.py
"""
Handles all database interactions with the SQL Server production database.
Uses SQLAlchemy ORM and sessions from connection_manager.py.
"""
from typing import List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import ProductionDBError, handle_exception
from app.database.models.sqlserver_models import (
    ProductionClaim, ProductionCMS1500Diagnosis, ProductionCMS1500LineItem,
    FailedClaimDetail,
    ProductionFacility, ProductionOrganization # etc. for master data if managed here
)

logger = get_logger('app.database.sqlserver_handler')

def save_claim_to_production(sql_session: Session, production_claim_orm: ProductionClaim) -> bool:
    """
    Saves a validated ProductionClaim ORM object (with its related diagnoses and line items)
    to the SQL Server production database.
    This assumes production_claim_orm is fully populated.
    """
    cid = get_correlation_id()
    logger.info(f"[{cid}] Attempting to save claim {production_claim_orm.ClaimId} to production SQL Server DB.")
    try:
        # Check if claim already exists to prevent duplicates, or rely on PK constraint
        # For simplicity, assuming new claims or that ClaimId is unique and will raise error if violated.
        # A more robust approach might use MERGE or check existence first if updates are possible.
        existing_claim = sql_session.query(ProductionClaim.ClaimId).filter_by(ClaimId=production_claim_orm.ClaimId).scalar()
        if existing_claim:
            logger.warning(f"[{cid}] Claim {production_claim_orm.ClaimId} already exists in production. Skipping insert.")
            # Potentially update logic here if re-processing can update existing prod claims.
            return False # Indicate skipped

        sql_session.add(production_claim_orm)
        # sql_session.commit() # Caller should commit the transaction
        logger.info(f"[{cid}] Claim {production_claim_orm.ClaimId} added to session for production DB. Commit pending by caller.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] SQLAlchemyError saving claim {production_claim_orm.ClaimId} to production: {e}", exc_info=True)
        # sql_session.rollback() # Caller should manage transaction
        raise ProductionDBError(f"Failed to save claim {production_claim_orm.ClaimId} to production: {e}", original_exception=e)
    except Exception as e_gen:
        logger.error(f"[{cid}] Unexpected error saving claim {production_claim_orm.ClaimId} to production: {e_gen}", exc_info=True)
        raise ProductionDBError(f"Unexpected error saving claim {production_claim_orm.ClaimId} to production: {e_gen}", original_exception=e_gen)


def save_failed_claim_detail(sql_session: Session, failed_claim_detail_orm: FailedClaimDetail) -> bool:
    """
    Saves details of a failed claim to the FailedClaimDetails table in SQL Server.
    """
    cid = get_correlation_id()
    original_claim_id = failed_claim_detail_orm.OriginalClaimId
    logger.info(f"[{cid}] Attempting to save failed claim detail for OriginalClaimId {original_claim_id} to SQL Server.")
    try:
        sql_session.add(failed_claim_detail_orm)
        # sql_session.commit() # Caller should commit
        logger.info(f"[{cid}] Failed claim detail for {original_claim_id} added to session. Commit pending by caller.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] SQLAlchemyError saving failed claim detail for {original_claim_id}: {e}", exc_info=True)
        # sql_session.rollback() # Caller should manage transaction
        raise ProductionDBError(f"Failed to save failed claim detail for {original_claim_id}: {e}", original_exception=e)
    except Exception as e_gen:
        logger.error(f"[{cid}] Unexpected error saving failed claim detail for {original_claim_id}: {e_gen}", exc_info=True)
        raise ProductionDBError(f"Unexpected error saving failed claim detail for {original_claim_id}: {e_gen}", original_exception=e_gen)


# --- Functions for Failed Claims UI (examples) ---

def get_failed_claims_summary(sql_session: Session, limit: int = 100, offset: int = 0, status: Optional[str] = None) -> List[FailedClaimDetail]:
    """Fetches a summary of failed claims, e.g., for a UI."""
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching failed claims summary: limit={limit}, offset={offset}, status={status}")
    try:
        query = sql_session.query(FailedClaimDetail)
        if status:
            query = query.filter(FailedClaimDetail.Status == status)
        
        claims = query.order_by(FailedClaimDetail.FailureTimestamp.desc())\
                      .offset(offset)\
                      .limit(limit)\
                      .all()
        logger.info(f"[{cid}] Fetched {len(claims)} failed claims.")
        return claims
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error fetching failed claims summary: {e}", exc_info=True)
        raise ProductionDBError(f"Failed to fetch failed claims summary: {e}", original_exception=e)

def get_failed_claim_by_id(sql_session: Session, failed_claim_detail_id: int) -> Optional[FailedClaimDetail]:
    """Fetches a specific failed claim by its FailedClaimDetailId."""
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching failed claim detail by ID: {failed_claim_detail_id}")
    try:
        return sql_session.query(FailedClaimDetail).filter_by(FailedClaimDetailId=failed_claim_detail_id).first()
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error fetching failed claim {failed_claim_detail_id}: {e}", exc_info=True)
        raise ProductionDBError(f"Failed to fetch failed claim {failed_claim_detail_id}: {e}", original_exception=e)

def update_failed_claim_status(sql_session: Session, failed_claim_detail_id: int, new_status: str, resolution_notes: Optional[str] = None, resolved_by: Optional[str] = None) -> bool:
    """Updates the status and resolution details of a failed claim."""
    cid = get_correlation_id()
    logger.info(f"[{cid}] Updating status of failed claim {failed_claim_detail_id} to '{new_status}'.")
    try:
        failed_claim = sql_session.query(FailedClaimDetail).filter_by(FailedClaimDetailId=failed_claim_detail_id).first()
        if not failed_claim:
            logger.warning(f"[{cid}] FailedClaimDetailId {failed_claim_detail_id} not found for update.")
            return False
        
        failed_claim.Status = new_status
        failed_claim.UpdatedDate = datetime.utcnow() # func.now() for DB time
        if resolution_notes is not None: # Allow clearing notes by passing empty string
            failed_claim.ResolutionNotes = resolution_notes
        if resolved_by:
            failed_claim.ResolvedBy = resolved_by
        if new_status.startswith("Resolved"): # Or specific resolved statuses
            failed_claim.ResolvedTimestamp = datetime.utcnow()
        
        # sql_session.commit() # Caller should commit
        logger.info(f"[{cid}] Failed claim {failed_claim_detail_id} status updated to '{new_status}'. Commit pending.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error updating status for failed claim {failed_claim_detail_id}: {e}", exc_info=True)
        # sql_session.rollback()
        raise ProductionDBError(f"Failed to update status for failed claim {failed_claim_detail_id}: {e}", original_exception=e)


# --- Master Data Management (dbo schema, if this handler is responsible) ---
# Example:
def get_production_facility_details(sql_session: Session, facility_id: str) -> Optional[ProductionFacility]:
    """Fetches facility details from dbo.Facilities in SQL Server."""
    try:
        return sql_session.query(ProductionFacility).filter_by(FacilityId=facility_id).first()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching Production facility {facility_id}: {e}", exc_info=True)
        raise ProductionDBError(f"Failed to fetch Production facility {facility_id}: {e}", original_exception=e)


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, get_sqlserver_session, dispose_engines
    from app.database.models.sqlserver_models import Base as SQLServerBase # For table creation
    from decimal import Decimal

    setup_logging()
    set_correlation_id("SQL_HANDLER_TEST")
    sql_session = None
    try:
        init_database_connections()
        sql_session = get_sqlserver_session()

        # Ensure tables exist
        SQLServerBase.metadata.create_all(sql_session.get_bind())
        sql_session.commit()

        logger.info("--- Testing SQL Server Handler ---")

        # Test save_claim_to_production
        mock_prod_claim = ProductionClaim(
            ClaimId="SQL_TEST_PROD001", FacilityId="FAC_SQL01", PatientId="PATSQL01",
            ServiceDate=datetime.now().date(), TotalChargeAmount=Decimal("300.00"),
            ProcessingStatus="COMPLETED_VALID", ValidationStatus="TEST_VALIDATED"
        )
        # Add mock line items and diagnoses if needed for a complete test
        line = ProductionCMS1500LineItem(LineNumber=1, CptCode="99213", LineChargeAmount=Decimal("150.00"))
        diag = ProductionCMS1500Diagnosis(DiagnosisSequence=1, IcdCode="R07.9", IsPrimary=True)
        mock_prod_claim.cms1500_line_items.append(line)
        mock_prod_claim.cms1500_diagnoses.append(diag)

        save_success = save_claim_to_production(sql_session, mock_prod_claim)
        sql_session.commit() # Commit this test
        logger.info(f"Save to production test for {mock_prod_claim.ClaimId}: Success={save_success}")
        if save_success:
            logger.info(f"Claim {mock_prod_claim.ClaimId} should be in production DB.")

        # Test save_failed_claim_detail
        mock_failed_detail = FailedClaimDetail(
            OriginalClaimId="SQL_TEST_FAIL001", StagingClaimId="SQL_TEST_FAIL001_STG",
            FacilityId="FAC_SQL02", ProcessingStage="ValidationRulesEngine",
            ErrorMessages="Rule VAL_FAC_001 failed: Facility ID not found.\nRule VAL_PAT_001 failed: Patient Account missing.",
            ErrorCodes="VAL_FAC_001, VAL_PAT_001", Status="New"
        )
        save_failed_success = save_failed_claim_detail(sql_session, mock_failed_detail)
        sql_session.commit() # Commit this test
        logger.info(f"Save failed claim detail test for {mock_failed_detail.OriginalClaimId}: Success={save_failed_success}")

        # Test get_failed_claims_summary
        failed_claims = get_failed_claims_summary(sql_session, limit=5, status="New")
        logger.info(f"Fetched {len(failed_claims)} 'New' failed claims.")
        for fc in failed_claims:
            logger.info(f"  Failed Claim ID: {fc.OriginalClaimId}, Stage: {fc.ProcessingStage}, Errors: {fc.ErrorMessages[:50]}...")
            # Test update_failed_claim_status on the first one
            if fc.FailedClaimDetailId:
                update_failed_claim_status(sql_session, fc.FailedClaimDetailId, "Pending Review", resolved_by="TestSystem")
                sql_session.commit()
                updated_fc = get_failed_claim_by_id(sql_session, fc.FailedClaimDetailId)
                logger.info(f"  Updated status for {fc.OriginalClaimId} to {updated_fc.Status if updated_fc else 'Not Found'}")
                break # Only update one for test

    except Exception as e:
        logger.critical(f"Error in SQL Server Handler test script: {e}", exc_info=True)
        if sql_session and sql_session.is_active:
            sql_session.rollback()
    finally:
        if sql_session:
            sql_session.close()
        dispose_engines()
        logger.info("SQL Server Handler test script finished.")