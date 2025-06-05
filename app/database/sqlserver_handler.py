# app/database/sqlserver_handler.py
"""
Handles all database interactions with the SQL Server production database.
Uses SQLAlchemy ORM and sessions from connection_manager.py.
Enhanced with reimbursement integration, UI support for failed claims,
sync status indicators, and failure pattern analytics.
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta # Import date and timedelta
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text, desc, asc, func # For advanced querying

from app.utils.logging_config import get_logger, get_correlation_id, set_correlation_id
from app.utils.error_handler import ProductionDBError
from app.database.models.sqlserver_models import (
    ProductionClaim, ProductionCMS1500Diagnosis, ProductionCMS1500LineItem,
    FailedClaimDetail,
    ProductionFacility
)
# Assuming your ReimbursementCalculator produces results that can be mapped
# from app.processing.reimbursement_calculator import ReimbursementCalculator # Not directly used here, but data comes from it

logger = get_logger('app.database.sqlserver_handler')

def save_claim_to_production(sql_session: Session, production_claim_orm: ProductionClaim, 
                             line_item_reimbursements: Optional[Dict[int, float]] = None) -> bool:
    """
    Saves a validated ProductionClaim ORM object (with its related diagnoses and line items)
    to the SQL Server production database.
    This assumes production_claim_orm is fully populated.

    Args:
        sql_session: SQLAlchemy session for SQL Server.
        production_claim_orm: The ProductionClaim ORM object.
        line_item_reimbursements: Optional dictionary mapping line_number to its calculated reimbursement amount.
                                   Example: {1: 120.50, 2: 75.00}
                                   This data comes from the ReimbursementCalculator.
    """
    cid = get_correlation_id()
    logger.info(f"[{cid}] Attempting to save claim {production_claim_orm.ClaimId} to production SQL Server DB.")
    try:
        existing_claim = sql_session.query(ProductionClaim.ClaimId).filter_by(ClaimId=production_claim_orm.ClaimId).scalar()
        if existing_claim:
            logger.warning(f"[{cid}] Claim {production_claim_orm.ClaimId} already exists in production. Skipping insert.")
            return False 

        # Integrate reimbursement amounts into line items if provided
        # PREREQUISITE: ProductionCMS1500LineItem model and dbo.Cms1500LineItems table
        # MUST have a field like 'EstimatedReimbursementAmount'.
        if line_item_reimbursements:
            for line_item_orm in production_claim_orm.cms1500_line_items:
                if hasattr(line_item_orm, 'EstimatedReimbursementAmount'): # Check if the field exists
                    reimbursement_val = line_item_reimbursements.get(line_item_orm.LineNumber)
                    if reimbursement_val is not None:
                        line_item_orm.EstimatedReimbursementAmount = reimbursement_val
                else:
                    logger.warning(f"[{cid}] ProductionCMS1500LineItem model is missing 'EstimatedReimbursementAmount' field. "
                                   "Cannot store calculated reimbursement.")
                    # Break if one is missing, or log once per call. For now, log per line missing.

        sql_session.add(production_claim_orm)
        logger.info(f"[{cid}] Claim {production_claim_orm.ClaimId} added to session for production DB. Commit pending by caller.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] SQLAlchemyError saving claim {production_claim_orm.ClaimId} to production: {e}", exc_info=True)
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
        logger.info(f"[{cid}] Failed claim detail for {original_claim_id} added to session. Commit pending by caller.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] SQLAlchemyError saving failed claim detail for {original_claim_id}: {e}", exc_info=True)
        raise ProductionDBError(f"Failed to save failed claim detail for {original_claim_id}: {e}", original_exception=e)
    except Exception as e_gen:
        logger.error(f"[{cid}] Unexpected error saving failed claim detail for {original_claim_id}: {e_gen}", exc_info=True)
        raise ProductionDBError(f"Unexpected error saving failed claim detail for {original_claim_id}: {e_gen}", original_exception=e_gen)


# --- Functions for Failed Claims UI (Enhanced) ---

def get_failed_claims_summary(sql_session: Session, 
                              limit: int = 100, 
                              offset: int = 0, 
                              status: Optional[str] = None,
                              date_from: Optional[date] = None,
                              date_to: Optional[date] = None,
                              error_code_like: Optional[str] = None,
                              facility_id: Optional[str] = None,
                              sort_by: Optional[str] = None, # e.g., "FailureTimestamp"
                              sort_order: str = "desc" # "asc" or "desc"
                              ) -> List[FailedClaimDetail]:
    """
    Fetches a summary of failed claims with enhanced filtering and sorting.
    """
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching failed claims summary with filters: status={status}, "
                f"date_from={date_from}, date_to={date_to}, error_code_like={error_code_like}, "
                f"facility_id={facility_id}, limit={limit}, offset={offset}, "
                f"sort_by={sort_by}, sort_order={sort_order}")
    try:
        query = sql_session.query(FailedClaimDetail)
        
        if status:
            query = query.filter(FailedClaimDetail.Status == status)
        if date_from:
            query = query.filter(FailedClaimDetail.FailureTimestamp >= datetime.combine(date_from, datetime.min.time()))
        if date_to:
            query = query.filter(FailedClaimDetail.FailureTimestamp <= datetime.combine(date_to, datetime.max.time()))
        if error_code_like:
            query = query.filter(FailedClaimDetail.ErrorCodes.ilike(f"%{error_code_like}%"))
        if facility_id:
            query = query.filter(FailedClaimDetail.FacilityId == facility_id)
        
        # Sorting
        if sort_by and hasattr(FailedClaimDetail, sort_by):
            column_to_sort = getattr(FailedClaimDetail, sort_by)
            if sort_order.lower() == "asc":
                query = query.order_by(asc(column_to_sort))
            else:
                query = query.order_by(desc(column_to_sort))
        else: # Default sort
            query = query.order_by(FailedClaimDetail.FailureTimestamp.desc())
                      
        claims = query.offset(offset).limit(limit).all()
        logger.info(f"[{cid}] Fetched {len(claims)} failed claims with applied filters.")
        return claims
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error fetching failed claims summary: {e}", exc_info=True)
        raise ProductionDBError(f"Failed to fetch failed claims summary: {e}", original_exception=e)

def get_failed_claim_by_id(sql_session: Session, failed_claim_detail_id: int) -> Optional[FailedClaimDetail]:
    """Fetches a specific failed claim by its FailedClaimDetailId."""
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching failed claim detail by ID: {failed_claim_detail_id}")
    try:
        # Use .get() for primary key lookup if FailedClaimDetailId is the sole PK
        # return sql_session.get(FailedClaimDetail, failed_claim_detail_id) 
        # If not, or to be sure:
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
        failed_claim.UpdatedDate = datetime.utcnow() 
        if resolution_notes is not None: 
            failed_claim.ResolutionNotes = resolution_notes
        if resolved_by:
            failed_claim.ResolvedBy = resolved_by
        if new_status.lower().startswith("resolved"): 
            failed_claim.ResolvedTimestamp = datetime.utcnow()
        
        logger.info(f"[{cid}] Failed claim {failed_claim_detail_id} status updated to '{new_status}'. Commit pending.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error updating status for failed claim {failed_claim_detail_id}: {e}", exc_info=True)
        raise ProductionDBError(f"Failed to update status for failed claim {failed_claim_detail_id}: {e}", original_exception=e)


# --- Functions for Sync Status Indicators (Querying existing SQL Server Views) ---

def get_real_time_dashboard_metrics(sql_session: Session) -> Optional[Dict[str, Any]]:
    """
    Fetches metrics from dbo.vw_RealTimeDashboard.
    This view needs to be defined in your sqlserver_create_results_database.sql.
    """
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching metrics from dbo.vw_RealTimeDashboard.")
    try:
        # Assuming vw_RealTimeDashboard returns a single row of metrics
        result = sql_session.execute(text("SELECT * FROM dbo.vw_RealTimeDashboard")).first()
        if result:
            # Convert RowProxy to dictionary
            return dict(result._mapping)
        logger.warning(f"[{cid}] No data returned from dbo.vw_RealTimeDashboard.")
        return None
    except SQLAlchemyError as e:
        # Check if the error is because the view doesn't exist
        if "Invalid object name 'dbo.vw_RealTimeDashboard'" in str(e):
            logger.error(f"[{cid}] View dbo.vw_RealTimeDashboard does not exist. Cannot fetch dashboard metrics.", exc_info=True)
            raise ProductionDBError("Dashboard view not found.", original_exception=e, details={"view_name": "dbo.vw_RealTimeDashboard"})
        logger.error(f"[{cid}] Error fetching from dbo.vw_RealTimeDashboard: {e}", exc_info=True)
        raise ProductionDBError("Failed to fetch dashboard metrics.", original_exception=e)


def get_system_health_summary(sql_session: Session) -> List[Dict[str, Any]]:
    """
    Fetches system health summary from dbo.vw_SystemHealth.
    This view needs to be defined in your sqlserver_create_results_database.sql.
    """
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching system health summary from dbo.vw_SystemHealth.")
    try:
        result = sql_session.execute(text("SELECT * FROM dbo.vw_SystemHealth")).fetchall()
        if result:
            return [dict(row._mapping) for row in result]
        logger.warning(f"[{cid}] No data returned from dbo.vw_SystemHealth.")
        return []
    except SQLAlchemyError as e:
        if "Invalid object name 'dbo.vw_SystemHealth'" in str(e):
            logger.error(f"[{cid}] View dbo.vw_SystemHealth does not exist. Cannot fetch system health.", exc_info=True)
            raise ProductionDBError("System health view not found.", original_exception=e, details={"view_name": "dbo.vw_SystemHealth"})
        logger.error(f"[{cid}] Error fetching from dbo.vw_SystemHealth: {e}", exc_info=True)
        raise ProductionDBError("Failed to fetch system health summary.", original_exception=e)

# --- Functions for Failure Pattern Analytics ---

def get_failed_claims_analytics_summary(sql_session: Session) -> List[Dict[str, Any]]:
    """
    Fetches aggregated failure analytics from dbo.vw_FailedClaimsAnalytics.
    This view needs to be defined in your sqlserver_create_results_database.sql.
    """
    cid = get_correlation_id()
    logger.debug(f"[{cid}] Fetching failed claims analytics from dbo.vw_FailedClaimsAnalytics.")
    try:
        result = sql_session.execute(text("SELECT * FROM dbo.vw_FailedClaimsAnalytics")).fetchall()
        if result:
            return [dict(row._mapping) for row in result]
        logger.warning(f"[{cid}] No data returned from dbo.vw_FailedClaimsAnalytics.")
        return []
    except SQLAlchemyError as e:
        if "Invalid object name 'dbo.vw_FailedClaimsAnalytics'" in str(e):
            logger.error(f"[{cid}] View dbo.vw_FailedClaimsAnalytics does not exist. Cannot fetch failure analytics.", exc_info=True)
            raise ProductionDBError("Failure analytics view not found.", original_exception=e, details={"view_name": "dbo.vw_FailedClaimsAnalytics"})
        logger.error(f"[{cid}] Error fetching from dbo.vw_FailedClaimsAnalytics: {e}", exc_info=True)
        raise ProductionDBError("Failed to fetch failure analytics summary.", original_exception=e)

def get_failed_claim_counts_by_attribute(sql_session: Session, 
                                       attribute_name: str,
                                       date_from: Optional[date] = None,
                                       date_to: Optional[date] = None,
                                       limit: int = 20
                                       ) -> List[Dict[str, Any]]:
    """
    Dynamically gets counts of failed claims grouped by a specified attribute
    (e.g., ProcessingStage, FacilityId, or a substring of ErrorCodes).

    Args:
        sql_session: SQLAlchemy session.
        attribute_name: The column name in FailedClaimDetail to group by.
                        Must be a valid column. Use with caution to prevent SQL injection if dynamically formed.
        date_from: Optional start date to filter failures.
        date_to: Optional end date to filter failures.
        limit: Max number of grouped results to return.

    Returns:
        A list of dictionaries, e.g., [{"attribute_value": "Validation", "failure_count": 150}, ...].
    """
    cid = get_correlation_id()
    if not hasattr(FailedClaimDetail, attribute_name):
        logger.error(f"[{cid}] Invalid attribute_name '{attribute_name}' for failed claim aggregation.")
        raise ValueError(f"Invalid attribute for aggregation: {attribute_name}")

    logger.debug(f"[{cid}] Aggregating failed claims by '{attribute_name}' from {date_from} to {date_to}.")
    
    try:
        query = sql_session.query(
            getattr(FailedClaimDetail, attribute_name).label("attribute_value"),
            func.count(FailedClaimDetail.FailedClaimDetailId).label("failure_count")
        )

        if date_from:
            query = query.filter(FailedClaimDetail.FailureTimestamp >= datetime.combine(date_from, datetime.min.time()))
        if date_to:
            query = query.filter(FailedClaimDetail.FailureTimestamp <= datetime.combine(date_to, datetime.max.time()))

        query = query.group_by(getattr(FailedClaimDetail, attribute_name))
        query = query.order_by(desc("failure_count"))
        query = query.limit(limit)

        results = query.all()
        
        return [{"attribute_value": row.attribute_value, "failure_count": row.failure_count} for row in results]

    except SQLAlchemyError as e:
        logger.error(f"[{cid}] Error aggregating failed claims by '{attribute_name}': {e}", exc_info=True)
        raise ProductionDBError(f"Failed to aggregate failed claims by '{attribute_name}'.", original_exception=e)


# --- Master Data Management (dbo schema, if this handler is responsible) ---
def get_production_facility_details(sql_session: Session, facility_id: str) -> Optional[ProductionFacility]:
    """Fetches facility details from dbo.Facilities in SQL Server."""
    try:
        return sql_session.query(ProductionFacility).filter_by(FacilityId=facility_id).first()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching Production facility {facility_id}: {e}", exc_info=True)
        raise ProductionDBError(f"Failed to fetch Production facility {facility_id}: {e}", original_exception=e)


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging
    from app.database.connection_manager import init_database_connections, get_sqlserver_session, dispose_engines
    from app.database.models.sqlserver_models import Base as SQLServerBase 
    from decimal import Decimal

    setup_logging()
    set_correlation_id("SQL_HANDLER_ENHANCED_TEST")
    sql_session = None
    try:
        init_database_connections()
        sql_session = get_sqlserver_session()

        SQLServerBase.metadata.create_all(sql_session.get_bind())
        sql_session.commit()

        logger.info("--- Testing Enhanced SQL Server Handler ---")

        # Test save_claim_to_production with reimbursement data
        # IMPORTANT: This assumes ProductionCMS1500LineItem has an 'EstimatedReimbursementAmount' field.
        # If not, the save_claim_to_production function will log a warning and not store it.
        mock_prod_claim = ProductionClaim(
            ClaimId="SQL_ENH_PROD001", FacilityId="FAC_SQL_ENH01", PatientId="PATSQL_ENH01",
            ServiceDate=datetime.now().date(), TotalChargeAmount=Decimal("450.00"),
            ProcessingStatus="COMPLETED_VALID", ValidationStatus="TEST_VALIDATED_ENH"
        )
        line1 = ProductionCMS1500LineItem(LineNumber=1, CptCode="99213", LineChargeAmount=Decimal("200.00"))
        # Assuming line1.EstimatedReimbursementAmount will be set
        line2 = ProductionCMS1500LineItem(LineNumber=2, CptCode="99214", LineChargeAmount=Decimal("250.00"))
        mock_prod_claim.cms1500_line_items.extend([line1, line2])
        diag = ProductionCMS1500Diagnosis(DiagnosisSequence=1, IcdCode="R07.9", IsPrimary=True)
        mock_prod_claim.cms1500_diagnoses.append(diag)

        # Simulate reimbursement data from a previous step
        reimbursements = {1: 150.25, 2: 190.75} # LineNumber: Amount

        save_success = save_claim_to_production(sql_session, mock_prod_claim, line_item_reimbursements=reimbursements)
        sql_session.commit()
        logger.info(f"Save to production test for {mock_prod_claim.ClaimId}: Success={save_success}")
        if save_success:
            # Verify if reimbursement was stored (if field exists)
            # loaded_claim = sql_session.query(ProductionClaim).filter_by(ClaimId=mock_prod_claim.ClaimId).options(orm.selectinload(ProductionClaim.cms1500_line_items)).first()
            # if loaded_claim and loaded_claim.cms1500_line_items and hasattr(loaded_claim.cms1500_line_items[0], 'EstimatedReimbursementAmount'):
            #    logger.info(f"  Line 1 Est. Reimbursement: {loaded_claim.cms1500_line_items[0].EstimatedReimbursementAmount}")
            pass


        # Test enhanced get_failed_claims_summary
        logger.info("\n--- Testing Enhanced Failed Claims Summary ---")
        # Add a few more failed claims for better testing
        for i in range(3):
            ts = datetime.utcnow() - timedelta(days=i)
            ec = f"CODE_A{i},CODE_B" if i % 2 == 0 else f"CODE_C,CODE_D{i}"
            fac = f"FAC_SQL_ENH0{i+1}"
            stat = "New" if i % 2 == 0 else "Pending Review"
            mock_fcd = FailedClaimDetail( OriginalClaimId=f"SQL_ENH_FAIL{i:03d}", FacilityId=fac, ProcessingStage=f"Stage{i}", ErrorCodes=ec, ErrorMessages=f"Error type {i}", Status=stat, FailureTimestamp=ts)
            save_failed_claim_detail(sql_session, mock_fcd)
        sql_session.commit()

        failed_summary = get_failed_claims_summary(sql_session, limit=5, status="New", sort_by="FacilityId", sort_order="asc")
        logger.info(f"Fetched {len(failed_summary)} 'New' failed claims, sorted by FacilityId asc:")
        for fc in failed_summary:
            logger.info(f"  ID: {fc.OriginalClaimId}, Facility: {fc.FacilityId}, Status: {fc.Status}, Timestamp: {fc.FailureTimestamp}")

        failed_summary_by_error = get_failed_claims_summary(sql_session, limit=5, error_code_like="CODE_A")
        logger.info(f"\nFetched {len(failed_summary_by_error)} failed claims with 'CODE_A' in ErrorCodes:")
        for fc in failed_summary_by_error:
            logger.info(f"  ID: {fc.OriginalClaimId}, ErrorCodes: {fc.ErrorCodes}")


        # Test Sync Status Indicators (these query views that should exist in your DB)
        logger.info("\n--- Testing Sync Status Indicators ---")
        try:
            dashboard_metrics = get_real_time_dashboard_metrics(sql_session)
            if dashboard_metrics:
                logger.info(f"Real-Time Dashboard Metrics: {dashboard_metrics}")
            else:
                logger.warning("Could not fetch real-time dashboard metrics (view might be empty or not exist).")
        except ProductionDBError as e:
             logger.warning(f"Could not fetch real-time dashboard metrics: {e.message} (Details: {e.details})")


        try:
            system_health = get_system_health_summary(sql_session)
            if system_health:
                logger.info(f"System Health Summary (first entry): {system_health[0] if system_health else 'N/A'}")
        except ProductionDBError as e:
            logger.warning(f"Could not fetch system health summary: {e.message} (Details: {e.details})")


        # Test Failure Pattern Analytics
        logger.info("\n--- Testing Failure Pattern Analytics ---")
        try:
            failure_analytics = get_failed_claims_analytics_summary(sql_session)
            if failure_analytics:
                logger.info(f"Failed Claims Analytics Summary (first entry): {failure_analytics[0] if failure_analytics else 'N/A'}")
            else:
                logger.warning("Could not fetch failure analytics summary (view might be empty or not exist).")
        except ProductionDBError as e:
            logger.warning(f"Could not fetch failure analytics summary: {e.message} (Details: {e.details})")

        try:
            counts_by_stage = get_failed_claim_counts_by_attribute(sql_session, "ProcessingStage")
            logger.info(f"\nFailed claim counts by ProcessingStage: {counts_by_stage}")
            
            counts_by_facility = get_failed_claim_counts_by_attribute(sql_session, "FacilityId", limit=3)
            logger.info(f"Failed claim counts by FacilityId (top 3): {counts_by_facility}")
        except ValueError as ve: # For invalid attribute
            logger.error(f"Error in custom aggregation: {ve}")
        except ProductionDBError as e:
             logger.warning(f"Could not fetch custom failure counts: {e.message}")


    except Exception as e:
        logger.critical(f"Error in SQL Server Handler (Enhanced) test script: {e}", exc_info=True)
        if sql_session and sql_session.is_active:
            sql_session.rollback()
    finally:
        if sql_session:
            sql_session.close()
        dispose_engines()
        logger.info("SQL Server Handler (Enhanced) test script finished.")

