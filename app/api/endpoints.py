# app/api/endpoints.py
"""
FastAPI endpoints for the Failed Claims UI and other API interactions.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date, datetime, timedelta

# Assuming schemas are in app.api.schemas
from app.api import schemas # Relative import
from app.database.connection_manager import get_sqlserver_session # For SQL Server data
from app.database.sqlserver_handler import ( # Functions from your SQL Server handler
    get_failed_claims_summary, get_failed_claim_by_id, update_failed_claim_status
)
from app.database.models.sqlserver_models import FailedClaimDetail
from app.services.claim_repair_service import ClaimRepairService # Placeholder service
from app.utils.logging_config import get_logger, set_correlation_id, get_correlation_id
from app.utils.error_handler import ProductionDBError, APIError
from app.database.connection_manager import CONFIG as APP_CONFIG # For service config

logger = get_logger('app.api.endpoints')
router = APIRouter()

# Initialize services that might be needed by endpoints
# This could also be done via FastAPI dependency injection for more complex setups
claim_repair_service_instance = ClaimRepairService(config=APP_CONFIG)


# --- Dependency for SQL Server Session ---
def get_db_session_sqlserver():
    """FastAPI dependency to get a SQL Server session."""
    db_sql = None
    try:
        db_sql = get_sqlserver_session()
        yield db_sql
    except Exception as e:
        logger.error(f"API: Could not get SQL Server session: {e}", exc_info=True)
        raise HTTPException(status_code=503, detail="Database service unavailable.")
    finally:
        if db_sql:
            db_sql.close()

# --- Endpoints for Failed Claims ---

@router.get("/failed-claims", response_model=List[schemas.FailedClaimDetailSchema], tags=["Failed Claims"])
async def read_failed_claims(
    status: Optional[str] = Query(None, description="Filter by status (e.g., New, Pending Review)"),
    skip: int = Query(0, ge=0, description="Number of records to skip (for pagination)"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records to return"),
    db: Session = Depends(get_db_session_sqlserver)
):
    """
    Retrieve a list of failed claims. Supports filtering by status and pagination.
    """
    cid = set_correlation_id(f"API_GET_FAILED_CLAIMS_{status or 'ALL'}")
    logger.info(f"[{cid}] GET /failed-claims called. Status: {status}, Skip: {skip}, Limit: {limit}")
    try:
        failed_claims_orm = get_failed_claims_summary(db, limit=limit, offset=skip, status=status)
        # Pydantic will automatically convert ORM objects to schemas.FailedClaimDetailSchema
        return failed_claims_orm
    except ProductionDBError as e:
        logger.error(f"[{cid}] Database error fetching failed claims: {e.message}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {e.message}")
    except Exception as e:
        logger.error(f"[{cid}] Unexpected error fetching failed claims: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")


@router.get("/failed-claims/{failed_claim_detail_id}", response_model=schemas.FailedClaimDetailSchema, tags=["Failed Claims"])
async def read_failed_claim_detail(
    failed_claim_detail_id: int = Path(..., ge=1, description="The ID of the failed claim detail to retrieve"),
    db: Session = Depends(get_db_session_sqlserver)
):
    """
    Retrieve details for a specific failed claim by its FailedClaimDetailId.
    """
    cid = set_correlation_id(f"API_GET_FAILED_CLAIM_{failed_claim_detail_id}")
    logger.info(f"[{cid}] GET /failed-claims/{failed_claim_detail_id} called.")
    try:
        db_failed_claim = get_failed_claim_by_id(db, failed_claim_detail_id)
        if db_failed_claim is None:
            logger.warning(f"[{cid}] Failed claim detail with ID {failed_claim_detail_id} not found.")
            raise HTTPException(status_code=404, detail="Failed claim detail not found.")
        return db_failed_claim
    except ProductionDBError as e:
        logger.error(f"[{cid}] Database error fetching failed claim {failed_claim_detail_id}: {e.message}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {e.message}")
    except Exception as e:
        logger.error(f"[{cid}] Unexpected error fetching failed claim {failed_claim_detail_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")


@router.put("/failed-claims/{failed_claim_detail_id}", response_model=schemas.FailedClaimDetailSchema, tags=["Failed Claims"])
async def update_failed_claim(
    failed_claim_detail_id: int = Path(..., ge=1, description="The ID of the failed claim detail to update"),
    update_data: schemas.FailedClaimUpdateSchema = Body(...),
    db: Session = Depends(get_db_session_sqlserver)
):
    """
    Update the status and resolution notes for a specific failed claim.
    This would typically be used in a claim resolution workflow.
    """
    cid = set_correlation_id(f"API_PUT_FAILED_CLAIM_{failed_claim_detail_id}")
    logger.info(f"[{cid}] PUT /failed-claims/{failed_claim_detail_id} called. New status: {update_data.Status}")
    try:
        success = update_failed_claim_status(
            db,
            failed_claim_detail_id=failed_claim_detail_id,
            new_status=update_data.Status,
            resolution_notes=update_data.ResolutionNotes,
            resolved_by=update_data.ResolvedBy # Could also get user from auth token
        )
        if not success:
            logger.warning(f"[{cid}] Failed claim detail with ID {failed_claim_detail_id} not found for update or update failed.")
            raise HTTPException(status_code=404, detail="Failed claim detail not found or update failed.")
        
        db.commit() # Commit the change after successful update in handler
        updated_claim = get_failed_claim_by_id(db, failed_claim_detail_id) # Fetch again to return updated object
        if updated_claim is None: # Should not happen if update_failed_claim_status returned True and committed
             raise HTTPException(status_code=404, detail="Failed claim detail not found after update attempt.")
        return updated_claim
    except ProductionDBError as e:
        db.rollback()
        logger.error(f"[{cid}] Database error updating failed claim {failed_claim_detail_id}: {e.message}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {e.message}")
    except HTTPException: # Re-raise HTTPExceptions
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"[{cid}] Unexpected error updating failed claim {failed_claim_detail_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during update.")


# --- Endpoints for Claim Repair Suggestions (Placeholder) ---
@router.post("/failed-claims/{original_claim_id}/suggest-repair", response_model=schemas.ClaimRepairSuggestionResponse, tags=["Claim Repair"])
async def suggest_claim_repair(
    original_claim_id: str = Path(..., description="The original ID of the failed claim for which to get repair suggestions"),
    # We might need more details about the failure, or the service fetches them.
    # For now, assume the service can use the claim_id to get failure details if needed.
    # Or, the client could post the FailedClaimDetail object.
    failed_claim_info: Optional[schemas.FailedClaimDetailSchema] = Body(None, description="Optional: Full details of the failed claim if available to client."),
    db: Session = Depends(get_db_session_sqlserver) # If service needs DB access
):
    """
    Get AI-powered (or rule-based) suggestions for repairing a failed claim.
    """
    cid = set_correlation_id(f"API_SUGGEST_REPAIR_{original_claim_id}")
    logger.info(f"[{cid}] POST /failed-claims/{original_claim_id}/suggest-repair called.")
    
    if not claim_repair_service_instance.enabled:
        logger.warning(f"[{cid}] Claim repair service is disabled.")
        raise HTTPException(status_code=501, detail="Claim repair suggestion service is not enabled.")

    try:
        # Fetch failed claim details if not provided or to ensure latest data
        failure_details_for_service = {}
        if failed_claim_info:
            failure_details_for_service = failed_claim_info.dict()
        else:
            # Attempt to find the failed claim by OriginalClaimId. This might return multiple if not careful.
            # Assuming FailedClaimDetails has OriginalClaimId indexed and we take the latest.
            failed_entry_orm = db.query(FailedClaimDetail)\
                                .filter(FailedClaimDetail.OriginalClaimId == original_claim_id)\
                                .order_by(FailedClaimDetail.FailureTimestamp.desc())\
                                .first()
            if not failed_entry_orm:
                raise HTTPException(status_code=404, detail=f"No failed claim details found for OriginalClaimId {original_claim_id} to generate suggestions.")
            failure_details_for_service = schemas.FailedClaimDetailSchema.from_orm(failed_entry_orm).dict()

        suggestions = claim_repair_service_instance.get_repair_suggestions(
            claim_id=original_claim_id,
            failed_claim_details=failure_details_for_service
        )
        return schemas.ClaimRepairSuggestionResponse(claim_id=original_claim_id, suggestions=suggestions)
        
    # except ExternalServiceError as e: # If service calls external API
    #     logger.error(f"[{cid}] External service error for claim repair suggestions for {original_claim_id}: {e.message}", exc_info=True)
    #     raise HTTPException(status_code=e.status_code, detail=f"External service error: {e.message}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{cid}] Unexpected error getting repair suggestions for {original_claim_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while generating repair suggestions.")


# --- Placeholder Endpoints for Analytics & Sync Status ---
@router.get("/analytics/failure-patterns", response_model=schemas.FailureAnalyticsResponse, tags=["Analytics"])
async def get_failure_pattern_analytics(
    start_date: date = Query(..., description="Start date for analytics period"),
    end_date: date = Query(..., description="End date for analytics period"),
    # db: Session = Depends(get_db_session_sqlserver) # If analytics are run on SQL Server data
):
    """
    (Placeholder) Retrieve analytics on common claim failure patterns.
    """
    cid = set_correlation_id("API_ANALYTICS_FAILURES")
    logger.info(f"[{cid}] GET /analytics/failure-patterns called for period {start_date} to {end_date}.")
    # --- Placeholder Implementation ---
    # In a real app, this would query FailedClaimDetails, aggregate data, etc.
    return schemas.FailureAnalyticsResponse(
        time_period_start=start_date,
        time_period_end=end_date,
        total_failures_in_period=125, # Dummy data
        common_failure_patterns=[
            schemas.FailurePatternDatapoint(error_code_or_stage="VAL_FAC_001", count=30, percentage_of_total_failures=0.24),
            schemas.FailurePatternDatapoint(error_code_or_stage="MLPrediction", count=20, percentage_of_total_failures=0.16),
        ]
    )

@router.get("/status/sync", response_model=schemas.SyncStatus, tags=["System Status"])
async def get_system_sync_status():
    """
    (Placeholder) Provides information about system data synchronization status.
    """
    cid = set_correlation_id("API_STATUS_SYNC")
    logger.info(f"[{cid}] GET /status/sync called.")
    # --- Placeholder Implementation ---
    # This would query logs or metadata tables to get actual sync times and counts.
    return schemas.SyncStatus(
        last_successful_sync_sqlserver=datetime.now() - timedelta(minutes=5),
        last_successful_sync_failed_claims_ui=datetime.now() - timedelta(minutes=2),
        pending_items_for_production=42, # Dummy data
        system_health="OK - Monitoring"
    )