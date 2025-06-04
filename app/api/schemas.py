# app/api/schemas.py
"""
Pydantic schemas for API request/response validation and serialization.
Used by FastAPI.
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from decimal import Decimal

# --- Base Schemas ---
class ResponseMessage(BaseModel):
    message: str
    details: Optional[Dict[str, Any]] = None

# --- Failed Claim Schemas ---
class FailedClaimBase(BaseModel):
    OriginalClaimId: str = Field(..., description="Identifier of the claim from its original source.")
    StagingClaimId: Optional[str] = None
    FacilityId: Optional[str] = None
    PatientAccountNumber: Optional[str] = None
    ServiceDate: Optional[date] = None
    FailureTimestamp: datetime
    ProcessingStage: str
    ErrorCodes: Optional[str] = Field(None, description="Comma-separated error codes or JSON array string.")
    ErrorMessages: str
    Status: str = Field(default="New", description="Current status in the resolution workflow.")

class FailedClaimDetailSchema(FailedClaimBase):
    FailedClaimDetailId: int
    ClaimDataSnapshot: Optional[str] = Field(None, description="Snapshot of claim data at time of failure (e.g., JSON).")
    ResolutionNotes: Optional[str] = None
    ResolvedBy: Optional[str] = None
    ResolvedTimestamp: Optional[datetime] = None
    CreatedDate: datetime
    UpdatedDate: datetime

    class Config:
        orm_mode = True # For compatibility with SQLAlchemy ORM objects

class FailedClaimUpdateSchema(BaseModel):
    Status: str = Field(..., description="New status for the failed claim.")
    ResolutionNotes: Optional[str] = Field(None, description="Notes regarding the resolution.")
    ResolvedBy: Optional[str] = Field(None, description="User or system ID that resolved the claim.")

# --- Claim Repair Suggestion Schemas ---
class RepairSuggestion(BaseModel):
    field_to_correct: Optional[str] = None
    current_value: Optional[Any] = None
    suggested_action: str
    suggested_value_options: Optional[List[Any]] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    reasoning: Optional[str] = None

class ClaimRepairSuggestionResponse(BaseModel):
    claim_id: str
    suggestions: List[RepairSuggestion]

# --- Analytics Schemas (Example) ---
class FailurePatternDatapoint(BaseModel):
    error_code_or_stage: str
    count: int
    percentage_of_total_failures: float

class FailureAnalyticsResponse(BaseModel):
    time_period_start: date
    time_period_end: date
    total_failures_in_period: int
    common_failure_patterns: List[FailurePatternDatapoint]
    # Add more analytics fields as needed (e.g., failure by facility, by payer)

# --- Sync Status Schemas (Example) ---
class SyncStatus(BaseModel):
    last_successful_sync_sqlserver: Optional[datetime] = Field(None, description="Timestamp of last successful data sync to SQL Server Production.")
    last_successful_sync_failed_claims_ui: Optional[datetime] = Field(None, description="Timestamp of last successful data sync/check for Failed Claims UI.")
    pending_items_for_production: int = Field(0, description="Approximate number of items pending transfer to production.")
    system_health: str = Field("OK", description="Overall system health indicator.")


if __name__ == '__main__':
    # Example usage of schemas
    failed_claim_example = FailedClaimDetailSchema(
        FailedClaimDetailId=1,
        OriginalClaimId="FAIL001X",
        FailureTimestamp=datetime.now(),
        ProcessingStage="ValidationRulesEngine",
        ErrorCodes="VAL_FAC_001,VAL_PAT_001",
        ErrorMessages="Facility ID invalid.\nPatient Account missing.",
        Status="Pending Review",
        CreatedDate=datetime.now(),
        UpdatedDate=datetime.now()
    )
    print("--- FailedClaimDetailSchema Example ---")
    print(failed_claim_example.json(indent=2))

    repair_suggestion_example = ClaimRepairSuggestionResponse(
        claim_id="FAIL001X",
        suggestions=[
            RepairSuggestion(
                field_to_correct="facility_id",
                suggested_action="Verify facility ID.",
                confidence=0.8
            )
        ]
    )
    print("\n--- ClaimRepairSuggestionResponse Example ---")
    print(repair_suggestion_example.json(indent=2))