# app/processing/edi_parser.py
"""
Parses EDI CMS 1500 claim files.
This is a simplified parser. Real EDI parsing is complex and typically uses dedicated libraries.
This parser will focus on extracting key fields and mapping them to staging ORM models.
"""
import os
import re # For simple pattern matching if not using a proper EDI library
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import EDIParserError, handle_exception
from app.database.models.postgres_models import (
    StagingClaim, StagingCMS1500Diagnosis, StagingCMS1500LineItem,
    ProcessedChunk, RetryQueue # For tracking parsing progress and failures
)
# from app.database.postgres_handler import add_parsed_claims_to_staging # This function would be in postgres_handler.py

logger = get_logger('app.processing.edi_parser')

# Placeholder for a more sophisticated EDI parsing library if one were used.
# For now, we'll simulate parsing by looking for segment identifiers.
# Example: ISA*, GS*, ST*, SE*, GE*, IEA* (envelope)
# HL* (Hierarchical Level)
# NM1* (Submitter, Receiver, Billing Provider, Patient, Payer, etc.)
# DMG* (Patient Demographics)
# CLM* (Claim Level Information)
# DTP* (Dates)
# LX* (Line Item Number)
# SV1* (Professional Service - Line Item)
# SVD* (Service Line Adjudication - if parsing 835s, not 837s)
# HI* (Health Care Diagnosis Code)

# This is a highly simplified representation. A real EDI parser would handle loops, segments, elements, sub-elements.
# We will assume a function that takes EDI content and yields structured claim data.

def parse_edi_file_content(file_path: str, edi_content: str) -> list[dict]:
    """
    Parses the content of an EDI file.
    This is a placeholder for a real EDI parsing logic.
    It should yield dictionaries, each representing a claim with its details.
    """
    logger.info(f"Starting EDI parsing for file: {file_path} (content length: {len(edi_content)})")
    parsed_claims_data = []
    # --- SIMPLIFIED MOCK PARSING ---
    # In a real scenario, you'd iterate through segments, identify loops (2000A, 2000B, 2300, 2400 etc.)
    # For now, let's assume the edi_content is a very simplified structure or pre-processed.

    # Example: Assume one claim per file for this mock, or a way to split them.
    # Let's imagine we extract one claim's data.
    # This would involve complex regex or a state machine if not using a library.

    # Mock extraction - replace with actual EDI parsing logic
    claim_id_match = re.search(r"CLM\*([^\*~]+)\*", edi_content) # Claim ID from CLM01
    patient_acc_match = re.search(r"REF\*EJ\*([^\*~]+)\*", edi_content) # Patient Account Number from REF segment with EJ qualifier
    total_charge_match = re.search(r"CLM\*[^\*]*\*[^\*]*\*([^\*~]+)\*", edi_content) # Total charges from CLM02

    if not claim_id_match:
        logger.warning(f"Could not find a clear claim ID in {file_path}. Skipping.")
        # In a real system, this might be an error or part of a larger transaction.
        return []

    mock_claim_id = claim_id_match.group(1) if claim_id_match else f"MOCK_CLAIM_{os.path.basename(file_path)}"
    
    # Simulate extracting a few key pieces of information
    claim_data = {
        "claim_id": mock_claim_id,
        "facility_id": "FAC001", # Placeholder - This would come from EDI (e.g., NM1*85 billing provider)
        "department_id": None, # Placeholder
        "financial_class_id": None, # Placeholder
        "patient_id": f"PAT_{mock_claim_id}", # Placeholder
        "patient_age": 35, # Placeholder
        "patient_dob": datetime.strptime("1988-07-15", "%Y-%m-%d").date(), # Placeholder
        "patient_sex": "M", # Placeholder
        "patient_account_number": patient_acc_match.group(1) if patient_acc_match else f"ACC_{mock_claim_id}",
        "provider_id": "PROV001", # Placeholder
        "provider_type": "Primary Care", # Placeholder
        "rendering_provider_npi": "1234567890", # Placeholder
        "place_of_service": "11", # Office - Placeholder
        "service_date": datetime.strptime("2023-10-01", "%Y-%m-%d").date(), # Placeholder - Claim level service date (e.g. CLM05-1)
        "total_charge_amount": Decimal(total_charge_match.group(1)) if total_charge_match else Decimal("500.00"),
        "total_claim_charges": Decimal(total_charge_match.group(1)) if total_charge_match else Decimal("500.00"), # Often the same as CLM02 for CMS1500
        "payer_name": "Mock Payer Inc.", # Placeholder
        "primary_insurance_id": "INS123", # Placeholder
        "secondary_insurance_id": None, # Placeholder
        "claim_data_raw": edi_content, # Store the raw segment or full claim for reference
        "diagnoses": [],
        "line_items": []
    }

    # Mock Diagnoses (HI segment in 2300 loop)
    # Example: HI*BK:D509~ABK:V700
    diag_matches = re.findall(r"HI\*([A-Z0-9]{2,3}):([^\*~]+)", edi_content)
    for i, match in enumerate(diag_matches[:12]): # Max 12 diagnoses on CMS1500
        qualifier, code = match
        diag_data = {
            "diagnosis_sequence": i + 1,
            "icd_code": code.strip(),
            "icd_code_type": "ICD10" if qualifier.startswith("AB") or qualifier.startswith("B") else "UNKNOWN", # Simplified
            "is_primary": i == 0, # First diagnosis is often primary
        }
        claim_data["diagnoses"].append(diag_data)
    
    if not claim_data["diagnoses"]: # Add a default if none found
        claim_data["diagnoses"].append({
            "diagnosis_sequence": 1, "icd_code": "R51", "icd_code_type": "ICD10", "is_primary": True, "diagnosis_description": "Headache"
        })


    # Mock Line Items (LX* and SV1* segments in 2400 loop)
    # Example: LX*1~SV1*HC:99213*150*UN*1***1:2:3~DTP*472*D8*20231001~
    line_item_matches = re.finditer(r"LX\*(\d+)\*?~SV1\*HC:([^\*~]+)\*([^\*~]+)\*([^\*~]+)\*([^\*~]+)(.*?)(?:~|$)", edi_content, re.DOTALL)
    line_num = 1
    for match in line_item_matches:
        #lx_num = match.group(1) # Line number from LX
        cpt_code = match.group(2)
        charge = match.group(3)
        units_qual = match.group(4) # e.g., UN
        units = match.group(5)
        # Modifiers and diagnosis pointers are more complex to extract reliably with simple regex from SV101-2 to SV101-6 and SV107
        
        line_data = {
            "line_number": line_num,
            "service_date_from": datetime.strptime("2023-10-01", "%Y-%m-%d").date(), # Placeholder - line level service date
            "service_date_to": datetime.strptime("2023-10-01", "%Y-%m-%d").date(), # Placeholder
            "place_of_service_code": "11", # Placeholder
            "cpt_code": cpt_code.strip(),
            "modifier_1": None, # Placeholder
            "line_charge_amount": Decimal(charge),
            "units": int(float(units)) if units else 1,
            "diagnosis_pointers": "1", # Placeholder - points to first diagnosis
        }
        claim_data["line_items"].append(line_data)
        line_num +=1

    if not claim_data["line_items"]: # Add a default if none found
        claim_data["line_items"].append({
            "line_number": 1, "service_date_from": datetime.strptime("2023-10-01", "%Y-%m-%d").date(),
            "place_of_service_code": "11", "cpt_code": "99213", "line_charge_amount": Decimal("150.00"),
            "units": 1, "diagnosis_pointers": "1"
        })
    
    parsed_claims_data.append(claim_data)
    logger.info(f"Mock parsed {len(parsed_claims_data)} claim(s) from file {file_path}.")
    # --- END SIMPLIFIED MOCK PARSING ---
    return parsed_claims_data


def map_parsed_data_to_orm(parsed_claim_data: dict, created_by: str = "edi_parser") -> StagingClaim:
    """Maps a dictionary of parsed claim data to Staging ORM objects."""
    try:
        claim = StagingClaim(
            claim_id=parsed_claim_data['claim_id'],
            facility_id=parsed_claim_data.get('facility_id'),
            department_id=parsed_claim_data.get('department_id'),
            financial_class_id=parsed_claim_data.get('financial_class_id'),
            patient_id=parsed_claim_data.get('patient_id'),
            patient_age=parsed_claim_data.get('patient_age'),
            patient_dob=parsed_claim_data.get('patient_dob'),
            patient_sex=parsed_claim_data.get('patient_sex'),
            patient_account_number=parsed_claim_data.get('patient_account_number'),
            provider_id=parsed_claim_data.get('provider_id'),
            provider_type=parsed_claim_data.get('provider_type'),
            rendering_provider_npi=parsed_claim_data.get('rendering_provider_npi'),
            place_of_service=parsed_claim_data.get('place_of_service'),
            service_date=parsed_claim_data.get('service_date'),
            total_charge_amount=parsed_claim_data.get('total_charge_amount'),
            total_claim_charges=parsed_claim_data.get('total_claim_charges'),
            payer_name=parsed_claim_data.get('payer_name'),
            primary_insurance_id=parsed_claim_data.get('primary_insurance_id'),
            secondary_insurance_id=parsed_claim_data.get('secondary_insurance_id'),
            claim_data=parsed_claim_data.get('claim_data_raw'), # Store raw EDI snippet or full claim JSON
            processing_status='PARSED',
            created_by=created_by,
            # Timestamps are handled by DB defaults or SQLAlchemy defaults
        )

        for diag_data in parsed_claim_data.get('diagnoses', []):
            diagnosis = StagingCMS1500Diagnosis(
                diagnosis_sequence=diag_data['diagnosis_sequence'],
                icd_code=diag_data['icd_code'],
                icd_code_type=diag_data.get('icd_code_type', 'ICD10'),
                diagnosis_description=diag_data.get('diagnosis_description'),
                is_primary=diag_data.get('is_primary', False)
            )
            claim.cms1500_diagnoses.append(diagnosis)

        for line_data in parsed_claim_data.get('line_items', []):
            line_item = StagingCMS1500LineItem(
                line_number=line_data['line_number'],
                service_date_from=line_data.get('service_date_from'),
                service_date_to=line_data.get('service_date_to'),
                place_of_service_code=line_data.get('place_of_service_code'),
                emergency_indicator=line_data.get('emergency_indicator'),
                cpt_code=line_data['cpt_code'],
                modifier_1=line_data.get('modifier_1'),
                modifier_2=line_data.get('modifier_2'),
                modifier_3=line_data.get('modifier_3'),
                modifier_4=line_data.get('modifier_4'),
                procedure_description=line_data.get('procedure_description'),
                diagnosis_pointers=line_data.get('diagnosis_pointers'),
                line_charge_amount=line_data['line_charge_amount'],
                units=line_data.get('units', 1),
                epsdt_family_plan=line_data.get('epsdt_family_plan'),
                rendering_provider_id_qualifier=line_data.get('rendering_provider_id_qualifier'),
                rendering_provider_id=line_data.get('rendering_provider_id')
            )
            claim.cms1500_line_items.append(line_item)
        
        return claim
    except KeyError as ke:
        msg = f"Missing expected key '{ke}' in parsed_claim_data for claim '{parsed_claim_data.get('claim_id', 'Unknown')}'. Cannot map to ORM."
        logger.error(msg)
        raise EDIParserError(msg, claim_id=parsed_claim_data.get('claim_id', 'Unknown'), details={"missing_key": str(ke)})
    except Exception as e:
        msg = f"Error mapping parsed data to ORM for claim '{parsed_claim_data.get('claim_id', 'Unknown')}': {e}"
        logger.error(msg, exc_info=True)
        raise EDIParserError(msg, claim_id=parsed_claim_data.get('claim_id', 'Unknown'))


def process_edi_file(file_path: str, pg_session: Session) -> tuple[int, int]:
    """
    Reads an EDI file, parses its content, maps to ORM objects, and saves to staging.
    Returns (number_of_claims_successfully_parsed, number_of_claims_failed).
    """
    cid = get_correlation_id()
    logger.info(f"[{cid}] Processing EDI file: {file_path}")
    successful_claims_count = 0
    failed_claims_count = 0
    
    # For chunk tracking (simplified)
    # In a real system, chunk_id might come from a batching system or file splitting
    chunk_id_placeholder = hash(file_path) # Simplistic chunk ID

    try:
        with open(file_path, 'r', encoding='utf-8') as f: # Assuming UTF-8, EDI often uses other encodings
            edi_content = f.read()
        
        parsed_claims_data_list = parse_edi_file_content(file_path, edi_content)
        
        if not parsed_claims_data_list:
            logger.warning(f"[{cid}] No claims data parsed from file: {file_path}")
            # Log to processed_chunks as completed with 0 claims if that's the policy
            # For now, just return.
            return 0, 0

        orm_claims_to_add = []
        for parsed_data in parsed_claims_data_list:
            try:
                orm_claim = map_parsed_data_to_orm(parsed_data)
                orm_claims_to_add.append(orm_claim)
                successful_claims_count += 1
            except EDIParserError as e:
                logger.error(f"[{cid}] Failed to map claim data from {file_path}, ClaimID '{e.claim_id}': {e.message}")
                failed_claims_count += 1
                # Optionally, add this specific failure to a retry queue or error log immediately
        
        if orm_claims_to_add:
            # This function would be in postgres_handler.py
            # add_parsed_claims_to_staging(pg_session, orm_claims_to_add)
            pg_session.add_all(orm_claims_to_add) # Assuming direct session usage here for simplicity
            pg_session.commit() # Commit per file or per batch of files
            logger.info(f"[{cid}] Successfully added {len(orm_claims_to_add)} claims from {file_path} to staging.")

        # Update ProcessedChunk status (simplified)
        # processed_chunk = pg_session.query(ProcessedChunk).filter_by(chunk_id=chunk_id_placeholder).first()
        # if not processed_chunk:
        #     processed_chunk = ProcessedChunk(chunk_id=chunk_id_placeholder)
        #     pg_session.add(processed_chunk)
        # processed_chunk.status = 'COMPLETED'
        # processed_chunk.claims_count = successful_claims_count
        # processed_chunk.processed_date = datetime.utcnow()
        # pg_session.commit()

    except FileNotFoundError:
        logger.error(f"[{cid}] EDI file not found: {file_path}")
        failed_claims_count = 1 # Count the file itself as a failure if it's expected to produce claims
        # Add to retry_queue if applicable
        # retry_entry = RetryQueue(chunk_id=chunk_id_placeholder, error_message=f"File not found: {file_path}", retry_count=0)
        # pg_session.add(retry_entry)
        # pg_session.commit()
    except EDIParserError as e: # Catch errors from parse_edi_file_content if it raises them
        logger.error(f"[{cid}] EDI Parser Error for file {file_path}: {e.message}", exc_info=True)
        failed_claims_count = 1 # Or count based on expected claims
        # Add to retry_queue
    except Exception as e:
        logger.error(f"[{cid}] Unexpected error processing EDI file {file_path}: {e}", exc_info=True)
        failed_claims_count = 1 # Or count based on expected claims
        # Add to retry_queue
        # handle_exception(e, context=f"EDIFileProcessing: {file_path}", re_raise_as=EDIParserError)
        if pg_session.is_active:
            pg_session.rollback() # Rollback any partial changes for this file on unexpected error

    return successful_claims_count, failed_claims_count


if __name__ == '__main__':
    # This is example usage. In a real app, this would be called by a batch processor or main orchestrator.
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, get_postgres_session, dispose_engines
    
    setup_logging()
    set_correlation_id("EDI_PARSER_TEST")
    
    # Create dummy EDI file for testing
    dummy_edi_dir = "data/sample_edi_claims_test/"
    os.makedirs(dummy_edi_dir, exist_ok=True)
    dummy_file_path = os.path.join(dummy_edi_dir, "test_claim_001.edi")
    
    # Simplified EDI-like content
    # This content is designed to be picked up by the mock parser's regex.
    # A real EDI file is much more complex.
    dummy_edi_content = """
    ISA*...~
    GS*...~
    ST*837*0001~
    BHT*...~
    NM1*41*2*Mock Submitter*****XX*1234567890~  // Submitter
    PER*...~
    NM1*40*2*Mock Payer Inc*****PI*PAYERID123~ // Payer
    HL*1**20*1~ // Billing Provider HL
    NM1*85*2*Mock Clinic*****XX*9876543210~ // Billing Provider Name (Facility FAC001)
    N3*123 Clinic Rd~
    N4*Springfield*IL*62701~
    REF*EI*TAXIDCLINIC~
    HL*2*1*22*0~ // Subscriber HL (Patient is subscriber for simplicity)
    SBR*P*18*******CI~
    NM1*IL*1*Doe*John*M***MI*PAT_MOCK_CLAIM_test_claim_001.edi~ // Patient Name
    N3*456 Patient Ln~
    N4*Springfield*IL*62701~
    DMG*D8*19880715*M~ // Patient Demographics (DOB, Sex)
    REF*EJ*ACC_PAT_MOCK_CLAIM_test_claim_001.edi~ // Patient Account Number
    CLM*MOCK_CLAIM_test_claim_001.edi*500***11:B:1*Y*A*Y*Y*P~ // Claim Information (ID, Total Charge)
    DTP*434*RD8*20231001-20231005~ // Claim Dates (Statement From/To)
    HI*BK:R51*ABK:Z000~ // Diagnosis Code (Primary: R51, Secondary: Z000)
    NM1*82*1*Rendering*Provider***XX*1234567890~ // Rendering Provider
    LX*1~ // Line Item 1
    SV1*HC:99213*150*UN*1***1~ // Service Line (CPT, Charge, Units, Diag Ptr)
    DTP*472*D8*20231001~ // Service Date for Line 1
    LX*2~ // Line Item 2
    SV1*HC:G0008*50*UN*1***2~ // Service Line (CPT, Charge, Units, Diag Ptr)
    DTP*472*D8*20231001~ // Service Date for Line 2
    SE*25*0001~
    GE*1*1~
    IEA*1*000000001~
    """
    with open(dummy_file_path, "w") as f:
        f.write(dummy_edi_content)

    logger.info(f"Dummy EDI file created at {dummy_file_path}")

    pg_session = None
    try:
        init_database_connections() # Initialize DB connections
        pg_session = get_postgres_session()
        
        success, fail = process_edi_file(dummy_file_path, pg_session)
        logger.info(f"Test processing finished. Success: {success}, Fail: {fail}")

        # Query to verify (optional)
        if success > 0:
            # from app.database.models.postgres_models import StagingClaim
            # test_claim = pg_session.query(StagingClaim).filter_by(claim_id="MOCK_CLAIM_test_claim_001.edi").first()
            # if test_claim:
            #     logger.info(f"Verified claim {test_claim.claim_id} in database with {len(test_claim.cms1500_line_items)} lines.")
            # else:
            #     logger.error("Test claim not found in database after parsing.")
            pass

    except Exception as e:
        logger.critical(f"Error in EDI parser test script: {e}", exc_info=True)
    finally:
        if pg_session:
            pg_session.close()
        dispose_engines() # Dispose engines at the end
        # os.remove(dummy_file_path) # Clean up
        # os.rmdir(dummy_edi_dir)
        logger.info("EDI parser test script finished.")
