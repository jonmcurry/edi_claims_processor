# app/database/models/postgres_models.py
"""
SQLAlchemy ORM models for the PostgreSQL database (edi_staging)
Reflects the schema defined in `database_scripts/postgresql_create_edi_databases.sql`.
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Boolean, Text, ForeignKey,
    UniqueConstraint, Index, JSON, DECIMAL, CHAR, DATE, ARRAY
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB # For PostgreSQL specific JSONB type
import datetime

Base = declarative_base()

# --- EDI Schema ---

class Organization(Base):
    """ edi.organizations table """
    __tablename__ = 'organizations'
    __table_args__ = {'schema': 'edi'}

    organization_id = Column(Integer, primary_key=True, autoincrement=True)
    organization_name = Column(String(200), nullable=False)
    organization_code = Column(String(20), unique=True)
    address_line_1 = Column(String(255))
    address_line_2 = Column(String(255))
    city = Column(String(100))
    state_code = Column(CHAR(2))
    zip_code = Column(String(10))
    country_code = Column(CHAR(3), default='USA')
    phone = Column(String(20))
    email = Column(String(255))
    website = Column(String(255))
    tax_id = Column(String(20))
    active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facilities = relationship("Facility", back_populates="organization")
    organization_regions = relationship("OrganizationRegion", back_populates="organization")

class Region(Base):
    """ edi.regions table """
    __tablename__ = 'regions'
    __table_args__ = {'schema': 'edi'}

    region_id = Column(Integer, primary_key=True, autoincrement=True)
    region_name = Column(String(200), nullable=False)
    region_code = Column(String(20), unique=True)
    description = Column(Text)
    active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facilities = relationship("Facility", back_populates="region")
    organization_regions = relationship("OrganizationRegion", back_populates="region")

class OrganizationRegion(Base):
    """ edi.organization_regions table """
    __tablename__ = 'organization_regions'
    __table_args__ = (
        UniqueConstraint('organization_id', 'region_id', name='pk_organization_regions'),
        {'schema': 'edi'}
    )

    organization_id = Column(Integer, ForeignKey('edi.organizations.organization_id', ondelete='CASCADE'), primary_key=True)
    region_id = Column(Integer, ForeignKey('edi.regions.region_id', ondelete='CASCADE'), primary_key=True)
    is_primary = Column(Boolean, default=False)
    effective_date = Column(DATE, default=datetime.date.today)
    end_date = Column(DATE)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    organization = relationship("Organization", back_populates="organization_regions")
    region = relationship("Region", back_populates="organization_regions")


class Facility(Base):
    """ edi.facilities table """
    __tablename__ = 'facilities'
    __table_args__ = {'schema': 'edi'}

    facility_id = Column(String(50), primary_key=True) # User-defined facility ID
    facility_name = Column(String(200), nullable=False)
    facility_code = Column(String(20), unique=True)
    organization_id = Column(Integer, ForeignKey('edi.organizations.organization_id'))
    region_id = Column(Integer, ForeignKey('edi.regions.region_id'))
    city = Column(String(100))
    state_code = Column(CHAR(2))
    zip_code = Column(String(10))
    address_line_1 = Column(String(255))
    address_line_2 = Column(String(255))
    bed_size = Column(Integer) # CHECK (bed_size >= 0) - handled by DB
    fiscal_reporting_month = Column(Integer) # CHECK (fiscal_reporting_month BETWEEN 1 AND 12)
    emr_system = Column(String(100))
    facility_type = Column(String(20)) # CHECK (facility_type IN ('Teaching', 'Non-Teaching', 'Unknown'))
    critical_access = Column(CHAR(1)) # CHECK (critical_access IN ('Y', 'N', 'U'))
    phone = Column(String(20))
    fax = Column(String(20))
    email = Column(String(255))
    npi_number = Column(String(10))
    tax_id = Column(String(20))
    license_number = Column(String(50))
    active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    organization = relationship("Organization", back_populates="facilities")
    region = relationship("Region", back_populates="facilities")
    financial_classes = relationship("FinancialClass", back_populates="facility")
    clinical_departments = relationship("ClinicalDepartment", back_populates="facility")

class StandardPayer(Base):
    """ edi.standard_payers table """
    __tablename__ = 'standard_payers'
    __table_args__ = {'schema': 'edi'}

    standard_payer_id = Column(Integer, primary_key=True, autoincrement=True)
    standard_payer_code = Column(String(20), unique=True, nullable=False)
    standard_payer_name = Column(String(100), nullable=False)
    payer_category = Column(String(50)) # CHECK (payer_category IN (...))
    description = Column(Text)
    active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    financial_classes = relationship("FinancialClass", back_populates="standard_payer")

class HccType(Base):
    """ edi.hcc_types table """
    __tablename__ = 'hcc_types'
    __table_args__ = {'schema': 'edi'}

    hcc_type_id = Column(Integer, primary_key=True, autoincrement=True)
    hcc_code = Column(String(10), unique=True, nullable=False)
    hcc_name = Column(String(100), nullable=False)
    hcc_agency = Column(String(10)) # CHECK (hcc_agency IN (...))
    description = Column(Text)
    active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    financial_classes = relationship("FinancialClass", back_populates="hcc_type")

class FinancialClass(Base):
    """ edi.financial_classes table """
    __tablename__ = 'financial_classes'
    __table_args__ = {'schema': 'edi'}

    financial_class_id = Column(String(50), primary_key=True) # User-defined ID
    financial_class_description = Column(String(200), nullable=False)
    standard_payer_id = Column(Integer, ForeignKey('edi.standard_payers.standard_payer_id'))
    hcc_type_id = Column(Integer, ForeignKey('edi.hcc_types.hcc_type_id'))
    facility_id = Column(String(50), ForeignKey('edi.facilities.facility_id'))
    payer_priority = Column(Integer, default=1)
    requires_authorization = Column(Boolean, default=False)
    authorization_required_services = Column(Text)
    copay_amount = Column(DECIMAL(8, 2))
    deductible_amount = Column(DECIMAL(10, 2))
    claim_submission_format = Column(String(20))
    days_to_file_claim = Column(Integer, default=365)
    accepts_electronic_claims = Column(Boolean, default=True)
    active = Column(Boolean, default=True)
    effective_date = Column(DATE, default=datetime.date.today)
    end_date = Column(DATE)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facility = relationship("Facility", back_populates="financial_classes")
    standard_payer = relationship("StandardPayer", back_populates="financial_classes")
    hcc_type = relationship("HccType", back_populates="financial_classes")

class ClinicalDepartment(Base):
    """ edi.clinical_departments table """
    __tablename__ = 'clinical_departments'
    __table_args__ = (
        UniqueConstraint('clinical_department_code', 'facility_id', name='uq_clinical_dept_code_facility'),
        {'schema': 'edi'}
    )

    department_id = Column(Integer, primary_key=True, autoincrement=True)
    clinical_department_code = Column(String(20), nullable=False)
    department_description = Column(String(200), nullable=False)
    facility_id = Column(String(50), ForeignKey('edi.facilities.facility_id'))
    department_type = Column(String(50))
    revenue_code = Column(String(10))
    cost_center = Column(String(20))
    specialty_code = Column(String(10))
    is_surgical = Column(Boolean, default=False)
    is_emergency = Column(Boolean, default=False)
    is_critical_care = Column(Boolean, default=False)
    requires_prior_auth = Column(Boolean, default=False)
    default_place_of_service = Column(String(2))
    billing_provider_npi = Column(String(10))
    department_manager = Column(String(100))
    phone_extension = Column(String(10))
    active = Column(Boolean, default=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facility = relationship("Facility", back_populates="clinical_departments")

class Filter(Base):
    """ edi.filters table """
    __tablename__ = 'filters'
    __table_args__ = (
        UniqueConstraint('filter_name', 'version', name='uq_filter_name_version'),
        {'schema': 'edi'}
    )

    filter_id = Column(Integer, primary_key=True, autoincrement=True)
    filter_name = Column(String(100), nullable=False)
    version = Column(Integer, default=1, nullable=False)
    rule_definition = Column(Text, nullable=False)
    description = Column(Text)
    rule_type = Column(String(50), default='DATALOG') # CHECK constraint in DB
    parameters = Column(JSONB)
    output_schema = Column(JSONB)
    is_active = Column(Boolean, default=True, nullable=False)
    is_latest_version = Column(Boolean, default=True, nullable=False)
    created_by = Column(String(100))
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_by = Column(String(100))
    updated_date = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

# --- Staging Schema ---

class StagingClaim(Base):
    """ staging.claims table """
    __tablename__ = 'claims'
    __table_args__ = {'schema': 'staging'}

    claim_id = Column(String(50), primary_key=True)
    facility_id = Column(String(50)) # FK to edi.facilities to be validated by app
    department_id = Column(Integer) # FK to edi.clinical_departments to be validated by app
    financial_class_id = Column(String(50)) # FK to edi.financial_classes to be validated by app
    patient_id = Column(String(50))
    patient_age = Column(Integer)
    patient_dob = Column(DATE)
    patient_sex = Column(CHAR(1))
    patient_account_number = Column(String(50))
    provider_id = Column(String(50))
    provider_type = Column(String(100))
    rendering_provider_npi = Column(String(10))
    place_of_service = Column(String(10))
    service_date = Column(DATE)
    total_charge_amount = Column(DECIMAL(12, 2))
    total_claim_charges = Column(DECIMAL(10, 2))
    payer_name = Column(String(100))
    primary_insurance_id = Column(String(50))
    secondary_insurance_id = Column(String(50))
    processing_status = Column(String(30), default='PENDING')
    processed_date = Column(DateTime)
    claim_data = Column(JSONB) # Full raw claim data
    facility_validated = Column(Boolean, default=False)
    department_validated = Column(Boolean, default=False)
    financial_class_validated = Column(Boolean, default=False)
    validation_errors = Column(ARRAY(Text))
    validation_warnings = Column(ARRAY(Text))
    datalog_rule_outcomes = Column(JSONB)
    created_by = Column(String(100))
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    exported_to_production = Column(Boolean, default=False)
    export_date = Column(DateTime)

    cms1500_diagnoses = relationship("StagingCMS1500Diagnosis", back_populates="claim", cascade="all, delete-orphan")
    cms1500_line_items = relationship("StagingCMS1500LineItem", back_populates="claim", cascade="all, delete-orphan")
    legacy_diagnoses = relationship("StagingDiagnosis", back_populates="claim", cascade="all, delete-orphan")
    legacy_procedures = relationship("StagingProcedure", back_populates="claim", cascade="all, delete-orphan")
    validation_results = relationship("StagingValidationResult", back_populates="claim", cascade="all, delete-orphan")
    claim_batches = relationship("StagingClaimBatch", back_populates="claim", cascade="all, delete-orphan")

class StagingCMS1500Diagnosis(Base):
    """ staging.cms1500_diagnoses table """
    __tablename__ = 'cms1500_diagnoses'
    __table_args__ = (
        UniqueConstraint('claim_id', 'diagnosis_sequence', name='uq_cms1500_diag_claim_seq'),
        {'schema': 'staging'}
    )

    diagnosis_entry_id = Column(Integer, primary_key=True, autoincrement=True)
    claim_id = Column(String(50), ForeignKey('staging.claims.claim_id', ondelete='CASCADE'), nullable=False)
    diagnosis_sequence = Column(Integer, nullable=False) # CHECK (1-12)
    icd_code = Column(String(10), nullable=False)
    icd_code_type = Column(String(10), default='ICD10')
    diagnosis_description = Column(Text)
    is_primary = Column(Boolean, default=False)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    claim = relationship("StagingClaim", back_populates="cms1500_diagnoses")

class StagingCMS1500LineItem(Base):
    """ staging.cms1500_line_items table """
    __tablename__ = 'cms1500_line_items'
    __table_args__ = (
        UniqueConstraint('claim_id', 'line_number', name='uq_cms1500_line_claim_num'),
        {'schema': 'staging'}
    )

    line_item_id = Column(Integer, primary_key=True, autoincrement=True)
    claim_id = Column(String(50), ForeignKey('staging.claims.claim_id', ondelete='CASCADE'), nullable=False)
    line_number = Column(Integer, nullable=False) # CHECK (1-99)
    service_date_from = Column(DATE)
    service_date_to = Column(DATE)
    place_of_service_code = Column(String(2))
    emergency_indicator = Column(CHAR(1))
    cpt_code = Column(String(10), nullable=False)
    modifier_1 = Column(String(2))
    modifier_2 = Column(String(2))
    modifier_3 = Column(String(2))
    modifier_4 = Column(String(2))
    procedure_description = Column(Text)
    diagnosis_pointers = Column(String(10))
    line_charge_amount = Column(DECIMAL(10, 2), nullable=False)
    units = Column(Integer, default=1)
    epsdt_family_plan = Column(CHAR(1))
    rendering_provider_id_qualifier = Column(String(2))
    rendering_provider_id = Column(String(20))
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    claim = relationship("StagingClaim", back_populates="cms1500_line_items")

class StagingDiagnosis(Base): # Legacy
    """ staging.diagnoses table (Legacy) """
    __tablename__ = 'diagnoses'
    __table_args__ = {'schema': 'staging'}

    diagnosis_id = Column(Integer, primary_key=True, autoincrement=True)
    claim_id = Column(String(50), ForeignKey('staging.claims.claim_id', ondelete='CASCADE'))
    diagnosis_sequence = Column(Integer)
    diagnosis_code = Column(String(20))
    diagnosis_type = Column(String(20))
    description = Column(Text)
    is_principal = Column(Boolean, default=False)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    claim = relationship("StagingClaim", back_populates="legacy_diagnoses")

class StagingProcedure(Base): # Legacy
    """ staging.procedures table (Legacy) """
    __tablename__ = 'procedures'
    __table_args__ = {'schema': 'staging'}

    procedure_id = Column(Integer, primary_key=True, autoincrement=True)
    claim_id = Column(String(50), ForeignKey('staging.claims.claim_id', ondelete='CASCADE'))
    procedure_sequence = Column(Integer)
    procedure_code = Column(String(20))
    procedure_type = Column(String(20))
    description = Column(Text)
    charge_amount = Column(DECIMAL(10, 2))
    service_date = Column(DATE)
    diagnosis_pointers = Column(String(10))
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    claim = relationship("StagingClaim", back_populates="legacy_procedures")

class StagingValidationResult(Base):
    """ staging.validation_results table """
    __tablename__ = 'validation_results'
    __table_args__ = {'schema': 'staging'}

    result_id = Column(Integer, primary_key=True, autoincrement=True) # Changed to Integer as BIGSERIAL is not standard SQLA type here
    claim_id = Column(String(50), ForeignKey('staging.claims.claim_id', ondelete='CASCADE'), nullable=False)
    validation_type = Column(String(50))
    validation_status = Column(String(20), nullable=False)
    validation_details = Column(JSONB)
    predicted_filters = Column(JSONB)
    ml_inference_time = Column(DECIMAL(10, 6))
    rule_engine_time = Column(DECIMAL(10, 6))
    processing_time = Column(DECIMAL(10, 6))
    error_message = Column(Text)
    facility_validation_details = Column(JSONB)
    model_version = Column(String(50))
    rule_version = Column(String(50))
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

    claim = relationship("StagingClaim", back_populates="validation_results")

# --- Staging Cache Tables ---
class StagingFacilitiesCache(Base):
    __tablename__ = 'facilities_cache'
    __table_args__ = {'schema': 'staging'}
    facility_id = Column(String(50), primary_key=True)
    facility_name = Column(String(200))
    facility_type = Column(String(20))
    critical_access = Column(CHAR(1))
    organization_id = Column(Integer)
    region_id = Column(Integer)
    active = Column(Boolean)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

class StagingDepartmentsCache(Base):
    __tablename__ = 'departments_cache'
    __table_args__ = {'schema': 'staging'}
    department_id = Column(Integer, primary_key=True)
    clinical_department_code = Column(String(20))
    department_description = Column(String(200))
    facility_id = Column(String(50)) # FK to staging.facilities_cache
    department_type = Column(String(50))
    default_place_of_service = Column(String(2))
    is_surgical = Column(Boolean)
    is_emergency = Column(Boolean)
    is_critical_care = Column(Boolean)
    active = Column(Boolean)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

class StagingFinancialClassesCache(Base):
    __tablename__ = 'financial_classes_cache'
    __table_args__ = {'schema': 'staging'}
    financial_class_id = Column(String(50), primary_key=True)
    financial_class_description = Column(String(200))
    facility_id = Column(String(50)) # FK to staging.facilities_cache
    standard_payer_id = Column(Integer) # FK to staging.standard_payers_cache
    payer_priority = Column(Integer)
    requires_authorization = Column(Boolean)
    active = Column(Boolean)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

class StagingStandardPayersCache(Base):
    __tablename__ = 'standard_payers_cache'
    __table_args__ = {'schema': 'staging'}
    standard_payer_id = Column(Integer, primary_key=True)
    standard_payer_code = Column(String(20))
    standard_payer_name = Column(String(100))
    payer_category = Column(String(50))
    active = Column(Boolean)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

# --- Staging Processing Tracking Tables ---
class StagingProcessingBatch(Base):
    __tablename__ = 'processing_batches'
    __table_args__ = {'schema': 'staging'}
    batch_id = Column(Integer, primary_key=True, autoincrement=True)
    batch_name = Column(String(100))
    status = Column(String(20), default='PENDING')
    total_claims = Column(Integer, default=0)
    processed_claims = Column(Integer, default=0)
    successful_claims = Column(Integer, default=0)
    failed_claims = Column(Integer, default=0)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    exported_to_production = Column(Boolean, default=False)

    claims_in_batch = relationship("StagingClaimBatch", back_populates="batch", cascade="all, delete-orphan")

class StagingClaimBatch(Base):
    __tablename__ = 'claim_batches'
    __table_args__ = (
        UniqueConstraint('claim_id', 'batch_id', name='pk_claim_batches'),
        {'schema': 'staging'}
    )
    claim_id = Column(String(50), ForeignKey('staging.claims.claim_id', ondelete='CASCADE'), primary_key=True)
    batch_id = Column(Integer, ForeignKey('staging.processing_batches.batch_id', ondelete='CASCADE'), primary_key=True)
    assigned_date = Column(DateTime, default=datetime.datetime.utcnow)

    claim = relationship("StagingClaim", back_populates="claim_batches")
    batch = relationship("StagingProcessingBatch", back_populates="claims_in_batch")

class StagingProcessingError(Base):
    __tablename__ = 'processing_errors'
    __table_args__ = {'schema': 'staging'}
    error_id = Column(Integer, primary_key=True, autoincrement=True) # Changed to Integer
    claim_id = Column(String(50)) # Not a FK, as claim might not exist or error is general
    batch_id = Column(Integer)    # Not a FK
    error_type = Column(String(50))
    error_message = Column(Text)
    error_details = Column(JSONB)
    stack_trace = Column(Text)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

class StagingExportLog(Base):
    __tablename__ = 'export_log'
    __table_args__ = {'schema': 'staging'}
    export_id = Column(Integer, primary_key=True, autoincrement=True) # Changed to Integer
    export_type = Column(String(50))
    record_count = Column(Integer)
    export_status = Column(String(20))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    error_message = Column(Text)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)

# --- EDI Schema - Parser related tables ---
class ProcessedChunk(Base):
    """ edi.processed_chunks table """
    __tablename__ = 'processed_chunks'
    __table_args__ = {'schema': 'edi'}

    chunk_id = Column(Integer, primary_key=True)
    status = Column(String(20), default='COMPLETED')
    processed_date = Column(DateTime, default=datetime.datetime.utcnow)
    claims_count = Column(Integer, default=0)
    processing_duration_seconds = Column(DECIMAL(10,3), default=0)
    error_message = Column(Text)

class RetryQueue(Base):
    """ edi.retry_queue table """
    __tablename__ = 'retry_queue'
    __table_args__ = {'schema': 'edi'}

    chunk_id = Column(Integer, primary_key=True)
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    last_retry_date = Column(DateTime)
    resolved = Column(Boolean, default=False)

# Example of how to create an engine and tables (typically done in a db_setup script)
# if __name__ == '__main__':
#     # Replace with your actual connection string from config
#     engine = create_engine('postgresql://user:password@host:port/edi_staging')
#     Base.metadata.create_all(engine)
#     print("PostgreSQL tables created (if they didn't exist).")