-- PostgreSQL Staging Database Schema for Claims Processing
-- ========================================================
-- This database is used for staging claims data, reference data caches,
-- and core facility/organization master data.
CREATE DATABASE edi_staging
    WITH
    OWNER = postgres -- Or another appropriate owner
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
-- Create edi schema for core master data (organizations, facilities, etc.)
CREATE SCHEMA IF NOT EXISTS edi;

-- Create staging schema for claims to be processed and reference data caches
CREATE SCHEMA IF NOT EXISTS staging;

-- ===========================
-- EDI SCHEMA - MASTER DATA TABLES
-- (Based on usage in facility_handler.py and production_facility_handler.py)
-- ===========================

-- Organizations table
CREATE TABLE edi.organizations (
    organization_id SERIAL PRIMARY KEY,
    organization_name VARCHAR(200) NOT NULL,
    organization_code VARCHAR(20) UNIQUE,
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    city VARCHAR(100),
    state_code CHAR(2),
    zip_code VARCHAR(10),
    country_code CHAR(3) DEFAULT 'USA',
    phone VARCHAR(20),
    email VARCHAR(255),
    website VARCHAR(255),
    tax_id VARCHAR(20),
    active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE edi.organizations IS 'Core healthcare organizations master data.';

-- Regions table
CREATE TABLE edi.regions (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(200) NOT NULL,
    region_code VARCHAR(20) UNIQUE,
    description TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE edi.regions IS 'Geographic or administrative regions.';

-- Organization-Region mapping table
CREATE TABLE edi.organization_regions (
    organization_id INTEGER REFERENCES edi.organizations(organization_id) ON DELETE CASCADE,
    region_id INTEGER REFERENCES edi.regions(region_id) ON DELETE CASCADE,
    is_primary BOOLEAN DEFAULT FALSE,
    effective_date DATE DEFAULT CURRENT_DATE,
    end_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_id, region_id)
);
COMMENT ON TABLE edi.organization_regions IS 'Many-to-many relationship between organizations and regions.';

-- Facilities table
CREATE TABLE edi.facilities (
    facility_id VARCHAR(50) PRIMARY KEY, -- User-defined facility ID
    facility_name VARCHAR(200) NOT NULL,
    facility_code VARCHAR(20) UNIQUE,
    organization_id INTEGER REFERENCES edi.organizations(organization_id),
    region_id INTEGER REFERENCES edi.regions(region_id),
    city VARCHAR(100),
    state_code CHAR(2),
    zip_code VARCHAR(10),
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    bed_size INTEGER CHECK (bed_size >= 0),
    fiscal_reporting_month INTEGER CHECK (fiscal_reporting_month BETWEEN 1 AND 12),
    emr_system VARCHAR(100),
    facility_type VARCHAR(20) CHECK (facility_type IN ('Teaching', 'Non-Teaching', 'Unknown')),
    critical_access CHAR(1) CHECK (critical_access IN ('Y', 'N', 'U')), -- U for Unknown
    phone VARCHAR(20),
    fax VARCHAR(20),
    email VARCHAR(255),
    npi_number VARCHAR(10),
    tax_id VARCHAR(20),
    license_number VARCHAR(50),
    active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE edi.facilities IS 'Master data for healthcare facilities.';

-- Standard Payers lookup table
CREATE TABLE edi.standard_payers (
    standard_payer_id SERIAL PRIMARY KEY,
    standard_payer_code VARCHAR(20) UNIQUE NOT NULL,
    standard_payer_name VARCHAR(100) NOT NULL,
    payer_category VARCHAR(50) CHECK (payer_category IN ('Government', 'Commercial', 'Managed Care', 'Self Pay', 'Workers Comp', 'Other')),
    description TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE edi.standard_payers IS 'Standardized payer types master data.';

-- HCC (Hierarchical Condition Categories) types
CREATE TABLE edi.hcc_types (
    hcc_type_id SERIAL PRIMARY KEY,
    hcc_code VARCHAR(10) UNIQUE NOT NULL,
    hcc_name VARCHAR(100) NOT NULL,
    hcc_agency VARCHAR(10) CHECK (hcc_agency IN ('CMS', 'HHS', 'Other')),
    description TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE edi.hcc_types IS 'HCC types master data.';

-- Financial Classes table
CREATE TABLE edi.financial_classes (
    financial_class_id VARCHAR(50) PRIMARY KEY, -- User-defined ID, potentially composite like FC_PAYERCODE_FACILITYID
    financial_class_description VARCHAR(200) NOT NULL,
    standard_payer_id INTEGER REFERENCES edi.standard_payers(standard_payer_id),
    hcc_type_id INTEGER REFERENCES edi.hcc_types(hcc_type_id),
    facility_id VARCHAR(50) REFERENCES edi.facilities(facility_id),
    payer_priority INTEGER DEFAULT 1,
    requires_authorization BOOLEAN DEFAULT FALSE,
    authorization_required_services TEXT, -- Consider JSONB if complex, or separate table
    copay_amount DECIMAL(8,2),
    deductible_amount DECIMAL(10,2),
    claim_submission_format VARCHAR(20),
    days_to_file_claim INTEGER DEFAULT 365,
    accepts_electronic_claims BOOLEAN DEFAULT TRUE,
    active BOOLEAN DEFAULT TRUE,
    effective_date DATE DEFAULT CURRENT_DATE,
    end_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE edi.financial_classes IS 'Facility-specific financial classes master data.';

-- Clinical Departments table
CREATE TABLE edi.clinical_departments (
    department_id SERIAL PRIMARY KEY,
    clinical_department_code VARCHAR(20) NOT NULL,
    department_description VARCHAR(200) NOT NULL,
    facility_id VARCHAR(50) REFERENCES edi.facilities(facility_id),
    department_type VARCHAR(50), -- Inpatient, Outpatient, Emergency, etc.
    revenue_code VARCHAR(10),
    cost_center VARCHAR(20),
    specialty_code VARCHAR(10),
    is_surgical BOOLEAN DEFAULT FALSE,
    is_emergency BOOLEAN DEFAULT FALSE,
    is_critical_care BOOLEAN DEFAULT FALSE,
    requires_prior_auth BOOLEAN DEFAULT FALSE,
    default_place_of_service VARCHAR(2),
    billing_provider_npi VARCHAR(10),
    department_manager VARCHAR(100),
    phone_extension VARCHAR(10),
    active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(clinical_department_code, facility_id)
);
COMMENT ON TABLE edi.clinical_departments IS 'Clinical departments master data within facilities.';

-- Table for filters (used by rule_generator.py and validator.py)
-- Enhanced for versioning and different rule types (including Datalog)
CREATE TABLE edi.filters (
    filter_id SERIAL PRIMARY KEY,
    filter_name VARCHAR(100) NOT NULL, -- Name of the rule or rule set
    version INTEGER DEFAULT 1 NOT NULL, -- Version of the rule
    rule_definition TEXT NOT NULL, -- Datalog rule, Python code, or other definition
    description TEXT,
    rule_type VARCHAR(50) DEFAULT 'DATALOG' CHECK (rule_type IN ('DATALOG', 'PYTHON_VALIDATION', 'ML_POSTPROCESSING', 'OTHER')), -- Type of rule
    parameters JSONB, -- Parameters for the rule, e.g., thresholds
    output_schema JSONB, -- Expected output or action schema
    is_active BOOLEAN DEFAULT TRUE NOT NULL, -- Whether this specific version is active
    is_latest_version BOOLEAN DEFAULT TRUE NOT NULL, -- Indicates if this is the latest version of this filter_name
    created_by VARCHAR(100),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(100),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(filter_name, version) -- Ensure unique version per filter name
);
COMMENT ON TABLE edi.filters IS 'Stores validation rules, Datalog rules, and other programmable filters with versioning.';
COMMENT ON COLUMN edi.filters.rule_type IS 'Type of rule: DATALOG for pyDatalog, PYTHON_VALIDATION for claims_validation_engine rules, etc.';
COMMENT ON COLUMN edi.filters.is_latest_version IS 'True if this is the most recent version of the rule with this filter_name.';

-- Ensure only one 'is_latest_version' per filter_name (requires a more complex constraint or trigger,
-- or application-level logic. For simplicity in SQL, this might be managed by the application).
/*
CREATE OR REPLACE FUNCTION edi.ensure_single_latest_version()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.is_latest_version = TRUE THEN
        UPDATE edi.filters
        SET is_latest_version = FALSE
        WHERE filter_name = NEW.filter_name AND filter_id != NEW.filter_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_filters_single_latest_version
BEFORE INSERT OR UPDATE ON edi.filters
FOR EACH ROW
EXECUTE FUNCTION edi.ensure_single_latest_version();
*/


-- ===========================
-- STAGING SCHEMA - CLAIMS PROCESSING AND CACHES
-- ===========================

-- Staging claims table (main table for claims to be processed)
CREATE TABLE staging.claims (
    claim_id VARCHAR(50) PRIMARY KEY,
    facility_id VARCHAR(50), -- References edi.facilities(facility_id) - to be validated
    department_id INTEGER,   -- References edi.clinical_departments(department_id) - to be validated
    financial_class_id VARCHAR(50), -- References edi.financial_classes(financial_class_id) - to be validated
    patient_id VARCHAR(50),
    patient_age INTEGER,
    patient_dob DATE,
    patient_sex CHAR(1),
    patient_account_number VARCHAR(50),
    provider_id VARCHAR(50), -- Billing provider from claim
    provider_type VARCHAR(100), -- Type of billing provider
    rendering_provider_npi VARCHAR(10), -- Rendering provider NPI if different
    place_of_service VARCHAR(10), -- From claim
    service_date DATE,
    total_charge_amount DECIMAL(12,2), -- Sum of line item charges
    total_claim_charges DECIMAL(10,2), -- Overall claim charges from source
    payer_name VARCHAR(100),
    primary_insurance_id VARCHAR(50),
    secondary_insurance_id VARCHAR(50),
    processing_status VARCHAR(30) DEFAULT 'PENDING', -- PENDING, PROCESSING, VALIDATED, VALIDATION_FAILED, ML_PREDICTED, DATALOG_RULES_APPLIED, REIMBURSEMENT_CALCULATED, COMPLETED, ERROR, RETRY
    processed_date TIMESTAMP,
    claim_data JSONB, -- Full raw claim data if needed
    facility_validated BOOLEAN DEFAULT FALSE,
    department_validated BOOLEAN DEFAULT FALSE,
    financial_class_validated BOOLEAN DEFAULT FALSE,
    validation_errors TEXT[],
    validation_warnings TEXT[],
    datalog_rule_outcomes JSONB, -- To store outcomes from Datalog engine
    created_by VARCHAR(100), -- User or system that created the claim
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    exported_to_production BOOLEAN DEFAULT FALSE,
    export_date TIMESTAMP
);
COMMENT ON TABLE staging.claims IS 'Staging table for claims with facility assignments to be validated.';
COMMENT ON COLUMN staging.claims.processing_status IS 'Current status of the claim in the processing pipeline.';
COMMENT ON COLUMN staging.claims.datalog_rule_outcomes IS 'Stores outcomes from the Datalog rule engine for this claim.';


-- Staging CMS 1500 diagnosis codes
CREATE TABLE staging.cms1500_diagnoses (
    diagnosis_entry_id SERIAL PRIMARY KEY,
    claim_id VARCHAR(50) REFERENCES staging.claims(claim_id) ON DELETE CASCADE,
    diagnosis_sequence INTEGER NOT NULL CHECK (diagnosis_sequence BETWEEN 1 AND 12),
    icd_code VARCHAR(10) NOT NULL,
    icd_code_type VARCHAR(10) DEFAULT 'ICD10',
    diagnosis_description TEXT,
    is_primary BOOLEAN DEFAULT FALSE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(claim_id, diagnosis_sequence)
);
COMMENT ON TABLE staging.cms1500_diagnoses IS 'Staging for CMS 1500 diagnosis codes (Section 21).';

-- Staging CMS 1500 procedures/line items
CREATE TABLE staging.cms1500_line_items (
    line_item_id SERIAL PRIMARY KEY,
    claim_id VARCHAR(50) REFERENCES staging.claims(claim_id) ON DELETE CASCADE,
    line_number INTEGER NOT NULL CHECK (line_number BETWEEN 1 AND 99),
    service_date_from DATE,
    service_date_to DATE,
    place_of_service_code VARCHAR(2),
    emergency_indicator CHAR(1),
    cpt_code VARCHAR(10) NOT NULL,
    modifier_1 VARCHAR(2),
    modifier_2 VARCHAR(2),
    modifier_3 VARCHAR(2),
    modifier_4 VARCHAR(2),
    procedure_description TEXT, -- Description of the procedure/service
    diagnosis_pointers VARCHAR(10), -- e.g., "1,2"
    line_charge_amount DECIMAL(10,2) NOT NULL,
    units INTEGER DEFAULT 1,
    epsdt_family_plan CHAR(1), -- EPSDT indicator
    rendering_provider_id_qualifier VARCHAR(2),
    rendering_provider_id VARCHAR(20),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(claim_id, line_number)
);
COMMENT ON TABLE staging.cms1500_line_items IS 'Staging for CMS 1500 line items (Sections 24A-J).';

-- Legacy diagnosis and procedure tables (if still needed for backward compatibility during transition)
-- Consider migrating data from these to cms1500_diagnoses and cms1500_line_items if they represent the same data.
CREATE TABLE staging.diagnoses (
    diagnosis_id SERIAL PRIMARY KEY,
    claim_id VARCHAR(50) REFERENCES staging.claims(claim_id) ON DELETE CASCADE,
    diagnosis_sequence INTEGER,
    diagnosis_code VARCHAR(20), -- Generic diagnosis code
    diagnosis_type VARCHAR(20), -- e.g., ICD9, ICD10
    description TEXT,
    is_principal BOOLEAN DEFAULT FALSE, -- Use is_primary in cms1500_diagnoses
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.diagnoses IS 'Legacy staging for diagnosis codes; prefer cms1500_diagnoses.';

CREATE TABLE staging.procedures (
    procedure_id SERIAL PRIMARY KEY,
    claim_id VARCHAR(50) REFERENCES staging.claims(claim_id) ON DELETE CASCADE,
    procedure_sequence INTEGER,
    procedure_code VARCHAR(20), -- Generic procedure code
    procedure_type VARCHAR(20), -- e.g., CPT, HCPCS
    description TEXT,
    charge_amount DECIMAL(10,2),
    service_date DATE,
    diagnosis_pointers VARCHAR(10),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.procedures IS 'Legacy staging for procedure codes; prefer cms1500_line_items.';


-- Validation results table
CREATE TABLE staging.validation_results (
    result_id BIGSERIAL PRIMARY KEY,
    claim_id VARCHAR(50) NOT NULL REFERENCES staging.claims(claim_id) ON DELETE CASCADE,
    validation_type VARCHAR(50), -- 'FACILITY', 'MEDICAL', 'BUSINESS_RULES', 'ML_FILTER', 'DATALOG_RULE'
    validation_status VARCHAR(20) NOT NULL, -- 'PENDING', 'PASSED', 'FAILED', 'WARNING'
    validation_details JSONB, -- Detailed results, rule outcomes
    predicted_filters JSONB, -- Filters predicted by ML model
    ml_inference_time DECIMAL(10,6), -- Time for ML prediction
    rule_engine_time DECIMAL(10,6), -- Time for Datalog rule evaluation
    processing_time DECIMAL(10,6), -- Total validation time for this claim
    error_message TEXT,
    facility_validation_details JSONB, -- Specific facility validation results
    model_version VARCHAR(50), -- Version of ML model used
    rule_version VARCHAR(50), -- Version of Datalog/Validation rule used
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.validation_results IS 'Stores results of claim validation, including ML and rule outcomes.';
COMMENT ON COLUMN staging.validation_results.rule_version IS 'Version of the Datalog rule or other validation rule used.';

-- ===========================
-- REFERENCE DATA CACHE TABLES (STAGING SCHEMA)
-- These tables cache reference data from SQL Server (or edi.* master tables) for validation purposes
-- ===========================

CREATE TABLE staging.facilities_cache (
    facility_id VARCHAR(50) PRIMARY KEY,
    facility_name VARCHAR(200),
    facility_type VARCHAR(20),
    critical_access CHAR(1),
    organization_id INTEGER, -- references edi.organizations
    region_id INTEGER,       -- references edi.regions
    active BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.facilities_cache IS 'Read-only cache of facility data from SQL Server or edi.facilities for validation.';

CREATE TABLE staging.departments_cache (
    department_id INTEGER PRIMARY KEY,
    clinical_department_code VARCHAR(20),
    department_description VARCHAR(200),
    facility_id VARCHAR(50), -- references staging.facilities_cache
    department_type VARCHAR(50),
    default_place_of_service VARCHAR(2),
    is_surgical BOOLEAN,
    is_emergency BOOLEAN,
    is_critical_care BOOLEAN,
    active BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.departments_cache IS 'Read-only cache of department data for validation.';

CREATE TABLE staging.financial_classes_cache (
    financial_class_id VARCHAR(50) PRIMARY KEY,
    financial_class_description VARCHAR(200),
    facility_id VARCHAR(50), -- references staging.facilities_cache
    standard_payer_id INTEGER, -- references staging.standard_payers_cache
    payer_priority INTEGER,
    requires_authorization BOOLEAN,
    active BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.financial_classes_cache IS 'Read-only cache of financial class data for validation.';

CREATE TABLE staging.standard_payers_cache (
    standard_payer_id INTEGER PRIMARY KEY,
    standard_payer_code VARCHAR(20),
    standard_payer_name VARCHAR(100),
    payer_category VARCHAR(50),
    active BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.standard_payers_cache IS 'Read-only cache of standard payer data for validation.';

-- ===========================
-- PROCESSING TRACKING TABLES (STAGING SCHEMA)
-- ===========================

CREATE TABLE staging.processing_batches (
    batch_id SERIAL PRIMARY KEY,
    batch_name VARCHAR(100),
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, PROCESSING, COMPLETED, FAILED
    total_claims INTEGER DEFAULT 0,
    processed_claims INTEGER DEFAULT 0,
    successful_claims INTEGER DEFAULT 0,
    failed_claims INTEGER DEFAULT 0,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    exported_to_production BOOLEAN DEFAULT FALSE
);
COMMENT ON TABLE staging.processing_batches IS 'Tracks batches of claims being processed.';

CREATE TABLE staging.claim_batches (
    claim_id VARCHAR(50) REFERENCES staging.claims(claim_id) ON DELETE CASCADE,
    batch_id INTEGER REFERENCES staging.processing_batches(batch_id) ON DELETE CASCADE,
    assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (claim_id, batch_id)
);
COMMENT ON TABLE staging.claim_batches IS 'Assigns claims to processing batches.';

CREATE TABLE staging.processing_errors (
    error_id BIGSERIAL PRIMARY KEY,
    claim_id VARCHAR(50),
    batch_id INTEGER,
    error_type VARCHAR(50),
    error_message TEXT,
    error_details JSONB,
    stack_trace TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.processing_errors IS 'Logs errors encountered during claim processing.';

CREATE TABLE staging.export_log (
    export_id BIGSERIAL PRIMARY KEY,
    export_type VARCHAR(50), -- 'CLAIMS', 'VALIDATION_RESULTS', 'BATCH'
    record_count INTEGER,
    export_status VARCHAR(20), -- 'SUCCESS', 'FAILED', 'PARTIAL'
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    error_message TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE staging.export_log IS 'Tracks data export operations to the production database.';

-- Table for processed chunks (used by parser.py)
CREATE TABLE edi.processed_chunks (
    chunk_id INTEGER PRIMARY KEY,
    status VARCHAR(20) DEFAULT 'COMPLETED', -- COMPLETED, FAILED
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    claims_count INTEGER DEFAULT 0,
    processing_duration_seconds DECIMAL(10,3) DEFAULT 0,
    error_message TEXT
);
COMMENT ON TABLE edi.processed_chunks IS 'Tracks the processing status of claim chunks.';

-- Table for retry queue (used by parser.py)
CREATE TABLE edi.retry_queue (
    chunk_id INTEGER PRIMARY KEY,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_retry_date TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE
);
COMMENT ON TABLE edi.retry_queue IS 'Queue for chunks that failed processing and need retrying.';


-- ===========================
-- INDEXES FOR PERFORMANCE
-- ===========================

-- edi schema indexes
CREATE INDEX idx_organizations_active ON edi.organizations(active);
CREATE INDEX idx_regions_active ON edi.regions(active);
CREATE INDEX idx_facilities_organization_id ON edi.facilities(organization_id);
CREATE INDEX idx_facilities_active ON edi.facilities(active);
CREATE INDEX idx_standard_payers_active ON edi.standard_payers(active);
CREATE INDEX idx_financial_classes_facility_id ON edi.financial_classes(facility_id);
CREATE INDEX idx_clinical_departments_facility_id ON edi.clinical_departments(facility_id);
CREATE INDEX idx_edi_filters_active_type_latest ON edi.filters(filter_name, is_active, is_latest_version, rule_type);


-- staging.claims indexes
CREATE INDEX idx_staging_claims_facility_id ON staging.claims(facility_id);
CREATE INDEX idx_staging_claims_department_id ON staging.claims(department_id);
CREATE INDEX idx_staging_claims_financial_class_id ON staging.claims(financial_class_id);
CREATE INDEX idx_staging_claims_processing_status ON staging.claims(processing_status);
CREATE INDEX idx_staging_claims_service_date ON staging.claims(service_date);
CREATE INDEX idx_staging_claims_exported ON staging.claims(exported_to_production);
CREATE INDEX idx_staging_claims_validation_status ON staging.claims(facility_validated, department_validated, financial_class_validated);

-- staging.cms1500_diagnoses and staging.cms1500_line_items indexes
CREATE INDEX idx_staging_cms1500_diagnoses_claim_id ON staging.cms1500_diagnoses(claim_id);
CREATE INDEX idx_staging_cms1500_diagnoses_icd_code ON staging.cms1500_diagnoses(icd_code);
CREATE INDEX idx_staging_cms1500_line_items_claim_id ON staging.cms1500_line_items(claim_id);
CREATE INDEX idx_staging_cms1500_line_items_cpt_code ON staging.cms1500_line_items(cpt_code);

-- staging.validation_results indexes
CREATE INDEX idx_staging_validation_results_claim_id ON staging.validation_results(claim_id);
CREATE INDEX idx_staging_validation_results_type_status ON staging.validation_results(validation_type, validation_status);
CREATE INDEX idx_staging_validation_results_created_date ON staging.validation_results(created_date);

-- Reference cache indexes
CREATE INDEX idx_staging_facilities_cache_active ON staging.facilities_cache(active);
CREATE INDEX idx_staging_departments_cache_facility ON staging.departments_cache(facility_id);
CREATE INDEX idx_staging_financial_classes_cache_facility ON staging.financial_classes_cache(facility_id);

-- Processing tracking indexes
CREATE INDEX idx_staging_processing_batches_status ON staging.processing_batches(status);
CREATE INDEX idx_staging_claim_batches_batch_id ON staging.claim_batches(batch_id);
CREATE INDEX idx_staging_processing_errors_claim_id ON staging.processing_errors(claim_id);


-- ===========================
-- VIEWS FOR PROCESSING (STAGING SCHEMA)
-- ===========================

CREATE OR REPLACE VIEW staging.v_claims_for_validation AS
SELECT
    c.claim_id,
    c.facility_id,
    c.department_id,
    c.financial_class_id,
    c.patient_id,
    c.patient_age,
    c.patient_dob,
    c.patient_sex,
    c.patient_account_number,
    c.provider_id,
    c.provider_type,
    c.rendering_provider_npi,
    c.place_of_service,
    c.service_date,
    c.total_charge_amount,
    c.total_claim_charges,
    c.payer_name,
    c.processing_status,
    c.claim_data,
    c.created_date,
    COALESCE(fac_cache.facility_name, 'N/A') as facility_name,
    COALESCE(dept_cache.department_description, 'N/A') as department_description,
    COALESCE(fin_cache.financial_class_description, 'N/A') as financial_class_description,
    COALESCE(array_agg(DISTINCT diag.icd_code) FILTER (WHERE diag.icd_code IS NOT NULL), ARRAY[]::VARCHAR[]) as diagnosis_codes,
    COALESCE(array_agg(DISTINCT li.cpt_code) FILTER (WHERE li.cpt_code IS NOT NULL), ARRAY[]::VARCHAR[]) as procedure_codes,
    COUNT(DISTINCT diag.diagnosis_entry_id) as diagnosis_count,
    COUNT(DISTINCT li.line_item_id) as line_item_count
FROM staging.claims c
LEFT JOIN staging.facilities_cache fac_cache ON c.facility_id = fac_cache.facility_id
LEFT JOIN staging.departments_cache dept_cache ON c.department_id = dept_cache.department_id
LEFT JOIN staging.financial_classes_cache fin_cache ON c.financial_class_id = fin_cache.financial_class_id
LEFT JOIN staging.cms1500_diagnoses diag ON c.claim_id = diag.claim_id
LEFT JOIN staging.cms1500_line_items li ON c.claim_id = li.claim_id
WHERE c.processing_status = 'PENDING'
  AND c.exported_to_production = FALSE
GROUP BY c.claim_id, fac_cache.facility_name, dept_cache.department_description, fin_cache.financial_class_description;
COMMENT ON VIEW staging.v_claims_for_validation IS 'View for claims ready for validation, joining with cached reference data.';

CREATE OR REPLACE VIEW staging.v_facility_validation_status AS
SELECT
    c.claim_id,
    c.facility_id,
    c.department_id,
    c.financial_class_id,
    fac_cache.facility_name,
    fac_cache.active as facility_active,
    dept_cache.department_description,
    dept_cache.active as department_active,
    dept_cache.facility_id as department_facility_id, -- Facility ID from the department's own record
    fin_cache.financial_class_description,
    fin_cache.active as financial_class_active,
    fin_cache.facility_id as financial_class_facility_id, -- Facility ID from the financial class's own record
    CASE
        WHEN c.facility_id IS NULL THEN 'No facility assigned'
        WHEN fac_cache.facility_id IS NULL THEN 'Facility not found in cache'
        WHEN fac_cache.active = FALSE THEN 'Facility inactive in cache'
        ELSE 'Valid'
    END as facility_status,
    CASE
        WHEN c.department_id IS NOT NULL AND dept_cache.department_id IS NULL THEN 'Department not found in cache'
        WHEN c.department_id IS NOT NULL AND dept_cache.active = FALSE THEN 'Department inactive in cache'
        WHEN c.department_id IS NOT NULL AND dept_cache.facility_id != c.facility_id THEN 'Department facility mismatch'
        ELSE 'Valid'
    END as department_status,
    CASE
        WHEN c.financial_class_id IS NOT NULL AND fin_cache.financial_class_id IS NULL THEN 'Financial class not found in cache'
        WHEN c.financial_class_id IS NOT NULL AND fin_cache.active = FALSE THEN 'Financial class inactive in cache'
        WHEN c.financial_class_id IS NOT NULL AND fin_cache.facility_id != c.facility_id THEN 'Financial class facility mismatch'
        ELSE 'Valid'
    END as financial_class_status
FROM staging.claims c
LEFT JOIN staging.facilities_cache fac_cache ON c.facility_id = fac_cache.facility_id
LEFT JOIN staging.departments_cache dept_cache ON c.department_id = dept_cache.department_id
LEFT JOIN staging.financial_classes_cache fin_cache ON c.financial_class_id = fin_cache.financial_class_id;
COMMENT ON VIEW staging.v_facility_validation_status IS 'View for checking the validation status of facility, department, and financial class assignments on claims against cached reference data.';

CREATE OR REPLACE VIEW staging.v_processing_statistics AS
SELECT
    COUNT(*) as total_claims,
    COUNT(*) FILTER (WHERE processing_status = 'PENDING') as pending_claims,
    COUNT(*) FILTER (WHERE processing_status = 'PROCESSING') as processing_claims,
    COUNT(*) FILTER (WHERE processing_status = 'COMPLETED') as completed_claims,
    COUNT(*) FILTER (WHERE processing_status = 'ERROR') as error_claims,
    COUNT(*) FILTER (WHERE exported_to_production = TRUE) as exported_claims,
    COUNT(*) FILTER (WHERE facility_validated = TRUE) as facility_validated_claims,
    COUNT(*) FILTER (WHERE department_validated = TRUE) as department_validated_claims,
    COUNT(*) FILTER (WHERE financial_class_validated = TRUE) as financial_class_validated_claims,
    MIN(service_date) as earliest_service_date,
    MAX(service_date) as latest_service_date,
    SUM(total_charge_amount) as total_charges,
    AVG(total_charge_amount) as avg_charge_amount
FROM staging.claims;
COMMENT ON VIEW staging.v_processing_statistics IS 'Aggregated statistics about the claims in the staging table.';

-- ===========================
-- FUNCTIONS FOR STAGING OPERATIONS
-- ===========================

CREATE OR REPLACE FUNCTION staging.refresh_reference_cache()
RETURNS TEXT AS $$
DECLARE
    refresh_count INTEGER;
    result_message TEXT := '';
BEGIN
    -- This function is a placeholder. The actual synchronization logic
    -- (fetching from SQL Server or edi.* master tables and inserting into staging.*_cache tables)
    -- should be handled by the application layer (e.g., in StagingPostgreSQLHandler).
    -- This function can be called by the application after it has performed the sync.

    UPDATE staging.facilities_cache SET last_updated = CURRENT_TIMESTAMP;
    GET DIAGNOSTICS refresh_count = ROW_COUNT;
    result_message := result_message || 'Facilities cache: ' || refresh_count || ' rows marked as refreshed. ';

    UPDATE staging.departments_cache SET last_updated = CURRENT_TIMESTAMP;
    GET DIAGNOSTICS refresh_count = ROW_COUNT;
    result_message := result_message || 'Departments cache: ' || refresh_count || ' rows marked as refreshed. ';

    UPDATE staging.financial_classes_cache SET last_updated = CURRENT_TIMESTAMP;
    GET DIAGNOSTICS refresh_count = ROW_COUNT;
    result_message := result_message || 'Financial classes cache: ' || refresh_count || ' rows marked as refreshed. ';

    UPDATE staging.standard_payers_cache SET last_updated = CURRENT_TIMESTAMP;
    GET DIAGNOSTICS refresh_count = ROW_COUNT;
    result_message := result_message || 'Standard payers cache: ' || refresh_count || ' rows marked as refreshed.';

    RETURN result_message;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.refresh_reference_cache() IS 'Placeholder function to mark reference caches as updated. Actual data sync is application-driven.';

CREATE OR REPLACE FUNCTION staging.validate_claim_facility_assignments_func(p_claim_id VARCHAR(50))
RETURNS TABLE(
    claim_id_out VARCHAR(50),
    validation_status_out VARCHAR(20),
    errors_out TEXT[],
    warnings_out TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        v.claim_id,
        CASE
            WHEN v.facility_status != 'Valid'
                OR v.department_status != 'Valid'
                OR v.financial_class_status != 'Valid'
            THEN 'FAILED'
            ELSE 'PASSED'
        END as validation_status,
        ARRAY_REMOVE(ARRAY[
            CASE WHEN v.facility_status != 'Valid' THEN v.facility_status END,
            CASE WHEN v.department_status != 'Valid' THEN v.department_status END,
            CASE WHEN v.financial_class_status != 'Valid' THEN v.financial_class_status END
        ], NULL) as errors,
        ARRAY[]::TEXT[] as warnings -- Placeholder for warnings
    FROM staging.v_facility_validation_status v
    WHERE v.claim_id = p_claim_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.validate_claim_facility_assignments_func(VARCHAR(50)) IS 'Validates facility, department, and financial class assignments for a single claim using the cache view.';


CREATE OR REPLACE FUNCTION staging.mark_claims_validated(
    p_claim_ids TEXT[],
    p_facility_valid BOOLEAN DEFAULT TRUE,
    p_department_valid BOOLEAN DEFAULT TRUE,
    p_financial_class_valid BOOLEAN DEFAULT TRUE
)
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE staging.claims
    SET
        facility_validated = p_facility_valid,
        department_validated = p_department_valid,
        financial_class_validated = p_financial_class_valid,
        updated_date = CURRENT_TIMESTAMP
    WHERE claim_id = ANY(p_claim_ids);
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.mark_claims_validated(TEXT[], BOOLEAN, BOOLEAN, BOOLEAN) IS 'Marks specified claims with their facility validation statuses.';

CREATE OR REPLACE FUNCTION staging.create_processing_batch(
    p_batch_name VARCHAR(100),
    p_claim_ids TEXT[] DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    new_batch_id INTEGER;
    claim_count INTEGER;
BEGIN
    INSERT INTO staging.processing_batches (batch_name, status, start_time)
    VALUES (p_batch_name, 'PENDING', CURRENT_TIMESTAMP)
    RETURNING batch_id INTO new_batch_id;

    IF p_claim_ids IS NOT NULL THEN
        INSERT INTO staging.claim_batches (claim_id, batch_id)
        SELECT unnest(p_claim_ids), new_batch_id;
        claim_count := array_length(p_claim_ids, 1);
    ELSE
        INSERT INTO staging.claim_batches (claim_id, batch_id)
        SELECT c.claim_id, new_batch_id
        FROM staging.claims c
        WHERE c.processing_status = 'PENDING' AND c.exported_to_production = FALSE;
        GET DIAGNOSTICS claim_count = ROW_COUNT;
    END IF;

    UPDATE staging.processing_batches
    SET total_claims = claim_count
    WHERE batch_id = new_batch_id;

    RETURN new_batch_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.create_processing_batch(VARCHAR(100), TEXT[]) IS 'Creates a new processing batch and assigns claims to it.';

CREATE OR REPLACE FUNCTION staging.get_claims_for_export(
    p_limit INTEGER DEFAULT 1000
)
RETURNS TABLE(
    r_claim_id VARCHAR(50),
    r_facility_id VARCHAR(50),
    r_department_id INTEGER,
    r_financial_class_id VARCHAR(50), -- Changed from VARCHAR(20) to VARCHAR(50) to match staging.claims
    r_validation_status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.claim_id,
        c.facility_id,
        c.department_id,
        c.financial_class_id,
        CASE
            WHEN c.facility_validated = TRUE
                AND c.department_validated = TRUE -- Assuming department_validated exists or logic is adjusted
                AND c.financial_class_validated = TRUE -- Assuming financial_class_validated exists
            THEN 'VALIDATED'
            ELSE 'PENDING_VALIDATION'
        END as validation_status
    FROM staging.claims c
    WHERE c.processing_status = 'COMPLETED' -- Or a status indicating it's ready for export post all processing
      AND c.exported_to_production = FALSE
      AND c.facility_validated = TRUE -- Ensure facility part is validated at least
    ORDER BY c.updated_date
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.get_claims_for_export(INTEGER) IS 'Retrieves claims that are validated and ready for export to the production system.';

-- ===========================
-- TRIGGERS FOR AUTOMATIC UPDATES (STAGING SCHEMA)
-- ===========================

CREATE OR REPLACE FUNCTION staging.update_claims_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_date = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_staging_claims_updated_date
    BEFORE UPDATE ON staging.claims
    FOR EACH ROW
    EXECUTE FUNCTION staging.update_claims_timestamp();
COMMENT ON TRIGGER tr_staging_claims_updated_date ON staging.claims IS 'Automatically updates the updated_date on staging.claims modifications.';

CREATE OR REPLACE FUNCTION staging.log_validation_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.facility_validated IS DISTINCT FROM NEW.facility_validated
       OR OLD.department_validated IS DISTINCT FROM NEW.department_validated
       OR OLD.financial_class_validated IS DISTINCT FROM NEW.financial_class_validated THEN

        INSERT INTO staging.validation_results (
            claim_id, validation_type, validation_status, validation_details
        ) VALUES (
            NEW.claim_id,
            'FACILITY_STRUCTURE_VALIDATION', -- More specific type
            CASE
                WHEN NEW.facility_validated = TRUE
                    AND NEW.department_validated = TRUE
                    AND NEW.financial_class_validated = TRUE
                THEN 'PASSED'
                ELSE 'FAILED'
            END,
            jsonb_build_object(
                'facility_validated', NEW.facility_validated,
                'department_validated', NEW.department_validated,
                'financial_class_validated', NEW.financial_class_validated,
                'validation_errors', NEW.validation_errors,
                'validation_warnings', NEW.validation_warnings
            )
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_staging_claims_validation_log
    AFTER UPDATE ON staging.claims
    FOR EACH ROW
    EXECUTE FUNCTION staging.log_validation_changes();
COMMENT ON TRIGGER tr_staging_claims_validation_log ON staging.claims IS 'Logs changes to facility validation statuses into staging.validation_results.';

-- ===========================
-- STORED PROCEDURES FOR BATCH OPERATIONS (STAGING SCHEMA)
-- ===========================

CREATE OR REPLACE FUNCTION staging.cleanup_old_data(
    p_days_to_keep INTEGER DEFAULT 30
)
RETURNS TEXT AS $$
DECLARE
    deleted_claims INTEGER := 0;
    deleted_validations INTEGER := 0;
    deleted_batches INTEGER := 0;
    result_message TEXT;
BEGIN
    -- Delete old exported claims and related data (cascading deletes handle diagnoses, line_items, claim_batches)
    DELETE FROM staging.claims
    WHERE exported_to_production = TRUE
      AND export_date < (CURRENT_DATE - (p_days_to_keep || ' days')::interval);
    GET DIAGNOSTICS deleted_claims = ROW_COUNT;

    -- Delete old validation results for claims that no longer exist or are very old
    DELETE FROM staging.validation_results vr
    WHERE vr.created_date < (CURRENT_DATE - (p_days_to_keep || ' days')::interval)
      AND NOT EXISTS (SELECT 1 FROM staging.claims c WHERE c.claim_id = vr.claim_id);
    GET DIAGNOSTICS deleted_validations = ROW_COUNT;
    
    -- Delete old completed batches
    DELETE FROM staging.processing_batches pb
    WHERE pb.status = 'COMPLETED'
      AND pb.exported_to_production = TRUE
      AND pb.end_time < (CURRENT_DATE - (p_days_to_keep || ' days')::interval);
    GET DIAGNOSTICS deleted_batches = ROW_COUNT;

    result_message := format(
        'Cleanup completed: %s claims, %s validation results, %s batches deleted',
        deleted_claims, deleted_validations, deleted_batches
    );
    RETURN result_message;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.cleanup_old_data(INTEGER) IS 'Cleans up old processed and exported data from staging tables.';

-- ===========================
-- GRANTS AND PERMISSIONS
-- ===========================
-- Example grants, adjust user names and permissions as needed.
-- GRANT USAGE ON SCHEMA edi TO edi_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA edi TO edi_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA edi TO edi_app_user;
--
-- GRANT USAGE ON SCHEMA staging TO edi_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO edi_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO edi_app_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA staging TO edi_app_user;

-- End of PostgreSQL Staging and EDI Master Database Schema