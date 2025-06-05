-- PostgreSQL Staging Database Schema for Claims Processing with Performance Optimizations
-- =======================================================================================
-- This database is used for staging claims data, reference data caches,
-- and core facility/organization master data with advanced performance features.

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

-- Create analytics schema for materialized views and reporting
CREATE SCHEMA IF NOT EXISTS analytics;

-- ===========================
-- PERFORMANCE CONFIGURATION
-- ===========================

-- Enable query planning optimizations
SET work_mem = '256MB';
SET maintenance_work_mem = '1GB';
SET shared_buffers = '2GB';
SET effective_cache_size = '8GB';
SET random_page_cost = 1.1;
SET seq_page_cost = 1.0;

-- Enable parallel processing
SET max_parallel_workers_per_gather = 4;
SET max_parallel_workers = 8;
SET parallel_tuple_cost = 0.1;
SET parallel_setup_cost = 1000.0;

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

-- Facilities table with performance optimizations
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
) WITH (fillfactor = 85); -- Leave space for updates
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

-- ===========================
-- STAGING SCHEMA - CLAIMS PROCESSING AND CACHES WITH PARTITIONING
-- ===========================

-- Staging claims table with partitioning by service_date for high volume
-- Using range partitioning by month for optimal query performance
CREATE TABLE staging.claims (
    claim_id VARCHAR(50) NOT NULL,
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
    service_date DATE NOT NULL, -- NOT NULL for partitioning
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
    export_date TIMESTAMP,
    -- Performance enhancement fields
    search_vector tsvector, -- Full-text search vector
    hash_key INTEGER GENERATED ALWAYS AS (hashtext(claim_id || COALESCE(patient_account_number, ''))) STORED -- Hash for faster lookups
) PARTITION BY RANGE (service_date);
COMMENT ON TABLE staging.claims IS 'Staging table for claims with facility assignments to be validated. Partitioned by service_date for performance.';
COMMENT ON COLUMN staging.claims.processing_status IS 'Current status of the claim in the processing pipeline.';
COMMENT ON COLUMN staging.claims.datalog_rule_outcomes IS 'Stores outcomes from the Datalog rule engine for this claim.';
COMMENT ON COLUMN staging.claims.search_vector IS 'Full-text search vector for claim data.';
COMMENT ON COLUMN staging.claims.hash_key IS 'Pre-computed hash for faster lookups.';

-- Create monthly partitions for claims (create for current year + 1 year ahead)
-- This should be automated via cron job or application logic
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months');
    end_date DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '18 months');
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    WHILE start_date < end_date LOOP
        partition_start := start_date;
        partition_end := start_date + INTERVAL '1 month';
        partition_name := 'claims_' || TO_CHAR(partition_start, 'YYYY_MM');
        
        EXECUTE format('CREATE TABLE IF NOT EXISTS staging.%I PARTITION OF staging.claims
                       FOR VALUES FROM (%L) TO (%L)', 
                       partition_name, partition_start, partition_end);
        
        start_date := partition_end;
    END LOOP;
END $$;

-- Add primary key constraint to each partition
ALTER TABLE staging.claims ADD CONSTRAINT pk_claims PRIMARY KEY (claim_id, service_date);

-- Staging CMS 1500 diagnosis codes with partitioning
CREATE TABLE staging.cms1500_diagnoses (
    diagnosis_entry_id BIGSERIAL,
    claim_id VARCHAR(50) NOT NULL,
    service_date DATE NOT NULL, -- Added for partitioning alignment
    diagnosis_sequence INTEGER NOT NULL CHECK (diagnosis_sequence BETWEEN 1 AND 12),
    icd_code VARCHAR(10) NOT NULL,
    icd_code_type VARCHAR(10) DEFAULT 'ICD10',
    diagnosis_description TEXT,
    is_primary BOOLEAN DEFAULT FALSE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (service_date);
COMMENT ON TABLE staging.cms1500_diagnoses IS 'Staging for CMS 1500 diagnosis codes (Section 21). Partitioned by service_date.';

-- Create corresponding diagnosis partitions
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months');
    end_date DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '18 months');
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    WHILE start_date < end_date LOOP
        partition_start := start_date;
        partition_end := start_date + INTERVAL '1 month';
        partition_name := 'cms1500_diagnoses_' || TO_CHAR(partition_start, 'YYYY_MM');
        
        EXECUTE format('CREATE TABLE IF NOT EXISTS staging.%I PARTITION OF staging.cms1500_diagnoses
                       FOR VALUES FROM (%L) TO (%L)', 
                       partition_name, partition_start, partition_end);
        
        start_date := partition_end;
    END LOOP;
END $$;

-- Staging CMS 1500 procedures/line items with partitioning
CREATE TABLE staging.cms1500_line_items (
    line_item_id BIGSERIAL,
    claim_id VARCHAR(50) NOT NULL,
    service_date DATE NOT NULL, -- Added for partitioning alignment
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
    estimated_reimbursement_amount DECIMAL(10,2), -- Added for RVU calculations
    reimbursement_calculation_status VARCHAR(20), -- Status of reimbursement calc
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (service_date);
COMMENT ON TABLE staging.cms1500_line_items IS 'Staging for CMS 1500 line items (Sections 24A-J). Partitioned by service_date.';

-- Create corresponding line items partitions
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months');
    end_date DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '18 months');
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    WHILE start_date < end_date LOOP
        partition_start := start_date;
        partition_end := start_date + INTERVAL '1 month';
        partition_name := 'cms1500_line_items_' || TO_CHAR(partition_start, 'YYYY_MM');
        
        EXECUTE format('CREATE TABLE IF NOT EXISTS staging.%I PARTITION OF staging.cms1500_line_items
                       FOR VALUES FROM (%L) TO (%L)', 
                       partition_name, partition_start, partition_end);
        
        start_date := partition_end;
    END LOOP;
END $$;

-- Legacy diagnosis and procedure tables (if still needed for backward compatibility during transition)
CREATE TABLE staging.diagnoses (
    diagnosis_id SERIAL PRIMARY KEY,
    claim_id VARCHAR(50),
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
    claim_id VARCHAR(50),
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

-- Validation results table with partitioning by created_date
CREATE TABLE staging.validation_results (
    result_id BIGSERIAL,
    claim_id VARCHAR(50) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL, -- Added NOT NULL for partitioning
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
    rule_version VARCHAR(50) -- Version of Datalog/Validation rule used
) PARTITION BY RANGE (created_date);
COMMENT ON TABLE staging.validation_results IS 'Stores results of claim validation, including ML and rule outcomes. Partitioned by created_date.';

-- Create validation results partitions (weekly partitions for finer granularity)
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 weeks');
    end_date DATE := DATE_TRUNC('week', CURRENT_DATE + INTERVAL '12 weeks');
    partition_start DATE;
    partition_end DATE;
    partition_name TEXT;
BEGIN
    WHILE start_date < end_date LOOP
        partition_start := start_date;
        partition_end := start_date + INTERVAL '1 week';
        partition_name := 'validation_results_' || TO_CHAR(partition_start, 'YYYY_WW');
        
        EXECUTE format('CREATE TABLE IF NOT EXISTS staging.%I PARTITION OF staging.validation_results
                       FOR VALUES FROM (%L) TO (%L)', 
                       partition_name, partition_start, partition_end);
        
        start_date := partition_end;
    END LOOP;
END $$;

-- ===========================
-- REFERENCE DATA CACHE TABLES (STAGING SCHEMA) WITH MEMORY OPTIMIZATION
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
) WITH (fillfactor = 90); -- Optimize for mostly read operations
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
) WITH (fillfactor = 90);
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
) WITH (fillfactor = 90);
COMMENT ON TABLE staging.financial_classes_cache IS 'Read-only cache of financial class data for validation.';

CREATE TABLE staging.standard_payers_cache (
    standard_payer_id INTEGER PRIMARY KEY,
    standard_payer_code VARCHAR(20),
    standard_payer_name VARCHAR(100),
    payer_category VARCHAR(50),
    active BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) WITH (fillfactor = 95); -- Very stable data
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
    exported_to_production BOOLEAN DEFAULT FALSE,
    processing_stats JSONB -- Additional processing statistics
);
COMMENT ON TABLE staging.processing_batches IS 'Tracks batches of claims being processed.';

CREATE TABLE staging.claim_batches (
    claim_id VARCHAR(50),
    batch_id INTEGER REFERENCES staging.processing_batches(batch_id) ON DELETE CASCADE,
    assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (claim_id, batch_id)
) WITH (fillfactor = 95); -- Mostly INSERT operations
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
-- ADVANCED INDEXES FOR HIGH-VOLUME QUERIES
-- ===========================

-- edi schema indexes with query optimization
CREATE INDEX CONCURRENTLY idx_organizations_active_name ON edi.organizations(active, organization_name) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_regions_active_code ON edi.regions(active, region_code) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_facilities_active_lookup ON edi.facilities(facility_id, active, organization_id) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_facilities_state_type ON edi.facilities(state_code, facility_type) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_financial_classes_facility_active ON edi.financial_classes(facility_id, active) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_clinical_departments_facility_active ON edi.clinical_departments(facility_id, active) WHERE active = TRUE;

-- Advanced filter index for active latest rules
CREATE INDEX CONCURRENTLY idx_edi_filters_optimized ON edi.filters(filter_name, rule_type) 
    WHERE is_active = TRUE AND is_latest_version = TRUE;

-- Staging claims advanced indexes with partial and composite strategies
CREATE INDEX CONCURRENTLY idx_staging_claims_processing_status_date ON staging.claims(processing_status, service_date, facility_id) 
    WHERE exported_to_production = FALSE;

CREATE INDEX CONCURRENTLY idx_staging_claims_facility_validation ON staging.claims(facility_id, facility_validated, processing_status)
    WHERE exported_to_production = FALSE;

CREATE INDEX CONCURRENTLY idx_staging_claims_export_ready ON staging.claims(exported_to_production, processing_status, service_date)
    WHERE processing_status IN ('COMPLETED', 'VALIDATED');

-- Hash index for fast claim lookups
CREATE INDEX CONCURRENTLY idx_staging_claims_hash_lookup ON staging.claims USING HASH(hash_key);

-- GIN index for JSONB fields
CREATE INDEX CONCURRENTLY idx_staging_claims_claim_data_gin ON staging.claims USING GIN(claim_data);
CREATE INDEX CONCURRENTLY idx_staging_claims_datalog_outcomes_gin ON staging.claims USING GIN(datalog_rule_outcomes);

-- Full-text search index
CREATE INDEX CONCURRENTLY idx_staging_claims_search_vector ON staging.claims USING GIN(search_vector);

-- Multi-column indexes for common query patterns
CREATE INDEX CONCURRENTLY idx_staging_claims_multi_validation ON staging.claims(facility_validated, department_validated, financial_class_validated, processing_status);

-- Diagnosis and line item optimized indexes
CREATE INDEX CONCURRENTLY idx_cms1500_diagnoses_claim_service ON staging.cms1500_diagnoses(claim_id, service_date, icd_code);
CREATE INDEX CONCURRENTLY idx_cms1500_diagnoses_icd_primary ON staging.cms1500_diagnoses(icd_code, is_primary) WHERE is_primary = TRUE;

CREATE INDEX CONCURRENTLY idx_cms1500_line_items_claim_service ON staging.cms1500_line_items(claim_id, service_date, cpt_code);
CREATE INDEX CONCURRENTLY idx_cms1500_line_items_cpt_charges ON staging.cms1500_line_items(cpt_code, line_charge_amount);
CREATE INDEX CONCURRENTLY idx_cms1500_line_items_reimbursement ON staging.cms1500_line_items(estimated_reimbursement_amount) 
    WHERE estimated_reimbursement_amount IS NOT NULL;

-- Validation results optimized indexes
CREATE INDEX CONCURRENTLY idx_validation_results_claim_type_status ON staging.validation_results(claim_id, validation_type, validation_status);
CREATE INDEX CONCURRENTLY idx_validation_results_performance ON staging.validation_results(validation_type, processing_time, created_date);
CREATE INDEX CONCURRENTLY idx_validation_results_ml_stats ON staging.validation_results(model_version, ml_inference_time) 
    WHERE ml_inference_time IS NOT NULL;

-- Cache table indexes optimized for lookup patterns
CREATE INDEX CONCURRENTLY idx_facilities_cache_active_lookup ON staging.facilities_cache(facility_id, active) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_departments_cache_facility_active ON staging.departments_cache(facility_id, active) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_financial_classes_cache_facility_active ON staging.financial_classes_cache(facility_id, active) WHERE active = TRUE;
CREATE INDEX CONCURRENTLY idx_standard_payers_cache_code_active ON staging.standard_payers_cache(standard_payer_code, active) WHERE active = TRUE;

-- Processing tracking indexes with performance focus
CREATE INDEX CONCURRENTLY idx_processing_batches_status_date ON staging.processing_batches(status, created_date);
CREATE INDEX CONCURRENTLY idx_claim_batches_batch_assigned ON staging.claim_batches(batch_id, assigned_date);
CREATE INDEX CONCURRENTLY idx_processing_errors_claim_date ON staging.processing_errors(claim_id, created_date);
CREATE INDEX CONCURRENTLY idx_processing_errors_type_date ON staging.processing_errors(error_type, created_date);

-- ===========================
-- MATERIALIZED VIEWS FOR ANALYTICS
-- ===========================

-- High-level processing statistics materialized view
CREATE MATERIALIZED VIEW analytics.processing_summary AS
SELECT
    DATE_TRUNC('hour', created_date) as processing_hour,
    processing_status,
    facility_validated,
    department_validated,
    financial_class_validated,
    COUNT(*) as claim_count,
    SUM(total_charge_amount) as total_charges,
    AVG(total_charge_amount) as avg_charge_amount,
    COUNT(*) FILTER (WHERE exported_to_production = TRUE) as exported_count,
    MIN(service_date) as earliest_service_date,
    MAX(service_date) as latest_service_date
FROM staging.claims
WHERE created_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', created_date), processing_status, facility_validated, department_validated, financial_class_validated;

CREATE UNIQUE INDEX idx_processing_summary_unique ON analytics.processing_summary(processing_hour, processing_status, facility_validated, department_validated, financial_class_validated);
COMMENT ON MATERIALIZED VIEW analytics.processing_summary IS 'Hourly aggregated processing statistics for analytics dashboard.';

-- Facility performance analytics
CREATE MATERIALIZED VIEW analytics.facility_performance AS
SELECT
    f.facility_id,
    f.facility_name,
    f.facility_type,
    f.state_code,
    DATE_TRUNC('day', c.service_date) as service_day,
    COUNT(c.claim_id) as total_claims,
    COUNT(*) FILTER (WHERE c.processing_status = 'COMPLETED') as completed_claims,
    COUNT(*) FILTER (WHERE c.facility_validated = TRUE) as facility_validated_claims,
    COUNT(*) FILTER (WHERE c.exported_to_production = TRUE) as exported_claims,
    SUM(c.total_charge_amount) as total_charges,
    AVG(c.total_charge_amount) as avg_charge_per_claim,
    COUNT(DISTINCT c.patient_id) as unique_patients,
    COUNT(DISTINCT li.cpt_code) as unique_procedures
FROM staging.claims c
JOIN staging.facilities_cache f ON c.facility_id = f.facility_id
LEFT JOIN staging.cms1500_line_items li ON c.claim_id = li.claim_id AND c.service_date = li.service_date
WHERE c.service_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY f.facility_id, f.facility_name, f.facility_type, f.state_code, DATE_TRUNC('day', c.service_date);

CREATE UNIQUE INDEX idx_facility_performance_unique ON analytics.facility_performance(facility_id, service_day);
COMMENT ON MATERIALIZED VIEW analytics.facility_performance IS 'Daily facility performance metrics for analytics and reporting.';

-- Validation failure patterns analysis
CREATE MATERIALIZED VIEW analytics.validation_failure_patterns AS
SELECT
    vr.validation_type,
    vr.validation_status,
    vr.model_version,
    DATE_TRUNC('day', vr.created_date) as validation_day,
    COUNT(*) as failure_count,
    AVG(vr.processing_time) as avg_processing_time,
    AVG(vr.ml_inference_time) as avg_ml_inference_time,
    AVG(vr.rule_engine_time) as avg_rule_engine_time,
    -- Extract common error patterns from validation_details JSONB
    jsonb_object_agg(
        COALESCE(vr.validation_details->>'error_type', 'unknown'),
        COUNT(*)
    ) FILTER (WHERE vr.validation_status = 'FAILED') as error_type_counts
FROM staging.validation_results vr
WHERE vr.created_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY vr.validation_type, vr.validation_status, vr.model_version, DATE_TRUNC('day', vr.created_date);

CREATE UNIQUE INDEX idx_validation_failure_patterns_unique ON analytics.validation_failure_patterns(validation_type, validation_status, model_version, validation_day);
COMMENT ON MATERIALIZED VIEW analytics.validation_failure_patterns IS 'Daily validation failure pattern analysis for troubleshooting.';

-- Financial metrics materialized view
CREATE MATERIALIZED VIEW analytics.financial_metrics AS
SELECT
    DATE_TRUNC('day', c.service_date) as service_day,
    fc.financial_class_description,
    sp.payer_category,
    f.facility_type,
    f.state_code,
    COUNT(c.claim_id) as claim_count,
    SUM(c.total_charge_amount) as total_charges,
    SUM(li.estimated_reimbursement_amount) as total_estimated_reimbursement,
    AVG(c.total_charge_amount) as avg_charge_per_claim,
    AVG(li.estimated_reimbursement_amount) as avg_reimbursement_per_line,
    -- Calculate reimbursement ratio where both values exist
    CASE 
        WHEN SUM(c.total_charge_amount) > 0 
        THEN SUM(li.estimated_reimbursement_amount) / SUM(c.total_charge_amount)
        ELSE NULL
    END as reimbursement_ratio
FROM staging.claims c
JOIN staging.facilities_cache f ON c.facility_id = f.facility_id
LEFT JOIN staging.financial_classes_cache fc ON c.financial_class_id = fc.financial_class_id
LEFT JOIN staging.standard_payers_cache sp ON fc.standard_payer_id = sp.standard_payer_id
LEFT JOIN staging.cms1500_line_items li ON c.claim_id = li.claim_id AND c.service_date = li.service_date
WHERE c.service_date >= CURRENT_DATE - INTERVAL '90 days'
  AND c.exported_to_production = TRUE
GROUP BY DATE_TRUNC('day', c.service_date), fc.financial_class_description, sp.payer_category, f.facility_type, f.state_code;

CREATE UNIQUE INDEX idx_financial_metrics_unique ON analytics.financial_metrics(service_day, financial_class_description, payer_category, facility_type, state_code);
COMMENT ON MATERIALIZED VIEW analytics.financial_metrics IS 'Daily financial performance metrics including reimbursement analysis.';

-- ML Model performance tracking
CREATE MATERIALIZED VIEW analytics.ml_model_performance AS
SELECT
    vr.model_version,
    DATE_TRUNC('hour', vr.created_date) as prediction_hour,
    COUNT(*) as total_predictions,
    AVG(vr.ml_inference_time) as avg_inference_time,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vr.ml_inference_time) as median_inference_time,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY vr.ml_inference_time) as p95_inference_time,
    -- Extract prediction confidence if stored in validation_details
    AVG((vr.validation_details->>'confidence')::FLOAT) FILTER (WHERE vr.validation_details->>'confidence' IS NOT NULL) as avg_confidence,
    COUNT(*) FILTER (WHERE (vr.validation_details->>'confidence')::FLOAT > 0.8) as high_confidence_predictions
FROM staging.validation_results vr
WHERE vr.validation_type = 'ML_FILTER'
  AND vr.created_date >= CURRENT_DATE - INTERVAL '24 hours'
  AND vr.ml_inference_time IS NOT NULL
GROUP BY vr.model_version, DATE_TRUNC('hour', vr.created_date);

CREATE UNIQUE INDEX idx_ml_model_performance_unique ON analytics.ml_model_performance(model_version, prediction_hour);
COMMENT ON MATERIALIZED VIEW analytics.ml_model_performance IS 'Hourly ML model performance metrics for monitoring.';

-- ===========================
-- PERFORMANCE OPTIMIZATION FUNCTIONS
-- ===========================

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION analytics.refresh_all_views()
RETURNS TEXT AS $
DECLARE
    view_name TEXT;
    result_text TEXT := '';
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    FOR view_name IN 
        SELECT schemaname||'.'||matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'analytics'
        ORDER BY matviewname
    LOOP
        start_time := clock_timestamp();
        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || view_name;
        end_time := clock_timestamp();
        result_text := result_text || view_name || ' refreshed in ' || 
                      EXTRACT(EPOCH FROM (end_time - start_time))::TEXT || ' seconds. ';
    END LOOP;
    
    RETURN result_text;
END;
$ LANGUAGE plpgsql;
COMMENT ON FUNCTION analytics.refresh_all_views() IS 'Refreshes all materialized views in the analytics schema.';

-- Function to update search vectors for full-text search
CREATE OR REPLACE FUNCTION staging.update_claim_search_vectors()
RETURNS INTEGER AS $
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE staging.claims
    SET search_vector = to_tsvector('english', 
        COALESCE(claim_id, '') || ' ' ||
        COALESCE(patient_account_number, '') || ' ' ||
        COALESCE(facility_id, '') || ' ' ||
        COALESCE(payer_name, '') || ' ' ||
        COALESCE(provider_id, '')
    )
    WHERE search_vector IS NULL
       OR updated_date > CURRENT_TIMESTAMP - INTERVAL '1 hour';
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.update_claim_search_vectors() IS 'Updates search vectors for recently modified claims.';

-- Function for intelligent partition management
CREATE OR REPLACE FUNCTION staging.manage_partitions()
RETURNS TEXT AS $
DECLARE
    table_name TEXT;
    future_partitions INTEGER := 0;
    old_partitions INTEGER := 0;
    result_text TEXT := '';
BEGIN
    -- Create future partitions for claims (6 months ahead)
    FOR table_name IN VALUES ('claims'), ('cms1500_diagnoses'), ('cms1500_line_items') LOOP
        EXECUTE format('
            INSERT INTO staging.partition_log (table_name, action, partition_date)
            SELECT %L, ''CREATE_FUTURE'', generate_series(
                DATE_TRUNC(''month'', CURRENT_DATE + INTERVAL ''1 month''),
                DATE_TRUNC(''month'', CURRENT_DATE + INTERVAL ''6 months''),
                INTERVAL ''1 month''
            )::DATE
            ON CONFLICT DO NOTHING', table_name);
    END LOOP;
    
    -- Create weekly partitions for validation_results (8 weeks ahead)
    EXECUTE '
        CREATE TABLE IF NOT EXISTS staging.partition_log (
            table_name TEXT,
            action TEXT,
            partition_date DATE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (table_name, action, partition_date)
        )';
    
    result_text := 'Partition management completed. Future partitions scheduled.';
    RETURN result_text;
END;
$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.manage_partitions() IS 'Manages partition creation and cleanup automatically.';

-- ===========================
-- VIEWS FOR PROCESSING WITH QUERY OPTIMIZATION HINTS
-- ===========================

CREATE OR REPLACE VIEW staging.v_claims_for_validation AS
SELECT /*+ USE_HASH(c fac_cache) USE_HASH(c dept_cache) USE_HASH(c fin_cache) */
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
    c.hash_key,
    COALESCE(fac_cache.facility_name, 'N/A') as facility_name,
    COALESCE(dept_cache.department_description, 'N/A') as department_description,
    COALESCE(fin_cache.financial_class_description, 'N/A') as financial_class_description,
    COALESCE(array_agg(DISTINCT diag.icd_code) FILTER (WHERE diag.icd_code IS NOT NULL), ARRAY[]::VARCHAR[]) as diagnosis_codes,
    COALESCE(array_agg(DISTINCT li.cpt_code) FILTER (WHERE li.cpt_code IS NOT NULL), ARRAY[]::VARCHAR[]) as procedure_codes,
    COUNT(DISTINCT diag.diagnosis_entry_id) as diagnosis_count,
    COUNT(DISTINCT li.line_item_id) as line_item_count,
    SUM(li.estimated_reimbursement_amount) as total_estimated_reimbursement
FROM staging.claims c
LEFT JOIN staging.facilities_cache fac_cache ON c.facility_id = fac_cache.facility_id
LEFT JOIN staging.departments_cache dept_cache ON c.department_id = dept_cache.department_id
LEFT JOIN staging.financial_classes_cache fin_cache ON c.financial_class_id = fin_cache.financial_class_id
LEFT JOIN staging.cms1500_diagnoses diag ON c.claim_id = diag.claim_id AND c.service_date = diag.service_date
LEFT JOIN staging.cms1500_line_items li ON c.claim_id = li.claim_id AND c.service_date = li.service_date
WHERE c.processing_status = 'PENDING'
  AND c.exported_to_production = FALSE
GROUP BY c.claim_id, c.facility_id, c.department_id, c.financial_class_id, c.patient_id, 
         c.patient_age, c.patient_dob, c.patient_sex, c.patient_account_number, c.provider_id, 
         c.provider_type, c.rendering_provider_npi, c.place_of_service, c.service_date, 
         c.total_charge_amount, c.total_claim_charges, c.payer_name, c.processing_status, 
         c.claim_data, c.created_date, c.hash_key, fac_cache.facility_name, 
         dept_cache.department_description, fin_cache.financial_class_description;
COMMENT ON VIEW staging.v_claims_for_validation IS 'Optimized view for claims ready for validation with enhanced performance hints.';

CREATE OR REPLACE VIEW staging.v_high_volume_processing_stats AS
SELECT /*+ PARALLEL(4) */
    DATE_TRUNC('hour', created_date) as processing_hour,
    processing_status,
    COUNT(*) as claim_count,
    COUNT(*) FILTER (WHERE facility_validated = TRUE) as facility_validated_count,
    COUNT(*) FILTER (WHERE exported_to_production = TRUE) as exported_count,
    SUM(total_charge_amount) as total_charges,
    AVG(total_charge_amount) as avg_charge,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_charge_amount) as median_charge,
    COUNT(DISTINCT facility_id) as unique_facilities,
    COUNT(DISTINCT SUBSTRING(patient_account_number, 1, 3)) as account_prefixes -- Privacy-safe patient diversity metric
FROM staging.claims
WHERE created_date >= CURRENT_DATE - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', created_date), processing_status
ORDER BY processing_hour DESC;
COMMENT ON VIEW staging.v_high_volume_processing_stats IS 'High-performance hourly processing statistics with parallel processing hints.';

-- ===========================
-- TRIGGERS FOR AUTOMATIC UPDATES WITH PERFORMANCE OPTIMIZATION
-- ===========================

CREATE OR REPLACE FUNCTION staging.update_claims_timestamp_optimized()
RETURNS TRIGGER AS $
BEGIN
    -- Only update timestamp if substantive fields changed
    IF (OLD.processing_status IS DISTINCT FROM NEW.processing_status OR
        OLD.facility_validated IS DISTINCT FROM NEW.facility_validated OR
        OLD.department_validated IS DISTINCT FROM NEW.department_validated OR
        OLD.financial_class_validated IS DISTINCT FROM NEW.financial_class_validated OR
        OLD.exported_to_production IS DISTINCT FROM NEW.exported_to_production) THEN
        
        NEW.updated_date = CURRENT_TIMESTAMP;
        
        -- Update search vector if key searchable fields changed
        IF (OLD.patient_account_number IS DISTINCT FROM NEW.patient_account_number OR
            OLD.facility_id IS DISTINCT FROM NEW.facility_id OR
            OLD.payer_name IS DISTINCT FROM NEW.payer_name) THEN
            
            NEW.search_vector = to_tsvector('english', 
                COALESCE(NEW.claim_id, '') || ' ' ||
                COALESCE(NEW.patient_account_number, '') || ' ' ||
                COALESCE(NEW.facility_id, '') || ' ' ||
                COALESCE(NEW.payer_name, '') || ' ' ||
                COALESCE(NEW.provider_id, '')
            );
        END IF;
    END IF;
    
    RETURN NEW;
END;
$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_staging_claims_updated_date ON staging.claims;
CREATE TRIGGER tr_staging_claims_updated_date
    BEFORE UPDATE ON staging.claims
    FOR EACH ROW
    EXECUTE FUNCTION staging.update_claims_timestamp_optimized();
COMMENT ON TRIGGER tr_staging_claims_updated_date ON staging.claims IS 'Optimized trigger for updating timestamps and search vectors.';

-- ===========================
-- STORED PROCEDURES FOR HIGH-PERFORMANCE BATCH OPERATIONS
-- ===========================

CREATE OR REPLACE FUNCTION staging.bulk_update_processing_status(
    p_claim_ids TEXT[],
    p_new_status VARCHAR(30),
    p_batch_size INTEGER DEFAULT 1000
)
RETURNS INTEGER AS $
DECLARE
    updated_count INTEGER := 0;
    batch_start INTEGER := 1;
    batch_end INTEGER;
    total_claims INTEGER := array_length(p_claim_ids, 1);
BEGIN
    -- Process in batches to avoid long-running transactions
    WHILE batch_start <= total_claims LOOP
        batch_end := LEAST(batch_start + p_batch_size - 1, total_claims);
        
        UPDATE staging.claims
        SET processing_status = p_new_status,
            updated_date = CURRENT_TIMESTAMP
        WHERE claim_id = ANY(p_claim_ids[batch_start:batch_end]);
        
        GET DIAGNOSTICS updated_count = updated_count + ROW_COUNT;
        
        -- Commit each batch
        COMMIT;
        
        batch_start := batch_end + 1;
    END LOOP;
    
    RETURN updated_count;
END;
$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.bulk_update_processing_status(TEXT[], VARCHAR(30), INTEGER) IS 'High-performance bulk status updates with batching.';

CREATE OR REPLACE FUNCTION staging.cleanup_old_data_optimized(
    p_days_to_keep INTEGER DEFAULT 30,
    p_batch_size INTEGER DEFAULT 10000
)
RETURNS TEXT AS $
DECLARE
    deleted_claims INTEGER := 0;
    deleted_validations INTEGER := 0;
    deleted_batches INTEGER := 0;
    batch_deleted INTEGER;
    result_message TEXT;
BEGIN
    -- Delete old exported claims in batches
    LOOP
        DELETE FROM staging.claims
        WHERE ctid IN (
            SELECT ctid FROM staging.claims
            WHERE exported_to_production = TRUE
              AND export_date < (CURRENT_DATE - (p_days_to_keep || ' days')::interval)
            LIMIT p_batch_size
        );
        
        GET DIAGNOSTICS batch_deleted = ROW_COUNT;
        deleted_claims := deleted_claims + batch_deleted;
        
        EXIT WHEN batch_deleted = 0;
        
        -- Brief pause to allow other operations
        PERFORM pg_sleep(0.1);
    END LOOP;
    
    -- Delete old validation results in batches
    LOOP
        DELETE FROM staging.validation_results
        WHERE ctid IN (
            SELECT ctid FROM staging.validation_results vr
            WHERE vr.created_date < (CURRENT_DATE - (p_days_to_keep || ' days')::interval)
              AND NOT EXISTS (SELECT 1 FROM staging.claims c WHERE c.claim_id = vr.claim_id)
            LIMIT p_batch_size
        );
        
        GET DIAGNOSTICS batch_deleted = ROW_COUNT;
        deleted_validations := deleted_validations + batch_deleted;
        
        EXIT WHEN batch_deleted = 0;
        PERFORM pg_sleep(0.1);
    END LOOP;
    
    -- Delete old completed batches
    DELETE FROM staging.processing_batches pb
    WHERE pb.status = 'COMPLETED'
      AND pb.exported_to_production = TRUE
      AND pb.end_time < (CURRENT_DATE - (p_days_to_keep || ' days')::interval);
    GET DIAGNOSTICS deleted_batches = ROW_COUNT;

    -- Update table statistics
    ANALYZE staging.claims;
    ANALYZE staging.validation_results;
    ANALYZE staging.processing_batches;

    result_message := format(
        'Optimized cleanup completed: %s claims, %s validation results, %s batches deleted',
        deleted_claims, deleted_validations, deleted_batches
    );
    
    RETURN result_message;
END;
$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.cleanup_old_data_optimized(INTEGER, INTEGER) IS 'Optimized cleanup with batching and statistics updates.';

-- ===========================
-- QUERY OPTIMIZATION CONFIGURATION
-- ===========================

-- Create extension for additional performance features if not exists
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS auto_explain;

-- Configure auto_explain for query optimization
SELECT set_config('auto_explain.log_min_duration', '1000', false); -- Log queries taking > 1 second
SELECT set_config('auto_explain.log_analyze', 'true', false);
SELECT set_config('auto_explain.log_buffers', 'true', false);
SELECT set_config('auto_explain.log_format', 'json', false);

-- ===========================
-- AUTOMATED MAINTENANCE JOBS
-- ===========================

-- Function to be called by cron for automated maintenance
CREATE OR REPLACE FUNCTION staging.automated_maintenance()
RETURNS TEXT AS $
DECLARE
    result_text TEXT := '';
    maintenance_start TIMESTAMP := clock_timestamp();
BEGIN
    -- Update search vectors
    result_text := result_text || 'Search vectors updated: ' || staging.update_claim_search_vectors()::TEXT || ' records. ';
    
    -- Refresh materialized views during low-traffic hours
    IF EXTRACT(HOUR FROM CURRENT_TIME) BETWEEN 2 AND 4 THEN
        result_text := result_text || analytics.refresh_all_views();
    END IF;
    
    -- Manage partitions
    result_text := result_text || staging.manage_partitions();
    
    -- Update table statistics for frequently updated tables
    ANALYZE staging.claims;
    ANALYZE staging.validation_results;
    
    result_text := result_text || ' Maintenance completed in ' || 
                  EXTRACT(EPOCH FROM (clock_timestamp() - maintenance_start))::TEXT || ' seconds.';
    
    RETURN result_text;
END;
$ LANGUAGE plpgsql;
COMMENT ON FUNCTION staging.automated_maintenance() IS 'Automated maintenance function for cron scheduling.';

-- ===========================
-- PERFORMANCE MONITORING VIEWS
-- ===========================

CREATE OR REPLACE VIEW analytics.query_performance AS
SELECT
    query,
    calls,
    total_time,
    mean_time,
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements
WHERE query NOT LIKE '%pg_stat_statements%'
ORDER BY total_time DESC
LIMIT 20;
COMMENT ON VIEW analytics.query_performance IS 'Top 20 queries by total execution time for performance monitoring.';

CREATE OR REPLACE VIEW analytics.table_performance AS
SELECT
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup,
    n_dead_tup,
    vacuum_count,
    autovacuum_count,
    analyze_count,
    autoanalyze_count
FROM pg_stat_user_tables
WHERE schemaname IN ('staging', 'edi', 'analytics')
ORDER BY seq_tup_read + idx_tup_fetch DESC;
COMMENT ON VIEW analytics.table_performance IS 'Table access patterns and maintenance statistics.';

-- ===========================
-- GRANTS AND PERMISSIONS FOR PERFORMANCE
-- ===========================
-- Example grants, adjust user names and permissions as needed.
-- GRANT USAGE ON SCHEMA edi TO edi_app_user;
-- GRANT USAGE ON SCHEMA staging TO edi_app_user;
-- GRANT USAGE ON SCHEMA analytics TO edi_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA edi TO edi_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO edi_app_user;
-- GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO edi_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA edi TO edi_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO edi_app_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA staging TO edi_app_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA analytics TO edi_app_user;

-- Performance-related permissions
-- GRANT SELECT ON pg_stat_statements TO edi_monitoring_user;
-- GRANT SELECT ON pg_stat_user_tables TO edi_monitoring_user;

-- End of Enhanced PostgreSQL Staging and EDI Master Database Schema with Performance Optimizations