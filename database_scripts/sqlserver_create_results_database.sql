-- SQL Server Production Database Schema with Comprehensive Facility Management
-- Enhanced with Performance Optimizations: Columnstore Indexes, Partitioning, Compression, and Monitoring
-- ================================================================================

-- Check if the database already exists and create it if not
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'edi_production')
BEGIN
    CREATE DATABASE edi_production;
END
GO

-- Switch to the newly created database context
USE edi_production;
GO

-- ===========================
-- PARTITION FUNCTIONS AND SCHEMES
-- ===========================

-- Partition function for Claims by ServiceDate (monthly partitions)
CREATE PARTITION FUNCTION pf_ClaimsByServiceDate (DATE)
AS RANGE RIGHT FOR VALUES 
(
    '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01',
    '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
    '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01',
    '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
    '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01', '2025-05-01', '2025-06-01',
    '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01',
    '2026-01-01'
);
GO

-- Partition scheme for Claims
CREATE PARTITION SCHEME ps_ClaimsByServiceDate
AS PARTITION pf_ClaimsByServiceDate
ALL TO ([PRIMARY]);
GO

-- Partition function for Line Items by ServiceDateFrom (monthly partitions)
CREATE PARTITION FUNCTION pf_LineItemsByServiceDate (DATE)
AS RANGE RIGHT FOR VALUES 
(
    '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01',
    '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
    '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01',
    '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
    '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01', '2025-05-01', '2025-06-01',
    '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01',
    '2026-01-01'
);
GO

-- Partition scheme for Line Items
CREATE PARTITION SCHEME ps_LineItemsByServiceDate
AS PARTITION pf_LineItemsByServiceDate
ALL TO ([PRIMARY]);
GO

-- ===========================
-- CORE TABLES WITH COMPRESSION
-- ===========================

-- Organizations table (supports one-to-many with regions)
CREATE TABLE dbo.Organizations (
    OrganizationId INT IDENTITY(1,1) PRIMARY KEY,
    OrganizationName NVARCHAR(200) NOT NULL,
    OrganizationCode NVARCHAR(20) UNIQUE,
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    City NVARCHAR(100),
    StateCode CHAR(2),
    ZipCode NVARCHAR(10),
    CountryCode CHAR(3) DEFAULT 'USA',
    Phone NVARCHAR(20),
    Email NVARCHAR(255),
    Website NVARCHAR(255),
    TaxId NVARCHAR(20),
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

-- Regions table (can belong to multiple organizations - many-to-many)
CREATE TABLE dbo.Regions (
    RegionId INT IDENTITY(1,1) PRIMARY KEY,
    RegionName NVARCHAR(200) NOT NULL,
    RegionCode NVARCHAR(20) NOT NULL UNIQUE,
    Description NVARCHAR(MAX),
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

-- Organization-Region mapping table (many-to-many relationship)
CREATE TABLE dbo.OrganizationRegions (
    OrganizationId INT REFERENCES dbo.Organizations(OrganizationId) ON DELETE CASCADE,
    RegionId INT REFERENCES dbo.Regions(RegionId) ON DELETE CASCADE,
    IsPrimary BIT DEFAULT 0,
    EffectiveDate DATE DEFAULT CAST(GETDATE() AS DATE),
    EndDate DATE,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    PRIMARY KEY (OrganizationId, RegionId)
) WITH (DATA_COMPRESSION = ROW);
GO

-- Facilities table (enhanced with all facility information)
CREATE TABLE dbo.Facilities (
    FacilityId NVARCHAR(50) PRIMARY KEY, -- User-defined facility ID
    FacilityName NVARCHAR(200) NOT NULL,
    FacilityCode NVARCHAR(20) UNIQUE, -- Internal facility code
    OrganizationId INT REFERENCES dbo.Organizations(OrganizationId),
    RegionId INT REFERENCES dbo.Regions(RegionId),

    -- Location information
    City NVARCHAR(100),
    StateCode CHAR(2),
    ZipCode NVARCHAR(10),
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),

    -- Facility characteristics
    BedSize INT CHECK (BedSize >= 0),
    FiscalReportingMonth INT CHECK (FiscalReportingMonth BETWEEN 1 AND 12),
    EmrSystem NVARCHAR(100), -- Electronic Medical Record system
    FacilityType NVARCHAR(20) CHECK (FacilityType IN ('Teaching', 'Non-Teaching')),
    CriticalAccess CHAR(1) CHECK (CriticalAccess IN ('Y', 'N')),

    -- Additional facility details
    Phone NVARCHAR(20),
    Fax NVARCHAR(20),
    Email NVARCHAR(255),
    NpiNumber NVARCHAR(10), -- National Provider Identifier
    TaxId NVARCHAR(20),
    LicenseNumber NVARCHAR(50),

    -- Status and audit
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

-- Standard Payers lookup table
CREATE TABLE dbo.StandardPayers (
    StandardPayerId INT IDENTITY(1,1) PRIMARY KEY,
    StandardPayerCode NVARCHAR(20) UNIQUE NOT NULL,
    StandardPayerName NVARCHAR(100) NOT NULL,
    PayerCategory NVARCHAR(50), -- Primary categorization
    Description NVARCHAR(MAX),
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

-- HCC (Hierarchical Condition Categories) types
CREATE TABLE dbo.HccTypes (
    HccTypeId INT IDENTITY(1,1) PRIMARY KEY,
    HccCode NVARCHAR(10) UNIQUE NOT NULL,
    HccName NVARCHAR(100) NOT NULL,
    HccAgency NVARCHAR(10) CHECK (HccAgency IN ('CMS', 'HHS')),
    Description NVARCHAR(MAX),
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

-- Financial Classes table (enhanced payer information)
CREATE TABLE dbo.FinancialClasses (
    FinancialClassId NVARCHAR(20) PRIMARY KEY, -- User-defined ID
    FinancialClassDescription NVARCHAR(200) NOT NULL,
    StandardPayerId INT REFERENCES dbo.StandardPayers(StandardPayerId),
    HccTypeId INT REFERENCES dbo.HccTypes(HccTypeId),
    FacilityId NVARCHAR(50) REFERENCES dbo.Facilities(FacilityId),

    -- Payer-specific details
    PayerPriority INT DEFAULT 1, -- Primary, secondary, tertiary
    RequiresAuthorization BIT DEFAULT 0,
    AuthorizationRequiredServices NVARCHAR(MAX), -- JSON array of service codes
    CopayAmount DECIMAL(8,2),
    DeductibleAmount DECIMAL(10,2),

    -- Processing rules
    ClaimSubmissionFormat NVARCHAR(20), -- CMS1500, UB04, Electronic
    DaysToFileClaim INT DEFAULT 365,
    AcceptsElectronicClaims BIT DEFAULT 1,

    -- Status and audit
    Active BIT DEFAULT 1,
    EffectiveDate DATE DEFAULT CAST(GETDATE() AS DATE),
    EndDate DATE,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

-- Clinical Departments table
CREATE TABLE dbo.ClinicalDepartments (
    DepartmentId INT IDENTITY(1,1) PRIMARY KEY,
    ClinicalDepartmentCode NVARCHAR(20) NOT NULL,
    DepartmentDescription NVARCHAR(200) NOT NULL,
    FacilityId NVARCHAR(50) REFERENCES dbo.Facilities(FacilityId),

    -- Department characteristics
    DepartmentType NVARCHAR(50), -- Inpatient, Outpatient, Emergency, etc.
    RevenueCode NVARCHAR(10), -- UB-04 revenue code
    CostCenter NVARCHAR(20),
    SpecialtyCode NVARCHAR(10),

    -- Clinical details
    IsSurgical BIT DEFAULT 0,
    IsEmergency BIT DEFAULT 0,
    IsCriticalCare BIT DEFAULT 0,
    RequiresPriorAuth BIT DEFAULT 0,

    -- Billing and operational
    DefaultPlaceOfService NVARCHAR(2), -- CMS place of service code
    BillingProviderNpi NVARCHAR(10),
    DepartmentManager NVARCHAR(100),
    PhoneExtension NVARCHAR(10),

    -- Status and audit
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE(),

    UNIQUE(ClinicalDepartmentCode, FacilityId)
) WITH (DATA_COMPRESSION = ROW);
GO

-- Enhanced Claims table (production claims with facility relationships) - PARTITIONED
CREATE TABLE dbo.Claims (
    ClaimId NVARCHAR(50) PRIMARY KEY,
    FacilityId NVARCHAR(50) REFERENCES dbo.Facilities(FacilityId), -- Links claim to facility
    DepartmentId INT REFERENCES dbo.ClinicalDepartments(DepartmentId), -- Links to department
    FinancialClassId NVARCHAR(20) REFERENCES dbo.FinancialClasses(FinancialClassId), -- Links to financial class

    -- Patient information
    PatientId NVARCHAR(50),
    PatientAge INT,
    PatientDob DATE,
    PatientSex CHAR(1),
    PatientAccountNumber NVARCHAR(50),

    -- Provider information
    ProviderId NVARCHAR(50),
    ProviderType NVARCHAR(100),
    RenderingProviderNpi NVARCHAR(10),

    -- Service information
    PlaceOfService NVARCHAR(10),
    ServiceDate DATE NOT NULL, -- Required for partitioning

    -- Financial information
    TotalChargeAmount DECIMAL(12,2),
    TotalClaimCharges DECIMAL(10,2),

    -- Payer information (CMS 1500 Section 1)
    PayerName NVARCHAR(100),
    PrimaryInsuranceId NVARCHAR(50),
    SecondaryInsuranceId NVARCHAR(50),

    -- Processing information
    ProcessingStatus NVARCHAR(20) DEFAULT 'PENDING',
    ProcessedDate DATETIME2,
    ClaimData NVARCHAR(MAX), -- JSON storage for additional claim details

    -- Validation results from PostgreSQL processing
    ValidationStatus NVARCHAR(20),
    ValidationResults NVARCHAR(MAX), -- JSON storage for validation results
    ValidationDate DATETIME2,

    -- Audit fields
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE(),
    CreatedBy NVARCHAR(100),
    UpdatedBy NVARCHAR(100)
) ON ps_ClaimsByServiceDate(ServiceDate) WITH (DATA_COMPRESSION = PAGE);
GO

-- CMS 1500 Diagnosis Codes Table (Section 21)
CREATE TABLE dbo.Cms1500Diagnoses (
    DiagnosisEntryId BIGINT IDENTITY(1,1) PRIMARY KEY,
    ClaimId NVARCHAR(50) NOT NULL REFERENCES dbo.Claims(ClaimId) ON DELETE CASCADE,
    DiagnosisSequence INT NOT NULL CHECK (DiagnosisSequence BETWEEN 1 AND 12),

    -- Section 21: ICD Diagnosis Codes
    IcdCode NVARCHAR(10) NOT NULL,
    IcdCodeType NVARCHAR(10) DEFAULT 'ICD10',
    DiagnosisDescription NVARCHAR(MAX),

    -- Pointer reference for line items
    IsPrimary BIT DEFAULT 0,

    CreatedDate DATETIME2 DEFAULT GETDATE(),

    -- Ensure unique sequence numbers per claim
    UNIQUE(ClaimId, DiagnosisSequence)
) WITH (DATA_COMPRESSION = PAGE);
GO

-- CMS 1500 Line Items Table (Sections 24A-J) - PARTITIONED
CREATE TABLE dbo.Cms1500LineItems (
    LineItemId BIGINT IDENTITY(1,1) PRIMARY KEY,
    ClaimId NVARCHAR(50) NOT NULL REFERENCES dbo.Claims(ClaimId) ON DELETE CASCADE,
    LineNumber INT NOT NULL CHECK (LineNumber BETWEEN 1 AND 99),

    -- Section 24A: Date of Service (From/To)
    ServiceDateFrom DATE NOT NULL, -- Required for partitioning
    ServiceDateTo DATE,

    -- Section 24B: Place of Service
    PlaceOfServiceCode NVARCHAR(2),

    -- Section 24C: EMG (Emergency indicator)
    EmergencyIndicator CHAR(1),

    -- Section 24D: Procedures, Services, or Supplies (CPT/HCPCS codes)
    CptCode NVARCHAR(10) NOT NULL,
    Modifier1 NVARCHAR(2),
    Modifier2 NVARCHAR(2),
    Modifier3 NVARCHAR(2),
    Modifier4 NVARCHAR(2),

    -- Section 24E: Diagnosis Pointer
    DiagnosisPointers NVARCHAR(10),

    -- Section 24F: Charges
    LineChargeAmount DECIMAL(10,2) NOT NULL,

    -- Section 24G: Days or Units
    Units INT DEFAULT 1,

    -- Section 24H: EPSDT Family Plan
    EpsdtFamilyPlan CHAR(1),

    -- Section 24I: ID Qualifier for Rendering Provider
    RenderingProviderIdQualifier NVARCHAR(2),

    -- Section 24J: Rendering Provider ID
    RenderingProviderId NVARCHAR(20),

    -- Additional metadata
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE(),

    -- Ensure unique line numbers per claim
    UNIQUE(ClaimId, LineNumber)
) ON ps_LineItemsByServiceDate(ServiceDateFrom) WITH (DATA_COMPRESSION = PAGE);
GO

-- Failed Claims Details table for UI (with compression)
CREATE TABLE dbo.FailedClaimDetails (
    FailedClaimDetailId BIGINT IDENTITY(1,1) PRIMARY KEY,
    OriginalClaimId NVARCHAR(50) NOT NULL,
    StagingClaimId NVARCHAR(50),
    FacilityId NVARCHAR(50),
    PatientAccountNumber NVARCHAR(50),
    ServiceDate DATE,
    FailureTimestamp DATETIME2 NOT NULL DEFAULT GETDATE(),
    ProcessingStage NVARCHAR(100) NOT NULL,
    ErrorCodes NVARCHAR(MAX),
    ErrorMessages NVARCHAR(MAX) NOT NULL,
    ClaimDataSnapshot NVARCHAR(MAX),
    Status NVARCHAR(50) NOT NULL DEFAULT 'New',
    ResolutionNotes NVARCHAR(MAX),
    ResolvedBy NVARCHAR(100),
    ResolvedTimestamp DATETIME2,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = PAGE);
GO

-- ===========================
-- COLUMNSTORE INDEXES FOR ANALYTICS
-- ===========================

-- Columnstore index for Claims analytics
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_Claims_Analytics_Columnstore
ON dbo.Claims (
    FacilityId, DepartmentId, FinancialClassId, ServiceDate, 
    TotalChargeAmount, TotalClaimCharges, ProcessingStatus, 
    ValidationStatus, PatientAge, PatientSex, CreatedDate
);
GO

-- Columnstore index for Line Items analytics
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_LineItems_Analytics_Columnstore
ON dbo.Cms1500LineItems (
    ClaimId, ServiceDateFrom, ServiceDateTo, CptCode, 
    LineChargeAmount, Units, PlaceOfServiceCode, CreatedDate
);
GO

-- Columnstore index for Diagnoses analytics
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_Diagnoses_Analytics_Columnstore
ON dbo.Cms1500Diagnoses (
    ClaimId, IcdCode, IcdCodeType, DiagnosisSequence, 
    IsPrimary, CreatedDate
);
GO

-- Columnstore index for Failed Claims analytics
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_FailedClaims_Analytics_Columnstore
ON dbo.FailedClaimDetails (
    FacilityId, ProcessingStage, Status, FailureTimestamp, 
    ServiceDate, CreatedDate
);
GO

-- ===========================
-- REGULAR INDEXES FOR PERFORMANCE (with compression)
-- ===========================

-- Organization indexes
CREATE INDEX IX_Organizations_Active ON dbo.Organizations(Active) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Organizations_Code ON dbo.Organizations(OrganizationCode) WITH (DATA_COMPRESSION = ROW);
GO

-- Region indexes
CREATE INDEX IX_Regions_Active ON dbo.Regions(Active) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Regions_Code ON dbo.Regions(RegionCode) WITH (DATA_COMPRESSION = ROW);
GO

-- Organization-Region mapping indexes
CREATE INDEX IX_OrganizationRegions_Org ON dbo.OrganizationRegions(OrganizationId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_OrganizationRegions_Region ON dbo.OrganizationRegions(RegionId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_OrganizationRegions_Primary ON dbo.OrganizationRegions(IsPrimary) WHERE IsPrimary = 1 WITH (DATA_COMPRESSION = ROW);
GO

-- Facility indexes
CREATE INDEX IX_Facilities_Org ON dbo.Facilities(OrganizationId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Facilities_Region ON dbo.Facilities(RegionId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Facilities_Active ON dbo.Facilities(Active) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Facilities_Type ON dbo.Facilities(FacilityType) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Facilities_State ON dbo.Facilities(StateCode) WITH (DATA_COMPRESSION = ROW);
GO

-- Standard Payer indexes
CREATE INDEX IX_StandardPayers_Active ON dbo.StandardPayers(Active) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_StandardPayers_Category ON dbo.StandardPayers(PayerCategory) WITH (DATA_COMPRESSION = ROW);
GO

-- Financial class indexes
CREATE INDEX IX_FinancialClasses_Facility ON dbo.FinancialClasses(FacilityId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_FinancialClasses_Payer ON dbo.FinancialClasses(StandardPayerId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_FinancialClasses_Active ON dbo.FinancialClasses(Active) WITH (DATA_COMPRESSION = ROW);
GO

-- Department indexes
CREATE INDEX IX_ClinicalDepartments_Facility ON dbo.ClinicalDepartments(FacilityId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_ClinicalDepartments_Code ON dbo.ClinicalDepartments(ClinicalDepartmentCode) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_ClinicalDepartments_Active ON dbo.ClinicalDepartments(Active) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_ClinicalDepartments_Type ON dbo.ClinicalDepartments(DepartmentType) WITH (DATA_COMPRESSION = ROW);
GO

-- Enhanced claim indexes (partitioned tables)
CREATE INDEX IX_Claims_Facility ON dbo.Claims(FacilityId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Claims_Department ON dbo.Claims(DepartmentId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Claims_FinancialClass ON dbo.Claims(FinancialClassId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Claims_Status_Facility ON dbo.Claims(ProcessingStatus, FacilityId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Claims_ServiceDate_Facility ON dbo.Claims(ServiceDate, FacilityId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Claims_ValidationStatus ON dbo.Claims(ValidationStatus) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Claims_ProcessingStatus ON dbo.Claims(ProcessingStatus) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Claims_CreatedDate ON dbo.Claims(CreatedDate) WITH (DATA_COMPRESSION = PAGE);
GO

-- CMS 1500 indexes
CREATE INDEX IX_Cms1500Diagnoses_Claim ON dbo.Cms1500Diagnoses(ClaimId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Cms1500Diagnoses_IcdCode ON dbo.Cms1500Diagnoses(IcdCode) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Cms1500LineItems_Claim ON dbo.Cms1500LineItems(ClaimId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Cms1500LineItems_CptCode ON dbo.Cms1500LineItems(CptCode) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Cms1500LineItems_ServiceDate ON dbo.Cms1500LineItems(ServiceDateFrom) WITH (DATA_COMPRESSION = PAGE);
GO

-- Failed Claims indexes
CREATE INDEX IX_FailedClaimDetails_OriginalClaimId ON dbo.FailedClaimDetails(OriginalClaimId) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_FailedClaimDetails_Status ON dbo.FailedClaimDetails(Status) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_FailedClaimDetails_ProcessingStage ON dbo.FailedClaimDetails(ProcessingStage) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_FailedClaimDetails_FailureTimestamp ON dbo.FailedClaimDetails(FailureTimestamp) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_FailedClaimDetails_FacilityId ON dbo.FailedClaimDetails(FacilityId) WITH (DATA_COMPRESSION = PAGE);
GO

-- ===========================
-- PERFORMANCE MONITORING VIEWS
-- ===========================

-- Real-time processing performance view
CREATE VIEW dbo.vw_ProcessingPerformance AS
SELECT
    -- Current processing metrics
    COUNT(*) as TotalClaimsInSystem,
    COUNT(CASE WHEN ProcessingStatus = 'PENDING' THEN 1 END) as PendingClaims,
    COUNT(CASE WHEN ProcessingStatus = 'PROCESSING' THEN 1 END) as ProcessingClaims,
    COUNT(CASE WHEN ProcessingStatus = 'COMPLETED' THEN 1 END) as CompletedClaims,
    COUNT(CASE WHEN ProcessingStatus = 'ERROR' THEN 1 END) as ErrorClaims,
    
    -- Performance metrics
    AVG(DATEDIFF(MINUTE, CreatedDate, ProcessedDate)) as AvgProcessingTimeMinutes,
    COUNT(CASE WHEN CreatedDate >= DATEADD(HOUR, -1, GETDATE()) THEN 1 END) as ClaimsLastHour,
    COUNT(CASE WHEN CreatedDate >= DATEADD(DAY, -1, GETDATE()) THEN 1 END) as ClaimsLast24Hours,
    
    -- Throughput calculations
    CASE 
        WHEN COUNT(CASE WHEN ProcessedDate >= DATEADD(HOUR, -1, GETDATE()) THEN 1 END) > 0 
        THEN COUNT(CASE WHEN ProcessedDate >= DATEADD(HOUR, -1, GETDATE()) THEN 1 END) / 1.0
        ELSE 0 
    END as ProcessedClaimsPerHour,
    
    -- Financial metrics
    SUM(TotalChargeAmount) as TotalChargesInSystem,
    AVG(TotalChargeAmount) as AvgClaimAmount,
    
    -- Data quality metrics
    COUNT(CASE WHEN ValidationStatus = 'VALIDATED' THEN 1 END) as ValidatedClaims,
    CAST(COUNT(CASE WHEN ValidationStatus = 'VALIDATED' THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as ValidationSuccessRate
FROM dbo.Claims
WHERE CreatedDate >= DATEADD(DAY, -30, GETDATE()); -- Last 30 days
GO

-- System health and capacity view
CREATE VIEW dbo.vw_SystemHealth AS
SELECT
    -- Table sizes and row counts
    'Claims' as TableName,
    COUNT(*) as RowCount,
    COUNT(*) * 8 / 1024.0 as EstimatedSizeMB, -- Rough estimate
    MAX(CreatedDate) as LastInsert,
    MIN(CreatedDate) as OldestRecord,
    COUNT(CASE WHEN CreatedDate >= DATEADD(DAY, -1, GETDATE()) THEN 1 END) as RecordsLast24Hours
FROM dbo.Claims

UNION ALL

SELECT
    'Cms1500LineItems' as TableName,
    COUNT(*) as RowCount,
    COUNT(*) * 4 / 1024.0 as EstimatedSizeMB,
    MAX(CreatedDate) as LastInsert,
    MIN(CreatedDate) as OldestRecord,
    COUNT(CASE WHEN CreatedDate >= DATEADD(DAY, -1, GETDATE()) THEN 1 END) as RecordsLast24Hours
FROM dbo.Cms1500LineItems

UNION ALL

SELECT
    'FailedClaimDetails' as TableName,
    COUNT(*) as RowCount,
    COUNT(*) * 6 / 1024.0 as EstimatedSizeMB,
    MAX(CreatedDate) as LastInsert,
    MIN(CreatedDate) as OldestRecord,
    COUNT(CASE WHEN CreatedDate >= DATEADD(DAY, -1, GETDATE()) THEN 1 END) as RecordsLast24Hours
FROM dbo.FailedClaimDetails;
GO

-- Partition performance view
CREATE VIEW dbo.vw_PartitionPerformance AS
SELECT
    t.name as TableName,
    p.partition_number,
    p.rows as RowCount,
    CAST(p.rows * 8 / 1024.0 AS DECIMAL(10,2)) as EstimatedSizeMB,
    rv.value as PartitionBoundary,
    fg.name as FileGroupName
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
INNER JOIN sys.partition_schemes ps ON p.partition_number = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id 
    AND p.partition_number = rv.boundary_id + 1
LEFT JOIN sys.filegroups fg ON p.data_compression_desc = fg.name
WHERE t.name IN ('Claims', 'Cms1500LineItems')
    AND p.index_id IN (0,1); -- Clustered index or heap
GO

-- Failed claims analytics view
CREATE VIEW dbo.vw_FailedClaimsAnalytics AS
SELECT
    ProcessingStage,
    Status,
    COUNT(*) as FailureCount,
    COUNT(CASE WHEN FailureTimestamp >= DATEADD(DAY, -1, GETDATE()) THEN 1 END) as FailuresLast24Hours,
    COUNT(CASE WHEN FailureTimestamp >= DATEADD(DAY, -7, GETDATE()) THEN 1 END) as FailuresLast7Days,
    AVG(CASE WHEN ResolvedTimestamp IS NOT NULL 
        THEN DATEDIFF(HOUR, FailureTimestamp, ResolvedTimestamp) END) as AvgResolutionTimeHours,
    COUNT(CASE WHEN Status = 'Resolved' THEN 1 END) as ResolvedCount,
    CAST(COUNT(CASE WHEN Status = 'Resolved' THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as ResolutionRate
FROM dbo.FailedClaimDetails
WHERE FailureTimestamp >= DATEADD(DAY, -30, GETDATE()) -- Last 30 days
GROUP BY ProcessingStage, Status;
GO

-- Facility performance view with compression statistics
CREATE VIEW dbo.vw_FacilityPerformance AS
SELECT
    f.FacilityId,
    f.FacilityName,
    f.FacilityType,
    f.CriticalAccess,
    
    -- Claim volumes
    COUNT(c.ClaimId) as TotalClaims,
    COUNT(CASE WHEN c.CreatedDate >= DATEADD(DAY, -1, GETDATE()) THEN 1 END) as ClaimsLast24Hours,
    COUNT(CASE WHEN c.CreatedDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 END) as ClaimsLast7Days,
    
    -- Processing performance
    AVG(CASE WHEN c.ProcessedDate IS NOT NULL 
        THEN DATEDIFF(MINUTE, c.CreatedDate, c.ProcessedDate) END) as AvgProcessingTimeMinutes,
    COUNT(CASE WHEN c.ProcessingStatus = 'COMPLETED' THEN 1 END) as CompletedClaims,
    COUNT(CASE WHEN c.ProcessingStatus = 'ERROR' THEN 1 END) as ErrorClaims,
    
    -- Financial metrics
    SUM(c.TotalChargeAmount) as TotalCharges,
    AVG(c.TotalChargeAmount) as AvgChargeAmount,
    
    -- Quality metrics
    COUNT(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 END) as ValidatedClaims,
    CAST(COUNT(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 END) * 100.0 / 
         NULLIF(COUNT(c.ClaimId), 0) AS DECIMAL(5,2)) as ValidationSuccessRate,
    
    -- Failed claims
    (SELECT COUNT(*) FROM dbo.FailedClaimDetails fcd WHERE fcd.FacilityId = f.FacilityId) as FailedClaimsCount
    
FROM dbo.Facilities f
LEFT JOIN dbo.Claims c ON f.FacilityId = c.FacilityId
WHERE f.Active = 1
GROUP BY f.FacilityId, f.FacilityName, f.FacilityType, f.CriticalAccess;
GO

-- Index usage and performance view
CREATE VIEW dbo.vw_IndexPerformance AS
SELECT
    OBJECT_SCHEMA_NAME(i.object_id) as SchemaName,
    OBJECT_NAME(i.object_id) as TableName,
    i.name as IndexName,
    i.type_desc as IndexType,
    ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates,
    ius.user_seeks + ius.user_scans + ius.user_lookups as TotalReads,
    CASE 
        WHEN ius.user_updates > 0 
        THEN (ius.user_seeks + ius.user_scans + ius.user_lookups) / CAST(ius.user_updates AS FLOAT)
        ELSE NULL
    END as ReadWriteRatio,
    ps.avg_fragmentation_in_percent,
    ps.page_count
FROM sys.indexes i
LEFT JOIN sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id
LEFT JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps 
    ON i.object_id = ps.object_id AND i.index_id = ps.index_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) = 'dbo'
    AND i.type > 0 -- Exclude heaps
    AND OBJECT_NAME(i.object_id) IN ('Claims', 'Cms1500LineItems', 'Cms1500Diagnoses', 'FailedClaimDetails');
GO

-- ===========================
-- VIEWS FOR REPORTING (Enhanced)
-- ===========================

CREATE VIEW dbo.vw_FacilityDetails AS
SELECT
    f.FacilityId,
    f.FacilityName,
    f.FacilityCode,
    f.City,
    f.StateCode,
    f.BedSize,
    f.FiscalReportingMonth,
    f.EmrSystem,
    f.FacilityType,
    f.CriticalAccess,
    f.Active as FacilityActive,

    -- Organization details
    o.OrganizationId,
    o.OrganizationName,
    o.OrganizationCode,

    -- Region details
    r.RegionId,
    r.RegionName,
    r.RegionCode,

    -- Aggregated counts
    (SELECT COUNT(*) FROM dbo.FinancialClasses fc WHERE fc.FacilityId = f.FacilityId AND fc.Active = 1) as ActiveFinancialClasses,
    (SELECT COUNT(*) FROM dbo.ClinicalDepartments cd WHERE cd.FacilityId = f.FacilityId AND cd.Active = 1) as ActiveDepartments,

    f.CreatedDate,
    f.UpdatedDate
FROM dbo.Facilities f
LEFT JOIN dbo.Organizations o ON f.OrganizationId = o.OrganizationId
LEFT JOIN dbo.Regions r ON f.RegionId = r.RegionId;
GO

CREATE VIEW dbo.vw_ClaimsByFacility AS
SELECT
    f.FacilityId,
    f.FacilityName,
    f.OrganizationId,
    f.RegionId,

    COUNT(c.ClaimId) as TotalClaims,
    COUNT(CASE WHEN c.ProcessingStatus = 'PENDING' THEN 1 END) as PendingClaims,
    COUNT(CASE WHEN c.ProcessingStatus = 'COMPLETED' THEN 1 END) as CompletedClaims,
    COUNT(CASE WHEN c.ProcessingStatus = 'ERROR' THEN 1 END) as ErrorClaims,
    COUNT(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 END) as ValidatedClaims,

    ISNULL(SUM(c.TotalChargeAmount), 0) as TotalCharges,
    ISNULL(AVG(c.TotalChargeAmount), 0) as AvgChargeAmount,

    MIN(c.ServiceDate) as EarliestServiceDate,
    MAX(c.ServiceDate) as LatestServiceDate,

    COUNT(DISTINCT c.DepartmentId) as DepartmentsWithClaims,
    COUNT(DISTINCT c.FinancialClassId) as FinancialClassesWithClaims

FROM dbo.Facilities f
LEFT JOIN dbo.Claims c ON f.FacilityId = c.FacilityId
GROUP BY f.FacilityId, f.FacilityName, f.OrganizationId, f.RegionId;
GO

CREATE VIEW dbo.vw_FinancialClassSummary AS
SELECT
    fc.FinancialClassId,
    fc.FinancialClassDescription,
    fc.FacilityId,
    f.FacilityName,

    sp.StandardPayerName,
    sp.StandardPayerCode,
    sp.PayerCategory,

    ht.HccCode,
    ht.HccName,
    ht.HccAgency,

    fc.Active as FinancialClassActive,
    fc.EffectiveDate,
    fc.EndDate,

    -- Claims using this financial class
    (SELECT COUNT(*) FROM dbo.Claims c WHERE c.FinancialClassId = fc.FinancialClassId) as TotalClaims

FROM dbo.FinancialClasses fc
LEFT JOIN dbo.Facilities f ON fc.FacilityId = f.FacilityId
LEFT JOIN dbo.StandardPayers sp ON fc.StandardPayerId = sp.StandardPayerId
LEFT JOIN dbo.HccTypes ht ON fc.HccTypeId = ht.HccTypeId;
GO

CREATE VIEW dbo.vw_DepartmentUtilization AS
SELECT
    cd.DepartmentId,
    cd.ClinicalDepartmentCode,
    cd.DepartmentDescription,
    cd.FacilityId,
    f.FacilityName,
    cd.DepartmentType,
    cd.IsSurgical,
    cd.IsEmergency,
    cd.IsCriticalCare,

    COUNT(c.ClaimId) as TotalClaims,
    ISNULL(SUM(c.TotalChargeAmount), 0) as TotalCharges,
    ISNULL(AVG(c.TotalChargeAmount), 0) as AvgChargePerClaim,

    COUNT(CASE WHEN c.ServiceDate >= DATEADD(day, -30, GETDATE()) THEN c.ClaimId END) as ClaimsLast30Days,
    COUNT(CASE WHEN c.ServiceDate >= DATEADD(day, -90, GETDATE()) THEN c.ClaimId END) as ClaimsLast90Days,

    MIN(c.ServiceDate) as FirstClaimDate,
    MAX(c.ServiceDate) as LastClaimDate

FROM dbo.ClinicalDepartments cd
LEFT JOIN dbo.Facilities f ON cd.FacilityId = f.FacilityId
LEFT JOIN dbo.Claims c ON cd.DepartmentId = c.DepartmentId
WHERE cd.Active = 1
GROUP BY cd.DepartmentId, cd.ClinicalDepartmentCode, cd.DepartmentDescription,
         cd.FacilityId, f.FacilityName, cd.DepartmentType, cd.IsSurgical,
         cd.IsEmergency, cd.IsCriticalCare;
GO

-- ===========================
-- STORED PROCEDURES (Enhanced)
-- ===========================

CREATE PROCEDURE dbo.sp_GetFacilityHierarchy
    @FacilityId NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT
        f.FacilityId, f.FacilityName, f.FacilityCode, f.FacilityType, f.CriticalAccess,
        o.OrganizationId, o.OrganizationName, o.OrganizationCode,
        r.RegionId, r.RegionName, r.RegionCode
    FROM dbo.Facilities f
    LEFT JOIN dbo.Organizations o ON f.OrganizationId = o.OrganizationId
    LEFT JOIN dbo.Regions r ON f.RegionId = r.RegionId
    WHERE f.FacilityId = @FacilityId;
END;
GO

CREATE PROCEDURE dbo.sp_ValidateFacilityClaimAssignment
    @ClaimId NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @IsValid BIT = 1;
    DECLARE @Errors NVARCHAR(MAX) = '';
    DECLARE @Warnings NVARCHAR(MAX) = '';

    IF OBJECT_ID('tempdb..#ClaimValidation') IS NOT NULL
        DROP TABLE #ClaimValidation;

    SELECT
        c.ClaimId, c.FacilityId, c.DepartmentId, c.FinancialClassId,
        f.FacilityName, f.Active as FacilityActive,
        d.DepartmentDescription, d.Active as DepartmentActive, d.FacilityId as DeptFacilityId,
        fc.FinancialClassDescription, fc.Active as FcActive, fc.FacilityId as FcFacilityId
    INTO #ClaimValidation
    FROM dbo.Claims c
    LEFT JOIN dbo.Facilities f ON c.FacilityId = f.FacilityId
    LEFT JOIN dbo.ClinicalDepartments d ON c.DepartmentId = d.DepartmentId
    LEFT JOIN dbo.FinancialClasses fc ON c.FinancialClassId = fc.FinancialClassId
    WHERE c.ClaimId = @ClaimId;

    IF NOT EXISTS (SELECT 1 FROM #ClaimValidation)
    BEGIN
        SET @IsValid = 0;
        SET @Errors = 'Claim not found in dbo.Claims or related tables.';
    END
    ELSE
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM #ClaimValidation WHERE FacilityId IS NOT NULL) BEGIN SET @IsValid = 0; SET @Errors = @Errors + 'Claim is not assigned to any facility; '; END
        ELSE IF NOT EXISTS (SELECT 1 FROM #ClaimValidation WHERE FacilityName IS NOT NULL) BEGIN SET @IsValid = 0; SET @Errors = @Errors + 'Assigned facility does not exist; '; END
        ELSE IF EXISTS (SELECT 1 FROM #ClaimValidation WHERE FacilityActive = 0) BEGIN SET @Warnings = @Warnings + 'Assigned facility is inactive; '; END

        IF EXISTS (SELECT 1 FROM #ClaimValidation WHERE DepartmentId IS NOT NULL AND DepartmentDescription IS NULL) BEGIN SET @IsValid = 0; SET @Errors = @Errors + 'Assigned department does not exist; '; END
        ELSE IF EXISTS (SELECT 1 FROM #ClaimValidation WHERE DepartmentId IS NOT NULL AND DepartmentActive = 0) BEGIN SET @Warnings = @Warnings + 'Assigned department is inactive; '; END
        ELSE IF EXISTS (SELECT 1 FROM #ClaimValidation WHERE DepartmentId IS NOT NULL AND DeptFacilityId != FacilityId) BEGIN SET @IsValid = 0; SET @Errors = @Errors + 'Department belongs to a different facility; '; END

        IF EXISTS (SELECT 1 FROM #ClaimValidation WHERE FinancialClassId IS NOT NULL AND FinancialClassDescription IS NULL) BEGIN SET @IsValid = 0; SET @Errors = @Errors + 'Assigned financial class does not exist; '; END
        ELSE IF EXISTS (SELECT 1 FROM #ClaimValidation WHERE FinancialClassId IS NOT NULL AND FcActive = 0) BEGIN SET @Warnings = @Warnings + 'Assigned financial class is inactive; '; END
        ELSE IF EXISTS (SELECT 1 FROM #ClaimValidation WHERE FinancialClassId IS NOT NULL AND FcFacilityId IS NOT NULL AND FcFacilityId != FacilityId) BEGIN SET @IsValid = 0; SET @Errors = @Errors + 'Financial class belongs to a different facility; '; END
    END

    SELECT
        @IsValid as IsValid,
        RTRIM(STUFF(REPLACE(@Errors, '; ', ';'), 1, 0, '')) as Errors,
        RTRIM(STUFF(REPLACE(@Warnings, '; ', ';'), 1, 0, '')) as Warnings,
        (SELECT TOP 1 FacilityId, FacilityName, DepartmentDescription, FinancialClassDescription
         FROM #ClaimValidation FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) as FacilityInfo;

    IF OBJECT_ID('tempdb..#ClaimValidation') IS NOT NULL
        DROP TABLE #ClaimValidation;
END;
GO

CREATE PROCEDURE dbo.sp_BulkAssignClaimsToFacility
    @ClaimIds NVARCHAR(MAX),
    @FacilityId NVARCHAR(50),
    @DepartmentId INT = NULL,
    @FinancialClassId NVARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @UpdateCount INT = 0;
    DECLARE @ErrorMessage NVARCHAR(MAX) = '';

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM dbo.Facilities WHERE FacilityId = @FacilityId)
        BEGIN SET @ErrorMessage = 'Facility ' + @FacilityId + ' does not exist'; SELECT 0 as Success, @ErrorMessage as ErrorMessage, 0 as UpdatedCount; RETURN; END

        IF @DepartmentId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.ClinicalDepartments WHERE DepartmentId = @DepartmentId AND FacilityId = @FacilityId)
        BEGIN SET @ErrorMessage = 'Department ' + CAST(@DepartmentId AS NVARCHAR(10)) + ' does not belong to facility ' + @FacilityId; SELECT 0 as Success, @ErrorMessage as ErrorMessage, 0 as UpdatedCount; RETURN; END

        IF @FinancialClassId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.FinancialClasses WHERE FinancialClassId = @FinancialClassId AND (FacilityId = @FacilityId OR FacilityId IS NULL))
        BEGIN SET @ErrorMessage = 'Financial class ' + @FinancialClassId + ' is not valid for facility ' + @FacilityId; SELECT 0 as Success, @ErrorMessage as ErrorMessage, 0 as UpdatedCount; RETURN; END

        IF OBJECT_ID('tempdb..#ClaimIdList') IS NOT NULL DROP TABLE #ClaimIdList;
        CREATE TABLE #ClaimIdList (ClaimId NVARCHAR(50) PRIMARY KEY);
        INSERT INTO #ClaimIdList (ClaimId) SELECT value FROM STRING_SPLIT(@ClaimIds, ',');

        UPDATE c SET FacilityId = @FacilityId, DepartmentId = @DepartmentId, FinancialClassId = @FinancialClassId, UpdatedDate = GETDATE()
        FROM dbo.Claims c INNER JOIN #ClaimIdList cil ON c.ClaimId = cil.ClaimId;
        SET @UpdateCount = @@ROWCOUNT;

        SELECT 1 as Success, '' as ErrorMessage, @UpdateCount as UpdatedCount;
        IF OBJECT_ID('tempdb..#ClaimIdList') IS NOT NULL DROP TABLE #ClaimIdList;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        SELECT 0 as Success, @ErrorMessage as ErrorMessage, 0 as UpdatedCount;
        IF OBJECT_ID('tempdb..#ClaimIdList') IS NOT NULL DROP TABLE #ClaimIdList;
    END CATCH
END;
GO

-- Performance optimization procedure
CREATE PROCEDURE dbo.sp_PerformanceOptimization
    @Action NVARCHAR(50) = 'ANALYZE', -- ANALYZE, REBUILD_INDEXES, UPDATE_STATS, MANAGE_PARTITIONS
    @TableName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @Action = 'ANALYZE'
    BEGIN
        -- Return performance analysis
        SELECT 'Performance Analysis Results' as Action;
        
        SELECT * FROM dbo.vw_ProcessingPerformance;
        SELECT * FROM dbo.vw_SystemHealth;
        SELECT * FROM dbo.vw_PartitionPerformance;
        
        -- Show fragmented indexes
        SELECT 
            SchemaName, TableName, IndexName, 
            avg_fragmentation_in_percent as FragmentationPercent,
            page_count as PageCount
        FROM dbo.vw_IndexPerformance 
        WHERE avg_fragmentation_in_percent > 10
        ORDER BY avg_fragmentation_in_percent DESC;
    END
    
    ELSE IF @Action = 'REBUILD_INDEXES'
    BEGIN
        DECLARE @SQL NVARCHAR(MAX);
        DECLARE @IndexName NVARCHAR(128);
        DECLARE @SchemaName NVARCHAR(128);
        DECLARE @TableNameLocal NVARCHAR(128);
        
        DECLARE index_cursor CURSOR FOR
        SELECT SchemaName, TableName, IndexName
        FROM dbo.vw_IndexPerformance 
        WHERE avg_fragmentation_in_percent > 30
            AND (@TableName IS NULL OR TableName = @TableName)
            AND page_count > 1000; -- Only rebuild substantial indexes
        
        OPEN index_cursor;
        FETCH NEXT FROM index_cursor INTO @SchemaName, @TableNameLocal, @IndexName;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @SchemaName + '].[' + @TableNameLocal + '] REBUILD WITH (ONLINE = ON, DATA_COMPRESSION = PAGE);';
            
            BEGIN TRY
                EXEC sp_executesql @SQL;
                PRINT 'Rebuilt index: ' + @IndexName + ' on ' + @SchemaName + '.' + @TableNameLocal;
            END TRY
            BEGIN CATCH
                PRINT 'Failed to rebuild index: ' + @IndexName + ' - ' + ERROR_MESSAGE();
            END CATCH
            
            FETCH NEXT FROM index_cursor INTO @SchemaName, @TableNameLocal, @IndexName;
        END
        
        CLOSE index_cursor;
        DEALLOCATE index_cursor;
    END
    
    ELSE IF @Action = 'UPDATE_STATS'
    BEGIN
        -- Update statistics for main tables
        UPDATE STATISTICS dbo.Claims WITH FULLSCAN;
        UPDATE STATISTICS dbo.Cms1500LineItems WITH FULLSCAN;
        UPDATE STATISTICS dbo.Cms1500Diagnoses WITH FULLSCAN;
        UPDATE STATISTICS dbo.FailedClaimDetails WITH FULLSCAN;
        
        PRINT 'Statistics updated for main tables';
    END
    
    ELSE IF @Action = 'MANAGE_PARTITIONS'
    BEGIN
        -- Add future partitions if needed
        DECLARE @NextBoundary DATE = DATEADD(MONTH, 1, EOMONTH(GETDATE()));
        DECLARE @PartitionFunction NVARCHAR(128);
        
        -- Check if we need to add partitions for next 3 months
        WHILE @NextBoundary <= DATEADD(MONTH, 3, GETDATE())
        BEGIN
            -- Check if partition already exists for this boundary
            IF NOT EXISTS (
                SELECT 1 FROM sys.partition_range_values rv
                INNER JOIN sys.partition_functions pf ON rv.function_id = pf.function_id
                WHERE pf.name IN ('pf_ClaimsByServiceDate', 'pf_LineItemsByServiceDate')
                    AND rv.value = @NextBoundary
            )
            BEGIN
                -- Add partition (this would need to be customized based on your partitioning strategy)
                PRINT 'Would add partition for boundary: ' + CAST(@NextBoundary AS NVARCHAR(10));
                -- ALTER PARTITION SCHEME ps_ClaimsByServiceDate NEXT USED [PRIMARY];
                -- ALTER PARTITION FUNCTION pf_ClaimsByServiceDate() SPLIT RANGE (@NextBoundary);
            END
            
            SET @NextBoundary = DATEADD(MONTH, 1, @NextBoundary);
        END
    END
END;
GO

-- ===========================
-- SAMPLE DATA INSERTION
-- ===========================

INSERT INTO dbo.StandardPayers (StandardPayerCode, StandardPayerName, PayerCategory) VALUES
('MEDICARE', 'Medicare', 'Government'),
('MEDICAID', 'Medicaid', 'Government'),
('BLUECROSS', 'Blue Cross', 'Commercial'),
('OTHERS', 'Others', 'Other'),
('SELFPAY', 'Self Pay', 'Self Pay'),
('HMO_PPO', 'Managed Care (HMO/PPO)', 'Managed Care'),
('TRICARE', 'Tricare/Champus', 'Government'),
('COMMERCIAL', 'Commercial', 'Commercial'),
('WORKCOMP', 'Workers Compensation', 'Workers Comp'),
('MEDICARE_ADV', 'Medicare Advantage', 'Government');
GO

INSERT INTO dbo.HccTypes (HccCode, HccName, HccAgency) VALUES
('CMS_V24', 'CMS-HCC Version 24', 'CMS'),
('CMS_V28', 'CMS-HCC Version 28', 'CMS'),
('HHS_V05', 'HHS-HCC Version 05', 'HHS'),
('HHS_V07', 'HHS-HCC Version 07', 'HHS');
GO

INSERT INTO dbo.Organizations (OrganizationName, OrganizationCode, City, StateCode, ZipCode) VALUES
('Regional Health System', 'RHS', 'Springfield', 'IL', '62701'),
('Metro Healthcare Network', 'MHN', 'Chicago', 'IL', '60601'),
('Community Care Partners', 'CCP', 'Peoria', 'IL', '61602');
GO

INSERT INTO dbo.Regions (RegionName, RegionCode, Description) VALUES
('Central Illinois', 'CIL', 'Central Illinois Region'),
('Northern Illinois', 'NIL', 'Northern Illinois Region'),
('Southern Illinois', 'SIL', 'Southern Illinois Region');
GO

INSERT INTO dbo.OrganizationRegions (OrganizationId, RegionId, IsPrimary)
SELECT o.OrganizationId, r.RegionId, 1
FROM dbo.Organizations o, dbo.Regions r
WHERE o.OrganizationCode = 'RHS' AND r.RegionCode = 'CIL';
GO

INSERT INTO dbo.Facilities (FacilityId, FacilityName, FacilityCode, OrganizationId, RegionId, City, StateCode, BedSize, FiscalReportingMonth, EmrSystem, FacilityType, CriticalAccess)
SELECT 'FAC001', 'Springfield General Hospital', 'SGH', o.OrganizationId, r.RegionId, 'Springfield', 'IL', 350, 7, 'Epic', 'Teaching', 'N'
FROM dbo.Organizations o, dbo.Regions r
WHERE o.OrganizationCode = 'RHS' AND r.RegionCode = 'CIL';
GO

INSERT INTO dbo.FinancialClasses (FinancialClassId, FinancialClassDescription, StandardPayerId, FacilityId)
SELECT 'FC_MEDICARE_001', 'Medicare Part A', sp.StandardPayerId, 'FAC001'
FROM dbo.StandardPayers sp
WHERE sp.StandardPayerCode = 'MEDICARE';
GO

INSERT INTO dbo.ClinicalDepartments (ClinicalDepartmentCode, DepartmentDescription, FacilityId, DepartmentType, DefaultPlaceOfService, IsSurgical, IsEmergency)
VALUES
('CARDIO', 'Cardiology', 'FAC001', 'Outpatient', '11', 0, 0),
('EMER', 'Emergency Department', 'FAC001', 'Emergency', '23', 0, 1),
('SURG', 'Surgery', 'FAC001', 'Inpatient', '21', 1, 0),
('ICU', 'Intensive Care Unit', 'FAC001', 'Inpatient', '21', 0, 0);
GO

-- ===========================
-- FUNCTIONS
-- ===========================

CREATE FUNCTION dbo.fn_GetFacilitySummary(@FacilityId NVARCHAR(50))
RETURNS TABLE
AS
RETURN
(
    SELECT
        f.FacilityId,
        f.FacilityName,
        f.FacilityType,
        f.CriticalAccess,
        f.BedSize,
        o.OrganizationName,
        r.RegionName,
        ISNULL(cs.TotalClaims, 0) as TotalClaims,
        ISNULL(cs.PendingClaims, 0) as PendingClaims,
        ISNULL(cs.CompletedClaims, 0) as CompletedClaims,
        ISNULL(cs.TotalCharges, 0) as TotalCharges,
        ISNULL(cs.AvgChargeAmount, 0) as AvgChargeAmount,
        (SELECT COUNT(*) FROM dbo.ClinicalDepartments cd WHERE cd.FacilityId = f.FacilityId AND cd.Active = 1) as TotalDepartments,
        (SELECT COUNT(*) FROM dbo.FinancialClasses fc WHERE fc.FacilityId = f.FacilityId AND fc.Active = 1) as TotalFinancialClasses
    FROM dbo.Facilities f
    LEFT JOIN dbo.Organizations o ON f.OrganizationId = o.OrganizationId
    LEFT JOIN dbo.Regions r ON f.RegionId = r.RegionId
    LEFT JOIN dbo.vw_ClaimsByFacility cs ON f.FacilityId = cs.FacilityId
    WHERE f.FacilityId = @FacilityId
);
GO

-- ===========================
-- TRIGGERS FOR AUDIT TRAIL
-- ===========================

CREATE TRIGGER tr_Facilities_UpdatedDate ON dbo.Facilities AFTER UPDATE AS
BEGIN SET NOCOUNT ON; UPDATE dbo.Facilities SET UpdatedDate = GETDATE() FROM dbo.Facilities f INNER JOIN inserted i ON f.FacilityId = i.FacilityId; END;
GO

CREATE TRIGGER tr_FinancialClasses_UpdatedDate ON dbo.FinancialClasses AFTER UPDATE AS
BEGIN SET NOCOUNT ON; UPDATE dbo.FinancialClasses SET UpdatedDate = GETDATE() FROM dbo.FinancialClasses fc INNER JOIN inserted i ON fc.FinancialClassId = i.FinancialClassId; END;
GO

CREATE TRIGGER tr_ClinicalDepartments_UpdatedDate ON dbo.ClinicalDepartments AFTER UPDATE AS
BEGIN SET NOCOUNT ON; UPDATE dbo.ClinicalDepartments SET UpdatedDate = GETDATE() FROM dbo.ClinicalDepartments cd INNER JOIN inserted i ON cd.DepartmentId = i.DepartmentId; END;
GO

-- ===========================
-- MAINTENANCE JOBS (Templates)
-- ===========================

-- Create a stored procedure for automated maintenance
CREATE PROCEDURE dbo.sp_AutomatedMaintenance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Log maintenance start
    PRINT 'Starting automated maintenance at ' + CAST(GETDATE() AS NVARCHAR(50));
    
    -- Update statistics
    EXEC dbo.sp_PerformanceOptimization @Action = 'UPDATE_STATS';
    
    -- Analyze performance and rebuild highly fragmented indexes
    DECLARE @FragmentedIndexes INT;
    SELECT @FragmentedIndexes = COUNT(*)
    FROM dbo.vw_IndexPerformance 
    WHERE avg_fragmentation_in_percent > 30;
    
    IF @FragmentedIndexes > 0
    BEGIN
        PRINT 'Found ' + CAST(@FragmentedIndexes AS NVARCHAR(10)) + ' highly fragmented indexes. Rebuilding...';
        EXEC dbo.sp_PerformanceOptimization @Action = 'REBUILD_INDEXES';
    END
    
    -- Archive old failed claims (older than 90 days and resolved)
    DELETE FROM dbo.FailedClaimDetails 
    WHERE Status = 'Resolved' 
        AND ResolvedTimestamp < DATEADD(DAY, -90, GETDATE());
    
    DECLARE @ArchivedCount INT = @@ROWCOUNT;
    IF @ArchivedCount > 0
    BEGIN
        PRINT 'Archived ' + CAST(@ArchivedCount AS NVARCHAR(10)) + ' old resolved failed claims.';
    END
    
    -- Manage partitions
    EXEC dbo.sp_PerformanceOptimization @Action = 'MANAGE_PARTITIONS';
    
    PRINT 'Automated maintenance completed at ' + CAST(GETDATE() AS NVARCHAR(50));
END;
GO

-- ===========================
-- COMPRESSION MONITORING
-- ===========================

CREATE VIEW dbo.vw_CompressionStats AS
SELECT 
    s.name as SchemaName,
    t.name as TableName,
    i.name as IndexName,
    p.partition_number,
    p.data_compression_desc as CompressionType,
    p.rows as RowCount,
    CAST(SUM(au.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)) as SizeMB,
    CAST(SUM(au.used_pages) * 8.0 / 1024 AS DECIMAL(10,2)) as UsedMB,
    CAST(SUM(au.data_pages) * 8.0 / 1024 AS DECIMAL(10,2)) as DataMB
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.allocation_units au ON p.partition_id = au.container_id
WHERE s.name = 'dbo'
    AND t.name IN ('Claims', 'Cms1500LineItems', 'Cms1500Diagnoses', 'FailedClaimDetails')
GROUP BY s.name, t.name, i.name, p.partition_number, p.data_compression_desc, p.rows;
GO

-- ===========================
-- REAL-TIME MONITORING VIEWS
-- ===========================

-- Real-time claim processing dashboard
CREATE VIEW dbo.vw_RealTimeDashboard AS
SELECT
    -- Current timestamp
    GETDATE() as LastUpdated,
    
    -- Processing volumes
    (SELECT COUNT(*) FROM dbo.Claims WHERE CreatedDate >= CAST(GETDATE() AS DATE)) as ClaimsToday,
    (SELECT COUNT(*) FROM dbo.Claims WHERE CreatedDate >= DATEADD(HOUR, -1, GETDATE())) as ClaimsLastHour,
    (SELECT COUNT(*) FROM dbo.Claims WHERE ProcessingStatus = 'PENDING') as PendingClaims,
    (SELECT COUNT(*) FROM dbo.Claims WHERE ProcessingStatus = 'PROCESSING') as ProcessingClaims,
    
    -- Error rates
    (SELECT COUNT(*) FROM dbo.FailedClaimDetails WHERE FailureTimestamp >= CAST(GETDATE() AS DATE)) as FailuresToday,
    (SELECT COUNT(*) FROM dbo.FailedClaimDetails WHERE FailureTimestamp >= DATEADD(HOUR, -1, GETDATE())) as FailuresLastHour,
    
    -- Performance metrics
    (SELECT AVG(DATEDIFF(SECOND, CreatedDate, ProcessedDate)) 
     FROM dbo.Claims 
     WHERE ProcessedDate >= DATEADD(HOUR, -1, GETDATE())) as AvgProcessingTimeSeconds,
    
    -- Financial metrics
    (SELECT SUM(TotalChargeAmount) FROM dbo.Claims WHERE CreatedDate >= CAST(GETDATE() AS DATE)) as TotalChargesProcessedToday,
    
    -- System capacity
    (SELECT COUNT(*) FROM dbo.Claims) as TotalClaimsInSystem,
    (SELECT COUNT(DISTINCT FacilityId) FROM dbo.Claims WHERE CreatedDate >= CAST(GETDATE() AS DATE)) as ActiveFacilitiesToday;
GO

-- Alert conditions view
CREATE VIEW dbo.vw_SystemAlerts AS
SELECT
    AlertType,
    AlertMessage,
    Severity,
    AlertValue,
    Threshold,
    GETDATE() as CheckTime
FROM (
    -- High error rate alert
    SELECT 
        'ERROR_RATE' as AlertType,
        'High error rate detected in last hour' as AlertMessage,
        'HIGH' as Severity,
        CAST((SELECT COUNT(*) FROM dbo.FailedClaimDetails WHERE FailureTimestamp >= DATEADD(HOUR, -1, GETDATE())) AS FLOAT) /
        NULLIF((SELECT COUNT(*) FROM dbo.Claims WHERE CreatedDate >= DATEADD(HOUR, -1, GETDATE())), 0) * 100 as AlertValue,
        10.0 as Threshold
    
    UNION ALL
    
    -- Slow processing alert
    SELECT 
        'SLOW_PROCESSING' as AlertType,
        'Average processing time exceeds threshold' as AlertMessage,
        'MEDIUM' as Severity,
        (SELECT AVG(CAST(DATEDIFF(SECOND, CreatedDate, ProcessedDate) AS FLOAT)) 
         FROM dbo.Claims 
         WHERE ProcessedDate >= DATEADD(HOUR, -1, GETDATE())) as AlertValue,
        300.0 as Threshold -- 5 minutes
    
    UNION ALL
    
    -- High pending volume alert
    SELECT 
        'HIGH_PENDING_VOLUME' as AlertType,
        'Large number of pending claims detected' as AlertMessage,
        'MEDIUM' as Severity,
        CAST((SELECT COUNT(*) FROM dbo.Claims WHERE ProcessingStatus = 'PENDING') AS FLOAT) as AlertValue,
        1000.0 as Threshold
    
    UNION ALL
    
    -- Database size alert
    SELECT 
        'DATABASE_SIZE' as AlertType,
        'Database size approaching capacity' as AlertMessage,
        'LOW' as Severity,
        (SELECT SUM(SizeMB) FROM dbo.vw_CompressionStats) as AlertValue,
        10000.0 as Threshold -- 10GB
) alerts
WHERE AlertValue > Threshold;
GO

-- ===========================
-- AUTOMATED PARTITION MANAGEMENT
-- ===========================

CREATE PROCEDURE dbo.sp_ManagePartitions
    @Action NVARCHAR(20) = 'ADD_FUTURE', -- ADD_FUTURE, DROP_OLD, ANALYZE
    @MonthsAhead INT = 6,
    @MonthsToKeep INT = 24
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @Action = 'ADD_FUTURE'
    BEGIN
        -- Add partitions for future months
        DECLARE @CurrentBoundary DATE = DATEADD(MONTH, 1, EOMONTH(GETDATE()));
        DECLARE @EndBoundary DATE = DATEADD(MONTH, @MonthsAhead, EOMONTH(GETDATE()));
        DECLARE @SQL NVARCHAR(MAX);
        
        WHILE @CurrentBoundary <= @EndBoundary
        BEGIN
            -- Check if partition boundary already exists
            IF NOT EXISTS (
                SELECT 1 FROM sys.partition_range_values rv
                INNER JOIN sys.partition_functions pf ON rv.function_id = pf.function_id
                WHERE pf.name = 'pf_ClaimsByServiceDate'
                    AND CAST(rv.value AS DATE) = @CurrentBoundary
            )
            BEGIN
                -- Add new partition boundary
                SET @SQL = 'ALTER PARTITION SCHEME ps_ClaimsByServiceDate NEXT USED [PRIMARY]; ' +
                          'ALTER PARTITION FUNCTION pf_ClaimsByServiceDate() SPLIT RANGE (''' + 
                          CAST(@CurrentBoundary AS NVARCHAR(10)) + ''');';
                
                BEGIN TRY
                    EXEC sp_executesql @SQL;
                    PRINT 'Added partition boundary for Claims: ' + CAST(@CurrentBoundary AS NVARCHAR(10));
                END TRY
                BEGIN CATCH
                    PRINT 'Failed to add partition boundary for Claims: ' + ERROR_MESSAGE();
                END CATCH
            END
            
            -- Same for LineItems
            IF NOT EXISTS (
                SELECT 1 FROM sys.partition_range_values rv
                INNER JOIN sys.partition_functions pf ON rv.function_id = pf.function_id
                WHERE pf.name = 'pf_LineItemsByServiceDate'
                    AND CAST(rv.value AS DATE) = @CurrentBoundary
            )
            BEGIN
                SET @SQL = 'ALTER PARTITION SCHEME ps_LineItemsByServiceDate NEXT USED [PRIMARY]; ' +
                          'ALTER PARTITION FUNCTION pf_LineItemsByServiceDate() SPLIT RANGE (''' + 
                          CAST(@CurrentBoundary AS NVARCHAR(10)) + ''');';
                
                BEGIN TRY
                    EXEC sp_executesql @SQL;
                    PRINT 'Added partition boundary for LineItems: ' + CAST(@CurrentBoundary AS NVARCHAR(10));
                END TRY
                BEGIN CATCH
                    PRINT 'Failed to add partition boundary for LineItems: ' + ERROR_MESSAGE();
                END CATCH
            END
            
            SET @CurrentBoundary = DATEADD(MONTH, 1, @CurrentBoundary);
        END
    END
    
    ELSE IF @Action = 'ANALYZE'
    BEGIN
        -- Show partition information
        SELECT * FROM dbo.vw_PartitionPerformance;
    END
    
    -- Note: DROP_OLD would require more complex logic to merge/drop old partitions
    -- This should be implemented carefully to avoid data loss
END;
GO

-- ===========================
-- PERMISSIONS AND SECURITY
-- ===========================

-- Create roles for different access levels
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'ClaimsProcessorApp' AND type = 'R')
    CREATE ROLE ClaimsProcessorApp;
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'ClaimsAnalyst' AND type = 'R')
    CREATE ROLE ClaimsAnalyst;
GO

IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'ClaimsAdmin' AND type = 'R')
    CREATE ROLE ClaimsAdmin;
GO

-- Grant permissions to ClaimsProcessorApp (read/write access to main tables)
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Claims TO ClaimsProcessorApp;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Cms1500Diagnoses TO ClaimsProcessorApp;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Cms1500LineItems TO ClaimsProcessorApp;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.FailedClaimDetails TO ClaimsProcessorApp;
GRANT SELECT ON dbo.Facilities TO ClaimsProcessorApp;
GRANT SELECT ON dbo.ClinicalDepartments TO ClaimsProcessorApp;
GRANT SELECT ON dbo.FinancialClasses TO ClaimsProcessorApp;
GRANT EXECUTE ON dbo.sp_ValidateFacilityClaimAssignment TO ClaimsProcessorApp;
GRANT EXECUTE ON dbo.sp_BulkAssignClaimsToFacility TO ClaimsProcessorApp;
GO

-- Grant permissions to ClaimsAnalyst (read-only access with views)
GRANT SELECT ON dbo.vw_ProcessingPerformance TO ClaimsAnalyst;
GRANT SELECT ON dbo.vw_SystemHealth TO ClaimsAnalyst;
GRANT SELECT ON dbo.vw_FacilityPerformance TO ClaimsAnalyst;
GRANT SELECT ON dbo.vw_FailedClaimsAnalytics TO ClaimsAnalyst;
GRANT SELECT ON dbo.vw_RealTimeDashboard TO ClaimsAnalyst;
GRANT SELECT ON dbo.vw_CompressionStats TO ClaimsAnalyst;
GRANT SELECT ON dbo.Claims TO ClaimsAnalyst;
GRANT SELECT ON dbo.FailedClaimDetails TO ClaimsAnalyst;
GO

-- Grant permissions to ClaimsAdmin (full access including maintenance)
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo TO ClaimsAdmin;
GRANT EXECUTE ON SCHEMA::dbo TO ClaimsAdmin;
GO

-- ===========================
-- COMMENTS FOR DOCUMENTATION
-- ===========================
EXEC sp_addextendedproperty 'MS_Description', 'Enhanced production database with columnstore indexes, partitioning, and compression for high-performance claims processing', 'SCHEMA', 'dbo';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Main organizations table for healthcare systems', 'SCHEMA', 'dbo', 'TABLE', 'Organizations';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Geographic or administrative regions', 'SCHEMA', 'dbo', 'TABLE', 'Regions';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Healthcare facilities with comprehensive facility information', 'SCHEMA', 'dbo', 'TABLE', 'Facilities';
GO
EXEC sp_addextendedproperty 'MS_Description', 'User-defined facility identifier', 'SCHEMA', 'dbo', 'TABLE', 'Facilities', 'COLUMN', 'FacilityId';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Month (1-12) when fiscal year ends for this facility', 'SCHEMA', 'dbo', 'TABLE', 'Facilities', 'COLUMN', 'FiscalReportingMonth';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Critical Access Hospital designation (Y/N)', 'SCHEMA', 'dbo', 'TABLE', 'Facilities', 'COLUMN', 'CriticalAccess';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Partitioned claims table with page compression for optimal performance', 'SCHEMA', 'dbo', 'TABLE', 'Claims';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Required for partitioning - links claim to service date partition', 'SCHEMA', 'dbo', 'TABLE', 'Claims', 'COLUMN', 'ServiceDate';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Partitioned line items table with page compression', 'SCHEMA', 'dbo', 'TABLE', 'Cms1500LineItems';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Failed claims tracking for UI with optimized analytics support', 'SCHEMA', 'dbo', 'TABLE', 'FailedClaimDetails';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Columnstore index for high-performance analytics on claims data', 'SCHEMA', 'dbo', 'INDEX', 'IX_Claims_Analytics_Columnstore';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Real-time performance monitoring view with key metrics', 'SCHEMA', 'dbo', 'VIEW', 'vw_ProcessingPerformance';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Automated maintenance procedure for optimal system performance', 'SCHEMA', 'dbo', 'PROCEDURE', 'sp_AutomatedMaintenance';
GO

PRINT 'Enhanced SQL Server Production Database Schema created successfully with:';
PRINT '- Columnstore indexes for analytics performance';
PRINT '- Table partitioning by service date';
PRINT '- Row/Page compression on all tables';
PRINT '- Performance monitoring views';
PRINT '- Automated maintenance procedures';
PRINT '- Real-time dashboard views';
PRINT '- System alerts and health monitoring';
GO

-- End of Enhanced SQL Server Production Database Schema