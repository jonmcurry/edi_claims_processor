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
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_ClaimsByServiceDate')
CREATE PARTITION FUNCTION pf_ClaimsByServiceDate (DATE)
AS RANGE RIGHT FOR VALUES 
(
    '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01',
    '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
    '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01',
    '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
    '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01', '2025-05-01', '2025-06-01',
    '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01',
    '2026-01-01', '2026-02-01', '2026-03-01', '2026-04-01', '2026-05-01', '2026-06-01', 
    '2026-07-01', '2026-08-01', '2026-09-01', '2026-10-01', '2026-11-01', '2026-12-01' 
);
GO

-- Partition scheme for Claims
IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'ps_ClaimsByServiceDate')
CREATE PARTITION SCHEME ps_ClaimsByServiceDate
AS PARTITION pf_ClaimsByServiceDate
ALL TO ([PRIMARY]);
GO

-- Partition function for Line Items by ServiceDateFrom (monthly partitions)
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_LineItemsByServiceDate')
CREATE PARTITION FUNCTION pf_LineItemsByServiceDate (DATE)
AS RANGE RIGHT FOR VALUES 
(
    '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01',
    '2023-07-01', '2023-08-01', '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
    '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01',
    '2024-07-01', '2024-08-01', '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
    '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01', '2025-05-01', '2025-06-01',
    '2025-07-01', '2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01',
    '2026-01-01', '2026-02-01', '2026-03-01', '2026-04-01', '2026-05-01', '2026-06-01',
    '2026-07-01', '2026-08-01', '2026-09-01', '2026-10-01', '2026-11-01', '2026-12-01' 
);
GO

-- Partition scheme for Line Items
IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'ps_LineItemsByServiceDate')
CREATE PARTITION SCHEME ps_LineItemsByServiceDate
AS PARTITION pf_LineItemsByServiceDate
ALL TO ([PRIMARY]);
GO

-- ===========================
-- CORE TABLES WITH COMPRESSION
-- ===========================

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

CREATE TABLE dbo.Facilities (
    FacilityId NVARCHAR(50) PRIMARY KEY,
    FacilityName NVARCHAR(200) NOT NULL,
    FacilityCode NVARCHAR(20) UNIQUE, 
    OrganizationId INT REFERENCES dbo.Organizations(OrganizationId),
    RegionId INT REFERENCES dbo.Regions(RegionId),
    City NVARCHAR(100),
    StateCode CHAR(2),
    ZipCode NVARCHAR(10),
    AddressLine1 NVARCHAR(255),
    AddressLine2 NVARCHAR(255),
    BedSize INT CHECK (BedSize >= 0),
    FiscalReportingMonth INT CHECK (FiscalReportingMonth BETWEEN 1 AND 12),
    EmrSystem NVARCHAR(100), 
    FacilityType NVARCHAR(20) CHECK (FacilityType IN ('Teaching', 'Non-Teaching')),
    CriticalAccess CHAR(1) CHECK (CriticalAccess IN ('Y', 'N')),
    Phone NVARCHAR(20),
    Fax NVARCHAR(20),
    Email NVARCHAR(255),
    NpiNumber NVARCHAR(10), 
    TaxId NVARCHAR(20),
    LicenseNumber NVARCHAR(50),
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

CREATE TABLE dbo.StandardPayers (
    StandardPayerId INT IDENTITY(1,1) PRIMARY KEY,
    StandardPayerCode NVARCHAR(20) UNIQUE NOT NULL,
    StandardPayerName NVARCHAR(100) NOT NULL,
    PayerCategory NVARCHAR(50), 
    Description NVARCHAR(MAX),
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

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

CREATE TABLE dbo.FinancialClasses (
    FinancialClassId NVARCHAR(20) PRIMARY KEY, 
    FinancialClassDescription NVARCHAR(200) NOT NULL,
    StandardPayerId INT REFERENCES dbo.StandardPayers(StandardPayerId),
    HccTypeId INT REFERENCES dbo.HccTypes(HccTypeId),
    FacilityId NVARCHAR(50) REFERENCES dbo.Facilities(FacilityId),
    PayerPriority INT DEFAULT 1, 
    RequiresAuthorization BIT DEFAULT 0,
    AuthorizationRequiredServices NVARCHAR(MAX), 
    CopayAmount DECIMAL(8,2),
    DeductibleAmount DECIMAL(10,2),
    ClaimSubmissionFormat NVARCHAR(20), 
    DaysToFileClaim INT DEFAULT 365,
    AcceptsElectronicClaims BIT DEFAULT 1,
    Active BIT DEFAULT 1,
    EffectiveDate DATE DEFAULT CAST(GETDATE() AS DATE),
    EndDate DATE,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DATA_COMPRESSION = ROW);
GO

CREATE TABLE dbo.ClinicalDepartments (
    DepartmentId INT IDENTITY(1,1) PRIMARY KEY,
    ClinicalDepartmentCode NVARCHAR(20) NOT NULL,
    DepartmentDescription NVARCHAR(200) NOT NULL,
    FacilityId NVARCHAR(50) REFERENCES dbo.Facilities(FacilityId),
    DepartmentType NVARCHAR(50), 
    RevenueCode NVARCHAR(10), 
    CostCenter NVARCHAR(20),
    SpecialtyCode NVARCHAR(10),
    IsSurgical BIT DEFAULT 0,
    IsEmergency BIT DEFAULT 0,
    IsCriticalCare BIT DEFAULT 0,
    RequiresPriorAuth BIT DEFAULT 0,
    DefaultPlaceOfService NVARCHAR(2), 
    BillingProviderNpi NVARCHAR(10),
    DepartmentManager NVARCHAR(100),
    PhoneExtension NVARCHAR(10),
    Active BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE(),
    UNIQUE(ClinicalDepartmentCode, FacilityId)
) WITH (DATA_COMPRESSION = ROW);
GO

CREATE TABLE dbo.Claims (
    ClaimId NVARCHAR(50) NOT NULL,
    FacilityId NVARCHAR(50) REFERENCES dbo.Facilities(FacilityId), 
    DepartmentId INT REFERENCES dbo.ClinicalDepartments(DepartmentId), 
    FinancialClassId NVARCHAR(20) REFERENCES dbo.FinancialClasses(FinancialClassId), 
    PatientId NVARCHAR(50),
    PatientAge INT,
    PatientDob DATE,
    PatientSex CHAR(1),
    PatientAccountNumber NVARCHAR(50),
    ProviderId NVARCHAR(50),
    ProviderType NVARCHAR(100),
    RenderingProviderNpi NVARCHAR(10),
    PlaceOfService NVARCHAR(10),
    ServiceDate DATE NOT NULL, 
    TotalChargeAmount DECIMAL(12,2),
    TotalClaimCharges DECIMAL(10,2),
    PayerName NVARCHAR(100),
    PrimaryInsuranceId NVARCHAR(50),
    SecondaryInsuranceId NVARCHAR(50),
    ProcessingStatus NVARCHAR(20) DEFAULT 'PENDING',
    ProcessedDate DATETIME2,
    ClaimData NVARCHAR(MAX), 
    ValidationStatus NVARCHAR(20),
    ValidationResults NVARCHAR(MAX), 
    ValidationDate DATETIME2,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE(),
    CreatedBy NVARCHAR(100),
    UpdatedBy NVARCHAR(100),
    PRIMARY KEY (ClaimId, ServiceDate) 
) ON ps_ClaimsByServiceDate(ServiceDate) WITH (DATA_COMPRESSION = PAGE);
GO

CREATE TABLE dbo.Cms1500Diagnoses (
    DiagnosisEntryId BIGINT IDENTITY(1,1) PRIMARY KEY,
    ClaimId NVARCHAR(50) NOT NULL,
    ClaimServiceDate DATE NOT NULL, 
    DiagnosisSequence INT NOT NULL CHECK (DiagnosisSequence BETWEEN 1 AND 12),
    IcdCode NVARCHAR(10) NOT NULL,
    IcdCodeType NVARCHAR(10) DEFAULT 'ICD10',
    DiagnosisDescription NVARCHAR(MAX),
    IsPrimary BIT DEFAULT 0,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_Cms1500Diagnoses_Claims FOREIGN KEY (ClaimId, ClaimServiceDate)
        REFERENCES dbo.Claims(ClaimId, ServiceDate) ON DELETE CASCADE,
    UNIQUE(ClaimId, ClaimServiceDate, DiagnosisSequence)
) WITH (DATA_COMPRESSION = PAGE);
GO

CREATE TABLE dbo.Cms1500LineItems (
    LineItemId BIGINT IDENTITY(1,1) NOT NULL, 
    ClaimId NVARCHAR(50) NOT NULL,
    ClaimServiceDate DATE NOT NULL, 
    LineNumber INT NOT NULL CHECK (LineNumber BETWEEN 1 AND 99),
    ServiceDateFrom DATE NOT NULL, 
    ServiceDateTo DATE,
    PlaceOfServiceCode NVARCHAR(2),
    EmergencyIndicator CHAR(1),
    CptCode NVARCHAR(10) NOT NULL,
    Modifier1 NVARCHAR(2),
    Modifier2 NVARCHAR(2),
    Modifier3 NVARCHAR(2),
    Modifier4 NVARCHAR(2),
    DiagnosisPointers NVARCHAR(10),
    LineChargeAmount DECIMAL(10,2) NOT NULL,
    Units INT DEFAULT 1,
    EpsdtFamilyPlan CHAR(1),
    RenderingProviderIdQualifier NVARCHAR(2),
    RenderingProviderId NVARCHAR(20),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    UpdatedDate DATETIME2 DEFAULT GETDATE(),
    PRIMARY KEY (LineItemId, ServiceDateFrom),
    CONSTRAINT FK_Cms1500LineItems_Claims FOREIGN KEY (ClaimId, ClaimServiceDate)
        REFERENCES dbo.Claims(ClaimId, ServiceDate) ON DELETE CASCADE,
    UNIQUE(ClaimId, ClaimServiceDate, LineNumber, ServiceDateFrom)
) ON ps_LineItemsByServiceDate(ServiceDateFrom) WITH (DATA_COMPRESSION = PAGE);
GO

CREATE TABLE dbo.FailedClaimDetails (
    FailedClaimDetailId BIGINT IDENTITY(1,1) PRIMARY KEY,
    OriginalClaimId NVARCHAR(50) NOT NULL, 
    OriginalClaimServiceDate DATE, 
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

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Claims_Analytics_Columnstore' AND object_id = OBJECT_ID('dbo.Claims'))
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_Claims_Analytics_Columnstore
ON dbo.Claims ( FacilityId, DepartmentId, FinancialClassId, ServiceDate, TotalChargeAmount, TotalClaimCharges, ProcessingStatus, ValidationStatus, PatientAge, PatientSex, CreatedDate );
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_LineItems_Analytics_Columnstore' AND object_id = OBJECT_ID('dbo.Cms1500LineItems'))
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_LineItems_Analytics_Columnstore
ON dbo.Cms1500LineItems ( ClaimId, ClaimServiceDate, ServiceDateFrom, ServiceDateTo, CptCode, LineChargeAmount, Units, PlaceOfServiceCode, CreatedDate );
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Diagnoses_Analytics_Columnstore' AND object_id = OBJECT_ID('dbo.Cms1500Diagnoses'))
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_Diagnoses_Analytics_Columnstore
ON dbo.Cms1500Diagnoses ( ClaimId, ClaimServiceDate, IcdCode, IcdCodeType, DiagnosisSequence, IsPrimary, CreatedDate );
GO
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FailedClaims_Analytics_Columnstore' AND object_id = OBJECT_ID('dbo.FailedClaimDetails'))
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_FailedClaims_Analytics_Columnstore
ON dbo.FailedClaimDetails ( FacilityId, ProcessingStage, Status, FailureTimestamp, ServiceDate, CreatedDate, OriginalClaimServiceDate );
GO

-- ===========================
-- REGULAR INDEXES FOR PERFORMANCE (with compression)
-- ===========================

CREATE INDEX IX_Organizations_Active ON dbo.Organizations(Active) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Organizations_Code_Includes ON dbo.Organizations(OrganizationCode) INCLUDE (OrganizationName, Active) WITH (DATA_COMPRESSION = ROW);
GO
CREATE INDEX IX_Regions_Active ON dbo.Regions(Active) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Regions_Code_Includes ON dbo.Regions(RegionCode) INCLUDE (RegionName, Active) WITH (DATA_COMPRESSION = ROW);
GO
CREATE INDEX IX_OrganizationRegions_Org ON dbo.OrganizationRegions(OrganizationId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_OrganizationRegions_Region ON dbo.OrganizationRegions(RegionId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_OrganizationRegions_Primary ON dbo.OrganizationRegions(IsPrimary) WHERE IsPrimary = 1 WITH (DATA_COMPRESSION = ROW);
GO
CREATE INDEX IX_Facilities_Org ON dbo.Facilities(OrganizationId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Facilities_Region ON dbo.Facilities(RegionId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Facilities_Active_Type ON dbo.Facilities(Active, FacilityType) INCLUDE (FacilityName, City, StateCode) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_Facilities_State ON dbo.Facilities(StateCode) WITH (DATA_COMPRESSION = ROW);
GO
CREATE INDEX IX_StandardPayers_Active_Category ON dbo.StandardPayers(Active, PayerCategory) INCLUDE (StandardPayerName) WITH (DATA_COMPRESSION = ROW);
GO
CREATE INDEX IX_FinancialClasses_Facility_Active ON dbo.FinancialClasses(FacilityId, Active) INCLUDE (FinancialClassDescription, StandardPayerId) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_FinancialClasses_Payer ON dbo.FinancialClasses(StandardPayerId) WITH (DATA_COMPRESSION = ROW);
GO
CREATE INDEX IX_ClinicalDepartments_Facility_Active ON dbo.ClinicalDepartments(FacilityId, Active) INCLUDE (DepartmentDescription, DepartmentType) WITH (DATA_COMPRESSION = ROW);
CREATE INDEX IX_ClinicalDepartments_Code ON dbo.ClinicalDepartments(ClinicalDepartmentCode) WITH (DATA_COMPRESSION = ROW);
GO

CREATE INDEX IX_Claims_Facility ON dbo.Claims(FacilityId) 
    WITH (DATA_COMPRESSION = PAGE) ON ps_ClaimsByServiceDate(ServiceDate);
CREATE INDEX IX_Claims_Department ON dbo.Claims(DepartmentId) 
    WITH (DATA_COMPRESSION = PAGE) ON ps_ClaimsByServiceDate(ServiceDate);
CREATE INDEX IX_Claims_FinancialClass ON dbo.Claims(FinancialClassId) 
    WITH (DATA_COMPRESSION = PAGE) ON ps_ClaimsByServiceDate(ServiceDate);
CREATE INDEX IX_Claims_Status_Facility ON dbo.Claims(ProcessingStatus, FacilityId) 
    WITH (DATA_COMPRESSION = PAGE) ON ps_ClaimsByServiceDate(ServiceDate);
CREATE INDEX IX_Claims_ValidationStatus ON dbo.Claims(ValidationStatus) 
    WITH (DATA_COMPRESSION = PAGE) ON ps_ClaimsByServiceDate(ServiceDate);
CREATE INDEX IX_Claims_CreatedDate ON dbo.Claims(CreatedDate) 
    WITH (DATA_COMPRESSION = PAGE) ON ps_ClaimsByServiceDate(ServiceDate);
GO

CREATE INDEX IX_Cms1500Diagnoses_Claim ON dbo.Cms1500Diagnoses(ClaimId, ClaimServiceDate) 
    INCLUDE (IcdCode, DiagnosisSequence, IsPrimary) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_Cms1500Diagnoses_IcdCode ON dbo.Cms1500Diagnoses(IcdCode) 
    INCLUDE (DiagnosisDescription) WITH (DATA_COMPRESSION = PAGE);
GO

CREATE INDEX IX_Cms1500LineItems_Claim ON dbo.Cms1500LineItems(ClaimId, ClaimServiceDate) 
    INCLUDE (CptCode, LineChargeAmount) WITH (DATA_COMPRESSION = PAGE) ON ps_LineItemsByServiceDate(ServiceDateFrom);
CREATE INDEX IX_Cms1500LineItems_CptCode ON dbo.Cms1500LineItems(CptCode) 
    INCLUDE (LineChargeAmount) WITH (DATA_COMPRESSION = PAGE) ON ps_LineItemsByServiceDate(ServiceDateFrom);
GO

CREATE INDEX IX_FailedClaimDetails_OriginalClaim ON dbo.FailedClaimDetails(OriginalClaimId, OriginalClaimServiceDate) 
    INCLUDE (Status, ProcessingStage) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_FailedClaimDetails_Status_Stage ON dbo.FailedClaimDetails(Status, ProcessingStage) 
    INCLUDE (FailureTimestamp) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_FailedClaimDetails_FailureTimestamp ON dbo.FailedClaimDetails(FailureTimestamp) 
    WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_FailedClaimDetails_FacilityId_Status ON dbo.FailedClaimDetails(FacilityId, Status) 
    WITH (DATA_COMPRESSION = PAGE);
GO

-- ===========================
-- PERFORMANCE MONITORING VIEWS
-- ===========================

CREATE OR ALTER VIEW dbo.vw_ProcessingPerformance AS
SELECT
    COUNT_BIG(*) as TotalClaimsInSystem,
    SUM(CASE WHEN c.ProcessingStatus = 'PENDING' THEN 1 ELSE 0 END) as PendingClaims,
    SUM(CASE WHEN c.ProcessingStatus = 'PROCESSING' THEN 1 ELSE 0 END) as ProcessingClaims,
    SUM(CASE WHEN c.ProcessingStatus = 'COMPLETED' THEN 1 ELSE 0 END) as CompletedClaims,
    SUM(CASE WHEN c.ProcessingStatus = 'ERROR' THEN 1 ELSE 0 END) as ErrorClaims,
    AVG(CAST(DATEDIFF(MINUTE, c.CreatedDate, c.ProcessedDate) as DECIMAL(10,2))) as AvgProcessingTimeMinutes,
    SUM(CASE WHEN c.CreatedDate >= DATEADD(HOUR, -1, GETDATE()) THEN 1 ELSE 0 END) as ClaimsLastHour,
    SUM(CASE WHEN c.CreatedDate >= DATEADD(DAY, -1, GETDATE()) THEN 1 ELSE 0 END) as ClaimsLast24Hours,
    (SELECT COUNT_BIG(*) FROM dbo.Claims c_hr WHERE c_hr.ProcessedDate >= DATEADD(HOUR, -1, GETDATE())) AS ProcessedClaimsLastHour,
    SUM(c.TotalChargeAmount) as TotalChargesInSystem,
    AVG(c.TotalChargeAmount) as AvgClaimAmount,
    SUM(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 ELSE 0 END) as ValidatedClaims,
    CAST(SUM(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT_BIG(*),0) AS DECIMAL(5,2)) as ValidationSuccessRate
FROM dbo.Claims c
WHERE c.CreatedDate >= DATEADD(DAY, -30, GETDATE());
GO

-- System health and capacity view
CREATE OR ALTER VIEW dbo.vw_SystemHealth AS
SELECT
    TableName,
    ActualRowCount,
    EstimatedSizeMB,
    LastTableInsert,
    OldestTableRecord,
    RecordsInLast24Hours
FROM (
    SELECT
        'Claims' as TableName, 
        SUM(s.row_count) as ActualRowCount, 
        CAST(SUM(s.used_page_count) * 8.0 / 1024 AS DECIMAL(10,2)) as EstimatedSizeMB, 
        (SELECT MAX(c.CreatedDate) FROM dbo.Claims c) as LastTableInsert, 
        (SELECT MIN(c.CreatedDate) FROM dbo.Claims c) as OldestTableRecord, 
        (SELECT COUNT_BIG(*) FROM dbo.Claims c WHERE c.CreatedDate >= DATEADD(DAY, -1, GETDATE())) as RecordsInLast24Hours
    FROM sys.dm_db_partition_stats s
    WHERE s.object_id = OBJECT_ID('dbo.Claims') AND s.index_id IN (0,1)
    GROUP BY s.object_id 

    UNION ALL

    SELECT
        'Cms1500LineItems' as TableName, 
        SUM(s.row_count) as ActualRowCount,
        CAST(SUM(s.used_page_count) * 8.0 / 1024 AS DECIMAL(10,2)) as EstimatedSizeMB,
        (SELECT MAX(li.CreatedDate) FROM dbo.Cms1500LineItems li) as LastTableInsert,
        (SELECT MIN(li.CreatedDate) FROM dbo.Cms1500LineItems li) as OldestTableRecord,
        (SELECT COUNT_BIG(*) FROM dbo.Cms1500LineItems li WHERE li.CreatedDate >= DATEADD(DAY, -1, GETDATE())) as RecordsInLast24Hours
    FROM sys.dm_db_partition_stats s
    WHERE s.object_id = OBJECT_ID('dbo.Cms1500LineItems') AND s.index_id IN (0,1)
    GROUP BY s.object_id

    UNION ALL

    SELECT
        'Cms1500Diagnoses' as TableName, 
        SUM(s.row_count) as ActualRowCount, 
        CAST(SUM(s.used_page_count) * 8.0 / 1024 AS DECIMAL(10,2)) as EstimatedSizeMB,
        (SELECT MAX(cd.CreatedDate) FROM dbo.Cms1500Diagnoses cd) as LastTableInsert,
        (SELECT MIN(cd.CreatedDate) FROM dbo.Cms1500Diagnoses cd) as OldestTableRecord,
        (SELECT COUNT_BIG(*) FROM dbo.Cms1500Diagnoses cd WHERE cd.CreatedDate >= DATEADD(DAY, -1, GETDATE())) as RecordsInLast24Hours
    FROM sys.dm_db_partition_stats s
    WHERE s.object_id = OBJECT_ID('dbo.Cms1500Diagnoses') AND s.index_id IN (0,1)
    GROUP BY s.object_id

    UNION ALL

    SELECT
        'FailedClaimDetails' as TableName, 
        SUM(s.row_count) as ActualRowCount, 
        CAST(SUM(s.used_page_count) * 8.0 / 1024 AS DECIMAL(10,2)) as EstimatedSizeMB,
        (SELECT MAX(fcd.CreatedDate) FROM dbo.FailedClaimDetails fcd) as LastTableInsert,
        (SELECT MIN(fcd.CreatedDate) FROM dbo.FailedClaimDetails fcd) as OldestTableRecord,
        (SELECT COUNT_BIG(*) FROM dbo.FailedClaimDetails fcd WHERE fcd.CreatedDate >= DATEADD(DAY, -1, GETDATE())) as RecordsInLast24Hours
    FROM sys.dm_db_partition_stats s
    WHERE s.object_id = OBJECT_ID('dbo.FailedClaimDetails') AND s.index_id IN (0,1)
    GROUP BY s.object_id
) AllTables;
GO

-- Partition performance view
CREATE OR ALTER VIEW dbo.vw_PartitionPerformance AS
WITH AllocationUnitsCTE AS (
    SELECT 
        container_id, 
        SUM(total_pages) AS total_pages, 
        SUM(used_pages) AS used_pages
    FROM sys.allocation_units
    GROUP BY container_id
)
SELECT
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name as TableName,
    p.partition_number,
    p.rows AS PartitionRows, 
    CAST(p.rows * 8.0 / 1024 AS DECIMAL(10,2)) as EstimatedSizeMB_Approx, 
    CAST(au.total_pages * 8.0 / 1024 AS DECIMAL(10,2)) AS TotalSpaceMB,
    CAST(au.used_pages * 8.0 / 1024 AS DECIMAL(10,2)) AS UsedSpaceMB,
    CASE 
        WHEN pf.boundary_value_on_right = 0 THEN 'Range LEFT (Boundary Value belongs to Left Partition)'
        ELSE 'Range RIGHT (Boundary Value belongs to Right Partition)'
    END AS PartitionType,
    LAG(rv.value, 1, NULL) OVER (PARTITION BY pf.function_id ORDER BY rv.boundary_id) AS LowerBoundary,
    rv.value AS UpperBoundaryValue,
    fg.name as FileGroupName,
    ps.name as PartitionSchemeName 
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id 
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id AND p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
LEFT JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id AND rv.boundary_id = p.partition_number - pf.boundary_value_on_right
LEFT JOIN AllocationUnitsCTE au ON p.partition_id = au.container_id OR p.hobt_id = au.container_id 
WHERE t.name IN ('Claims', 'Cms1500LineItems')
    AND i.index_id IN (0,1);
GO

CREATE OR ALTER VIEW dbo.vw_FailedClaimsAnalytics AS
SELECT
    ProcessingStage,
    Status,
    COUNT_BIG(*) as FailureCount,
    SUM(CASE WHEN FailureTimestamp >= DATEADD(DAY, -1, GETDATE()) THEN 1 ELSE 0 END) as FailuresLast24Hours,
    SUM(CASE WHEN FailureTimestamp >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END) as FailuresLast7Days,
    AVG(CASE WHEN ResolvedTimestamp IS NOT NULL 
        THEN CAST(DATEDIFF(HOUR, FailureTimestamp, ResolvedTimestamp) as DECIMAL(10,2)) END) as AvgResolutionTimeHours,
    SUM(CASE WHEN Status = 'Resolved' THEN 1 ELSE 0 END) as ResolvedCount,
    CAST(SUM(CASE WHEN Status = 'Resolved' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT_BIG(*),0) AS DECIMAL(5,2)) as ResolutionRate
FROM dbo.FailedClaimDetails
WHERE FailureTimestamp >= DATEADD(DAY, -30, GETDATE()) 
GROUP BY ProcessingStage, Status;
GO

CREATE OR ALTER VIEW dbo.vw_FacilityPerformance AS
SELECT
    f.FacilityId,
    f.FacilityName,
    f.FacilityType,
    f.CriticalAccess,
    COUNT(c.ClaimId) as TotalClaims,
    SUM(CASE WHEN c.CreatedDate >= DATEADD(DAY, -1, GETDATE()) THEN 1 ELSE 0 END) as ClaimsLast24Hours,
    SUM(CASE WHEN c.CreatedDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END) as ClaimsLast7Days,
    AVG(CASE WHEN c.ProcessedDate IS NOT NULL THEN CAST(DATEDIFF(MINUTE, c.CreatedDate, c.ProcessedDate) AS DECIMAL(10,2)) END) as AvgProcessingTimeMinutes,
    SUM(CASE WHEN c.ProcessingStatus = 'COMPLETED' THEN 1 ELSE 0 END) as CompletedClaims,
    SUM(CASE WHEN c.ProcessingStatus = 'ERROR' THEN 1 ELSE 0 END) as ErrorClaims,
    SUM(c.TotalChargeAmount) as TotalCharges,
    AVG(c.TotalChargeAmount) as AvgChargeAmount,
    SUM(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 ELSE 0 END) as ValidatedClaims,
    CAST(SUM(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(c.ClaimId), 0) AS DECIMAL(5,2)) as ValidationSuccessRate,
    (SELECT COUNT_BIG(*) FROM dbo.FailedClaimDetails fcd WHERE fcd.FacilityId = f.FacilityId AND fcd.FailureTimestamp >= DATEADD(DAY, -30, GETDATE())) as FailedClaimsCountLast30Days
FROM dbo.Facilities f
LEFT JOIN dbo.Claims c ON f.FacilityId = c.FacilityId 
WHERE f.Active = 1
GROUP BY f.FacilityId, f.FacilityName, f.FacilityType, f.CriticalAccess;
GO

CREATE OR ALTER VIEW dbo.vw_IndexPerformance AS
SELECT
    OBJECT_SCHEMA_NAME(i.object_id) as SchemaName,
    OBJECT_NAME(i.object_id) as TableName,
    i.name as IndexName,
    i.type_desc as IndexType,
    ISNULL(ius.user_seeks, 0) as user_seeks,
    ISNULL(ius.user_scans, 0) as user_scans,
    ISNULL(ius.user_lookups, 0) as user_lookups,
    ISNULL(ius.user_updates, 0) as user_updates,
    ISNULL(ius.user_seeks, 0) + ISNULL(ius.user_scans, 0) + ISNULL(ius.user_lookups, 0) as TotalReads,
    CASE 
        WHEN ISNULL(ius.user_updates, 0) > 0 
        THEN CAST((ISNULL(ius.user_seeks, 0) + ISNULL(ius.user_scans, 0) + ISNULL(ius.user_lookups, 0)) AS FLOAT) / ius.user_updates
        ELSE NULL
    END as ReadWriteRatio,
    ps.avg_fragmentation_in_percent,
    ps.page_count
FROM sys.indexes i
LEFT JOIN sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = DB_ID()
LEFT JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') ps 
    ON i.object_id = ps.object_id AND i.index_id = ps.index_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) = 'dbo'
    AND i.type > 0 
    AND OBJECT_NAME(i.object_id) IN ('Claims', 'Cms1500LineItems', 'Cms1500Diagnoses', 'FailedClaimDetails', 'Facilities', 'Organizations', 'Regions', 'StandardPayers', 'FinancialClasses', 'ClinicalDepartments');
GO

-- ===========================
-- VIEWS FOR REPORTING (Enhanced)
-- ===========================

CREATE OR ALTER VIEW dbo.vw_FacilityDetails AS
SELECT
    f.FacilityId, f.FacilityName, f.FacilityCode, f.City, f.StateCode, f.BedSize,
    f.FiscalReportingMonth, f.EmrSystem, f.FacilityType, f.CriticalAccess, f.Active as FacilityActive,
    o.OrganizationId, o.OrganizationName, o.OrganizationCode,
    r.RegionId, r.RegionName, r.RegionCode,
    (SELECT COUNT_BIG(*) FROM dbo.FinancialClasses fc WHERE fc.FacilityId = f.FacilityId AND fc.Active = 1) as ActiveFinancialClasses,
    (SELECT COUNT_BIG(*) FROM dbo.ClinicalDepartments cd WHERE cd.FacilityId = f.FacilityId AND cd.Active = 1) as ActiveDepartments,
    f.CreatedDate, f.UpdatedDate
FROM dbo.Facilities f
LEFT JOIN dbo.Organizations o ON f.OrganizationId = o.OrganizationId
LEFT JOIN dbo.Regions r ON f.RegionId = r.RegionId;
GO

CREATE OR ALTER VIEW dbo.vw_ClaimsByFacility AS
SELECT
    f.FacilityId, f.FacilityName, o.OrganizationName, r.RegionName,
    COUNT_BIG(c.ClaimId) as TotalClaims,
    SUM(CASE WHEN c.ProcessingStatus = 'PENDING' THEN 1 ELSE 0 END) as PendingClaims,
    SUM(CASE WHEN c.ProcessingStatus = 'COMPLETED' THEN 1 ELSE 0 END) as CompletedClaims,
    SUM(CASE WHEN c.ProcessingStatus = 'ERROR' THEN 1 ELSE 0 END) as ErrorClaims,
    SUM(CASE WHEN c.ValidationStatus = 'VALIDATED' THEN 1 ELSE 0 END) as ValidatedClaims,
    ISNULL(SUM(c.TotalChargeAmount), 0) as TotalCharges,
    ISNULL(AVG(c.TotalChargeAmount), 0) as AvgChargeAmount,
    MIN(c.ServiceDate) as EarliestServiceDate, MAX(c.ServiceDate) as LatestServiceDate,
    COUNT_BIG(DISTINCT c.DepartmentId) as DepartmentsWithClaims,
    COUNT_BIG(DISTINCT c.FinancialClassId) as FinancialClassesWithClaims
FROM dbo.Facilities f
LEFT JOIN dbo.Organizations o ON f.OrganizationId = o.OrganizationId
LEFT JOIN dbo.Regions r ON f.RegionId = r.RegionId
LEFT JOIN dbo.Claims c ON f.FacilityId = c.FacilityId
GROUP BY f.FacilityId, f.FacilityName, o.OrganizationName, r.RegionName;
GO

CREATE OR ALTER VIEW dbo.vw_FinancialClassSummary AS
SELECT
    fc.FinancialClassId, fc.FinancialClassDescription, fc.FacilityId, f.FacilityName,
    sp.StandardPayerName, sp.StandardPayerCode, sp.PayerCategory,
    ht.HccCode, ht.HccName, ht.HccAgency,
    fc.Active as FinancialClassActive, fc.EffectiveDate, fc.EndDate,
    (SELECT COUNT_BIG(*) FROM dbo.Claims c WHERE c.FinancialClassId = fc.FinancialClassId AND c.FacilityId = fc.FacilityId) as TotalClaimsForThisFacilityLink
FROM dbo.FinancialClasses fc
LEFT JOIN dbo.Facilities f ON fc.FacilityId = f.FacilityId
LEFT JOIN dbo.StandardPayers sp ON fc.StandardPayerId = sp.StandardPayerId
LEFT JOIN dbo.HccTypes ht ON fc.HccTypeId = ht.HccTypeId;
GO

CREATE OR ALTER VIEW dbo.vw_DepartmentUtilization AS
SELECT
    cd.DepartmentId, cd.ClinicalDepartmentCode, cd.DepartmentDescription, cd.FacilityId, f.FacilityName,
    cd.DepartmentType, cd.IsSurgical, cd.IsEmergency, cd.IsCriticalCare,
    COUNT_BIG(c.ClaimId) as TotalClaims,
    ISNULL(SUM(c.TotalChargeAmount), 0) as TotalCharges,
    ISNULL(AVG(c.TotalChargeAmount), 0) as AvgChargePerClaim,
    SUM(CASE WHEN c.ServiceDate >= DATEADD(day, -30, GETDATE()) THEN 1 ELSE 0 END) as ClaimsLast30Days,
    SUM(CASE WHEN c.ServiceDate >= DATEADD(day, -90, GETDATE()) THEN 1 ELSE 0 END) as ClaimsLast90Days,
    MIN(c.ServiceDate) as FirstClaimDate, MAX(c.ServiceDate) as LastClaimDate
FROM dbo.ClinicalDepartments cd
LEFT JOIN dbo.Facilities f ON cd.FacilityId = f.FacilityId
LEFT JOIN dbo.Claims c ON cd.DepartmentId = c.DepartmentId AND c.FacilityId = cd.FacilityId 
WHERE cd.Active = 1
GROUP BY cd.DepartmentId, cd.ClinicalDepartmentCode, cd.DepartmentDescription,
         cd.FacilityId, f.FacilityName, cd.DepartmentType, cd.IsSurgical,
         cd.IsEmergency, cd.IsCriticalCare;
GO

-- ===========================
-- STORED PROCEDURES (Enhanced)
-- ===========================

IF TYPE_ID(N'dbo.ClaimIdServiceDateList') IS NULL
CREATE TYPE dbo.ClaimIdServiceDateList AS TABLE (
    ClaimId NVARCHAR(50) NOT NULL,
    ServiceDate DATE NOT NULL,
    PRIMARY KEY (ClaimId, ServiceDate)
);
GO

CREATE OR ALTER PROCEDURE dbo.sp_GetFacilityHierarchy
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

CREATE OR ALTER PROCEDURE dbo.sp_ValidateFacilityClaimAssignment
    @ClaimId NVARCHAR(50),
    @ClaimServiceDate DATE 
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @IsValid BIT = 1;
    DECLARE @Errors NVARCHAR(MAX) = '';
    DECLARE @Warnings NVARCHAR(MAX) = '';

    IF OBJECT_ID('tempdb..#ClaimValidation') IS NOT NULL DROP TABLE #ClaimValidation;

    SELECT
        c.ClaimId, c.FacilityId, c.DepartmentId, c.FinancialClassId, c.ServiceDate,
        f.FacilityName, f.Active as FacilityActive,
        d.DepartmentDescription, d.Active as DepartmentActive, d.FacilityId as DeptFacilityId,
        fc.FinancialClassDescription, fc.Active as FcActive, fc.FacilityId as FcFacilityId
    INTO #ClaimValidation
    FROM dbo.Claims c
    LEFT JOIN dbo.Facilities f ON c.FacilityId = f.FacilityId
    LEFT JOIN dbo.ClinicalDepartments d ON c.DepartmentId = d.DepartmentId
    LEFT JOIN dbo.FinancialClasses fc ON c.FinancialClassId = fc.FinancialClassId
    WHERE c.ClaimId = @ClaimId AND c.ServiceDate = @ClaimServiceDate;

    IF NOT EXISTS (SELECT 1 FROM #ClaimValidation)
    BEGIN SET @IsValid = 0; SET @Errors = 'Claim not found in dbo.Claims or related tables for the given ClaimId and ServiceDate.'; END
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

    SELECT @IsValid as IsValid, 
           NULLIF(RTRIM(LTRIM(REPLACE(@Errors, '; ', ';'))),'') as Errors, 
           NULLIF(RTRIM(LTRIM(REPLACE(@Warnings, '; ', ';'))),'') as Warnings,
           (SELECT TOP 1 FacilityId, FacilityName, DepartmentDescription, FinancialClassDescription FROM #ClaimValidation FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) as FacilityInfo;

    IF OBJECT_ID('tempdb..#ClaimValidation') IS NOT NULL DROP TABLE #ClaimValidation;
END;
GO

CREATE OR ALTER PROCEDURE dbo.sp_BulkAssignClaimsToFacility
    @ClaimServiceDatePairs dbo.ClaimIdServiceDateList READONLY, 
    @FacilityId NVARCHAR(50),
    @DepartmentId INT = NULL,
    @FinancialClassId NVARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @UpdateCount INT = 0;
    DECLARE @ErrorMessage NVARCHAR(MAX) = '';

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM dbo.Facilities WHERE FacilityId = @FacilityId AND Active = 1)
        BEGIN SET @ErrorMessage = 'Facility ' + @FacilityId + ' does not exist or is inactive.'; SELECT 0 as Success, @ErrorMessage as Message, 0 as UpdatedCount; RETURN; END

        IF @DepartmentId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.ClinicalDepartments WHERE DepartmentId = @DepartmentId AND FacilityId = @FacilityId AND Active = 1)
        BEGIN SET @ErrorMessage = 'Department ' + CAST(@DepartmentId AS NVARCHAR(10)) + ' does not belong to facility ' + @FacilityId + ' or is inactive.'; SELECT 0 as Success, @ErrorMessage as Message, 0 as UpdatedCount; RETURN; END

        IF @FinancialClassId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dbo.FinancialClasses WHERE FinancialClassId = @FinancialClassId AND (FacilityId = @FacilityId OR FacilityId IS NULL) AND Active = 1)
        BEGIN SET @ErrorMessage = 'Financial class ' + @FinancialClassId + ' is not valid for facility ' + @FacilityId + ' or is inactive.'; SELECT 0 as Success, @ErrorMessage as Message, 0 as UpdatedCount; RETURN; END

        UPDATE c SET FacilityId = @FacilityId, DepartmentId = @DepartmentId, FinancialClassId = @FinancialClassId, UpdatedDate = GETDATE(), UpdatedBy = SUSER_SNAME()
        FROM dbo.Claims c INNER JOIN @ClaimServiceDatePairs ctu ON c.ClaimId = ctu.ClaimId AND c.ServiceDate = ctu.ServiceDate;
        SET @UpdateCount = @@ROWCOUNT;

        SELECT 1 as Success, CAST(@UpdateCount AS NVARCHAR(10)) + ' claims updated successfully.' as Message, @UpdateCount as UpdatedCount;
    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        SELECT 0 as Success, @ErrorMessage as Message, 0 as UpdatedCount;
    END CATCH
END;
GO

CREATE OR ALTER PROCEDURE dbo.sp_PerformanceOptimization
    @Action NVARCHAR(50) = 'ANALYZE', 
    @TableName NVARCHAR(128) = NULL
AS BEGIN SET NOCOUNT ON; IF @Action = 'ANALYZE' BEGIN SELECT 'Performance Analysis Results' as Action; SELECT * FROM dbo.vw_ProcessingPerformance; SELECT * FROM dbo.vw_SystemHealth; SELECT * FROM dbo.vw_PartitionPerformance; SELECT SchemaName, TableName, IndexName, avg_fragmentation_in_percent as FragmentationPercent, page_count as PageCount FROM dbo.vw_IndexPerformance WHERE avg_fragmentation_in_percent > 10 ORDER BY avg_fragmentation_in_percent DESC; END ELSE IF @Action = 'REBUILD_INDEXES' BEGIN DECLARE @SQL NVARCHAR(MAX); DECLARE @IndexName NVARCHAR(128); DECLARE @SchemaName NVARCHAR(128); DECLARE @TableNameLocal NVARCHAR(128); DECLARE index_cursor CURSOR FOR SELECT SchemaName, TableName, IndexName FROM dbo.vw_IndexPerformance WHERE avg_fragmentation_in_percent > 30 AND (@TableName IS NULL OR TableName = @TableName) AND page_count > 1000; OPEN index_cursor; FETCH NEXT FROM index_cursor INTO @SchemaName, @TableNameLocal, @IndexName; WHILE @@FETCH_STATUS = 0 BEGIN SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @SchemaName + '].[' + @TableNameLocal + '] REBUILD WITH (ONLINE = ON, DATA_COMPRESSION = PAGE);'; BEGIN TRY EXEC sp_executesql @SQL; PRINT 'Rebuilt index: ' + @IndexName + ' on ' + @SchemaName + '.' + @TableNameLocal; END TRY BEGIN CATCH PRINT 'Failed to rebuild index: ' + @IndexName + ' - ' + ERROR_MESSAGE(); END CATCH FETCH NEXT FROM index_cursor INTO @SchemaName, @TableNameLocal, @IndexName; END CLOSE index_cursor; DEALLOCATE index_cursor; END ELSE IF @Action = 'UPDATE_STATS' BEGIN UPDATE STATISTICS dbo.Claims WITH FULLSCAN; UPDATE STATISTICS dbo.Cms1500LineItems WITH FULLSCAN; UPDATE STATISTICS dbo.Cms1500Diagnoses WITH FULLSCAN; UPDATE STATISTICS dbo.FailedClaimDetails WITH FULLSCAN; PRINT 'Statistics updated for main tables'; END ELSE IF @Action = 'MANAGE_PARTITIONS' BEGIN DECLARE @NextBoundary DATE = DATEADD(MONTH, 1, EOMONTH(GETDATE())); DECLARE @PartitionFunction NVARCHAR(128); WHILE @NextBoundary <= DATEADD(MONTH, 3, GETDATE()) BEGIN IF NOT EXISTS ( SELECT 1 FROM sys.partition_range_values rv INNER JOIN sys.partition_functions pf ON rv.function_id = pf.function_id WHERE pf.name IN ('pf_ClaimsByServiceDate', 'pf_LineItemsByServiceDate') AND rv.value = @NextBoundary ) BEGIN PRINT 'Action Required: Add partition for boundary: ' + CAST(@NextBoundary AS NVARCHAR(10)) + '. Manual SPLIT RANGE needed.'; END SET @NextBoundary = DATEADD(MONTH, 1, @NextBoundary); END END END;
GO

CREATE OR ALTER PROCEDURE dbo.sp_AutomatedMaintenance
AS BEGIN SET NOCOUNT ON; PRINT 'Starting automated maintenance at ' + CAST(GETDATE() AS NVARCHAR(50)); EXEC dbo.sp_PerformanceOptimization @Action = 'UPDATE_STATS'; DECLARE @FragmentedIndexes INT; SELECT @FragmentedIndexes = COUNT(*) FROM dbo.vw_IndexPerformance WHERE avg_fragmentation_in_percent > 30 AND page_count > 1000; IF @FragmentedIndexes > 0 BEGIN PRINT 'Found ' + CAST(@FragmentedIndexes AS NVARCHAR(10)) + ' highly fragmented indexes. Consider Rebuilding.'; END DELETE FROM dbo.FailedClaimDetails WHERE Status = 'Resolved' AND ResolvedTimestamp < DATEADD(DAY, -90, GETDATE()); DECLARE @ArchivedCount INT = @@ROWCOUNT; IF @ArchivedCount > 0 BEGIN PRINT 'Archived ' + CAST(@ArchivedCount AS NVARCHAR(10)) + ' old resolved failed claims.'; END EXEC dbo.sp_PerformanceOptimization @Action = 'MANAGE_PARTITIONS'; PRINT 'Automated maintenance completed at ' + CAST(GETDATE() AS NVARCHAR(50)); END;
GO

CREATE OR ALTER PROCEDURE dbo.sp_ManagePartitions
    @Action NVARCHAR(20) = 'ADD_FUTURE', @MonthsAhead INT = 6
AS BEGIN SET NOCOUNT ON; IF @Action = 'ADD_FUTURE' BEGIN DECLARE @CurrentBoundary DATE = EOMONTH(GETDATE()); DECLARE @EndBoundary DATE = EOMONTH(DATEADD(MONTH, @MonthsAhead, GETDATE())); DECLARE @SQL_Cmd NVARCHAR(MAX); WHILE @CurrentBoundary < @EndBoundary BEGIN SET @CurrentBoundary = DATEADD(MONTH, 1, @CurrentBoundary); SET @CurrentBoundary = DATEFROMPARTS(YEAR(@CurrentBoundary), MONTH(@CurrentBoundary), 1); IF NOT EXISTS (SELECT 1 FROM sys.partition_range_values prv JOIN sys.partition_functions pf ON prv.function_id = pf.function_id WHERE pf.name = 'pf_ClaimsByServiceDate' AND CONVERT(date, prv.value) = @CurrentBoundary) BEGIN SET @SQL_Cmd = 'ALTER PARTITION SCHEME ps_ClaimsByServiceDate NEXT USED [PRIMARY]; ALTER PARTITION FUNCTION pf_ClaimsByServiceDate() SPLIT RANGE (''' + CONVERT(VARCHAR(10), @CurrentBoundary, 120) + ''');'; BEGIN TRY EXEC (@SQL_Cmd); PRINT 'Added partition for Claims: ' + CONVERT(VARCHAR(10), @CurrentBoundary, 120); END TRY BEGIN CATCH PRINT 'Failed to add partition for Claims ' + CONVERT(VARCHAR(10), @CurrentBoundary, 120) + ': ' + ERROR_MESSAGE(); END CATCH; END; IF NOT EXISTS (SELECT 1 FROM sys.partition_range_values prv JOIN sys.partition_functions pf ON prv.function_id = pf.function_id WHERE pf.name = 'pf_LineItemsByServiceDate' AND CONVERT(date, prv.value) = @CurrentBoundary) BEGIN SET @SQL_Cmd = 'ALTER PARTITION SCHEME ps_LineItemsByServiceDate NEXT USED [PRIMARY]; ALTER PARTITION FUNCTION pf_LineItemsByServiceDate() SPLIT RANGE (''' + CONVERT(VARCHAR(10), @CurrentBoundary, 120) + ''');'; BEGIN TRY EXEC (@SQL_Cmd); PRINT 'Added partition for LineItems: ' + CONVERT(VARCHAR(10), @CurrentBoundary, 120); END TRY BEGIN CATCH PRINT 'Failed to add partition for LineItems ' + CONVERT(VARCHAR(10), @CurrentBoundary, 120) + ': ' + ERROR_MESSAGE(); END CATCH; END; END; END ELSE IF @Action = 'ANALYZE' BEGIN SELECT * FROM dbo.vw_PartitionPerformance ORDER BY TableName, partition_number; END END;
GO

-- ===========================
-- SAMPLE DATA INSERTION 
-- ===========================

DELETE FROM dbo.Claims;
DELETE FROM dbo.Cms1500Diagnoses;
DELETE FROM dbo.Cms1500LineItems;
DELETE FROM dbo.FailedClaimDetails;
DELETE FROM dbo.OrganizationRegions; 
DELETE FROM dbo.ClinicalDepartments; 
DELETE FROM dbo.FinancialClasses; 
DELETE FROM dbo.Facilities; 
DELETE FROM dbo.Organizations; 
DELETE FROM dbo.Regions; 
DELETE FROM dbo.StandardPayers; 
DELETE FROM dbo.HccTypes;
GO

INSERT INTO dbo.StandardPayers (StandardPayerCode, StandardPayerName, PayerCategory) VALUES
('MEDICARE', 'Medicare', 'Government'), ('MEDICAID', 'Medicaid', 'Government'),
('BLUECROSS', 'Blue Cross', 'Commercial'), ('OTHERS', 'Others', 'Other'),
('SELFPAY', 'Self Pay', 'Self Pay'), ('HMO_PPO', 'Managed Care (HMO/PPO)', 'Managed Care'),
('TRICARE', 'Tricare/Champus', 'Government'), ('COMMERCIAL', 'Commercial', 'Commercial'),
('WORKCOMP', 'Workers Compensation', 'Workers Comp'), ('MEDICARE_ADV', 'Medicare Advantage', 'Government');
GO

INSERT INTO dbo.HccTypes (HccCode, HccName, HccAgency) VALUES
('CMS_V24', 'CMS-HCC Version 24', 'CMS'), ('CMS_V28', 'CMS-HCC Version 28', 'CMS'),
('HHS_V05', 'HHS-HCC Version 05', 'HHS'), ('HHS_V07', 'HHS-HCC Version 07', 'HHS');
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
SELECT 'FAC001', 'Springfield General Hospital', 'SGH001', o.OrganizationId, r.RegionId, 'Springfield', 'IL', 350, 7, 'Epic', 'Teaching', 'N'
FROM dbo.Organizations o JOIN dbo.Regions r ON r.RegionCode = 'CIL' WHERE o.OrganizationCode = 'RHS';
INSERT INTO dbo.Facilities (FacilityId, FacilityName, FacilityCode, OrganizationId, RegionId, City, StateCode, BedSize, FiscalReportingMonth, EmrSystem, FacilityType, CriticalAccess)
SELECT 'FAC002', 'Chicago Metro Care', 'CMC001', o.OrganizationId, r.RegionId, 'Chicago', 'IL', 500, 1, 'Cerner', 'Teaching', 'N'
FROM dbo.Organizations o JOIN dbo.Regions r ON r.RegionCode = 'NIL' WHERE o.OrganizationCode = 'MHN';
GO

INSERT INTO dbo.FinancialClasses (FinancialClassId, FinancialClassDescription, StandardPayerId, FacilityId)
SELECT 'FC_MCARE_SGH', 'Medicare Part A - SGH', sp.StandardPayerId, f.FacilityId
FROM dbo.StandardPayers sp, dbo.Facilities f WHERE sp.StandardPayerCode = 'MEDICARE' AND f.FacilityCode = 'SGH001';
INSERT INTO dbo.FinancialClasses (FinancialClassId, FinancialClassDescription, StandardPayerId, FacilityId)
SELECT 'FC_BCBS_CMC', 'BlueCross PPO - CMC', sp.StandardPayerId, f.FacilityId
FROM dbo.StandardPayers sp, dbo.Facilities f WHERE sp.StandardPayerCode = 'BLUECROSS' AND f.FacilityCode = 'CMC001';
GO

INSERT INTO dbo.ClinicalDepartments (ClinicalDepartmentCode, DepartmentDescription, FacilityId, DepartmentType, DefaultPlaceOfService, IsSurgical, IsEmergency)
VALUES
('CARD01', 'Cardiology SGH', (SELECT FacilityId FROM dbo.Facilities WHERE FacilityCode = 'SGH001'), 'Outpatient', '11', 0, 0),
('EMER01', 'Emergency Dept SGH', (SELECT FacilityId FROM dbo.Facilities WHERE FacilityCode = 'SGH001'), 'Emergency', '23', 0, 1),
('SURG01', 'General Surgery CMC', (SELECT FacilityId FROM dbo.Facilities WHERE FacilityCode = 'CMC001'), 'Inpatient', '21', 1, 0),
('ICU02', 'Intensive Care Unit CMC', (SELECT FacilityId FROM dbo.Facilities WHERE FacilityCode = 'CMC001'), 'Inpatient', '21', 0, 0);
GO

-- ===========================
-- FUNCTIONS 
-- ===========================

CREATE OR ALTER FUNCTION dbo.fn_GetFacilitySummary(@FacilityId NVARCHAR(50))
RETURNS TABLE
AS RETURN
( SELECT
    f.FacilityId, f.FacilityName, f.FacilityType, f.CriticalAccess, f.BedSize,
    o.OrganizationName, r.RegionName,
    ISNULL(cs.TotalClaims, 0) as TotalClaims, ISNULL(cs.PendingClaims, 0) as PendingClaims,
    ISNULL(cs.CompletedClaims, 0) as CompletedClaims, ISNULL(cs.TotalCharges, 0) as TotalCharges,
    ISNULL(cs.AvgChargeAmount, 0) as AvgChargeAmount,
    (SELECT COUNT_BIG(*) FROM dbo.ClinicalDepartments cd WHERE cd.FacilityId = f.FacilityId AND cd.Active = 1) as TotalActiveDepartments,
    (SELECT COUNT_BIG(*) FROM dbo.FinancialClasses fc WHERE fc.FacilityId = f.FacilityId AND fc.Active = 1) as TotalActiveFinancialClasses
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

CREATE OR ALTER TRIGGER tr_Facilities_UpdatedDate ON dbo.Facilities AFTER UPDATE AS
BEGIN SET NOCOUNT ON; IF UPDATE(UpdatedDate) RETURN; UPDATE dbo.Facilities SET UpdatedDate = GETDATE() FROM dbo.Facilities f INNER JOIN inserted i ON f.FacilityId = i.FacilityId; END;
GO
CREATE OR ALTER TRIGGER tr_FinancialClasses_UpdatedDate ON dbo.FinancialClasses AFTER UPDATE AS
BEGIN SET NOCOUNT ON; IF UPDATE(UpdatedDate) RETURN; UPDATE dbo.FinancialClasses SET UpdatedDate = GETDATE() FROM dbo.FinancialClasses fc INNER JOIN inserted i ON fc.FinancialClassId = i.FinancialClassId; END;
GO
CREATE OR ALTER TRIGGER tr_ClinicalDepartments_UpdatedDate ON dbo.ClinicalDepartments AFTER UPDATE AS
BEGIN SET NOCOUNT ON; IF UPDATE(UpdatedDate) RETURN; UPDATE dbo.ClinicalDepartments SET UpdatedDate = GETDATE() FROM dbo.ClinicalDepartments cd INNER JOIN inserted i ON cd.DepartmentId = i.DepartmentId; END;
GO
CREATE OR ALTER TRIGGER tr_Claims_UpdatedDate ON dbo.Claims AFTER UPDATE AS
BEGIN SET NOCOUNT ON; IF UPDATE(UpdatedDate) RETURN; UPDATE dbo.Claims SET UpdatedDate = GETDATE() FROM dbo.Claims c INNER JOIN inserted i ON c.ClaimId = i.ClaimId AND c.ServiceDate = i.ServiceDate; END;
GO

CREATE OR ALTER VIEW dbo.vw_CompressionStats AS
SELECT 
    s.name as SchemaName, 
    t.name as TableName, 
    i.name as IndexName, 
    p.partition_number, 
    p.data_compression_desc as CompressionType, 
    p.rows as PartitionRows, 
    CAST(SUM(au.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)) as SizeMB, 
    CAST(SUM(au.used_pages) * 8.0 / 1024 AS DECIMAL(10,2)) as UsedMB,
    CAST(SUM(au.data_pages) * 8.0 / 1024 AS DECIMAL(10,2)) as DataMB
FROM sys.tables t 
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id 
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id 
INNER JOIN sys.allocation_units au ON p.partition_id = au.container_id
WHERE s.name = 'dbo' AND t.name IN ('Claims', 'Cms1500LineItems', 'Cms1500Diagnoses', 'FailedClaimDetails')
GROUP BY s.name, t.name, i.name, p.partition_number, p.data_compression_desc, p.rows;
GO

CREATE OR ALTER VIEW dbo.vw_RealTimeDashboard AS
SELECT GETDATE() as LastUpdated,
    (SELECT COUNT_BIG(*) FROM dbo.Claims WHERE CreatedDate >= CAST(GETDATE() AS DATE)) as ClaimsToday,
    (SELECT COUNT_BIG(*) FROM dbo.Claims WHERE CreatedDate >= DATEADD(HOUR, -1, GETDATE())) as ClaimsLastHour,
    (SELECT COUNT_BIG(*) FROM dbo.Claims WHERE ProcessingStatus = 'PENDING') as PendingClaims,
    (SELECT COUNT_BIG(*) FROM dbo.Claims WHERE ProcessingStatus = 'PROCESSING') as ProcessingClaims,
    (SELECT COUNT_BIG(*) FROM dbo.FailedClaimDetails WHERE FailureTimestamp >= CAST(GETDATE() AS DATE)) as FailuresToday,
    (SELECT COUNT_BIG(*) FROM dbo.FailedClaimDetails WHERE FailureTimestamp >= DATEADD(HOUR, -1, GETDATE())) as FailuresLastHour,
    (SELECT AVG(CAST(DATEDIFF(SECOND, CreatedDate, ProcessedDate) AS DECIMAL(10,2))) FROM dbo.Claims WHERE ProcessedDate >= DATEADD(HOUR, -1, GETDATE())) as AvgProcessingTimeSecondsLastHour,
    (SELECT SUM(TotalChargeAmount) FROM dbo.Claims WHERE CreatedDate >= CAST(GETDATE() AS DATE)) as TotalChargesProcessedToday,
    (SELECT COUNT_BIG(*) FROM dbo.Claims) as TotalClaimsInSystem,
    (SELECT COUNT_BIG(DISTINCT FacilityId) FROM dbo.Claims WHERE CreatedDate >= CAST(GETDATE() AS DATE)) as ActiveFacilitiesToday;
GO

CREATE OR ALTER VIEW dbo.vw_SystemAlerts AS
SELECT AlertType, AlertMessage, Severity, AlertValue, Threshold, GETDATE() as CheckTime
FROM (
    SELECT 'ERROR_RATE' as AlertType, 'High error rate detected in last hour' as AlertMessage, 'HIGH' as Severity,
        CAST(ISNULL((SELECT COUNT_BIG(*) FROM dbo.FailedClaimDetails WHERE FailureTimestamp >= DATEADD(HOUR, -1, GETDATE())),0) AS FLOAT) /
        NULLIF(ISNULL((SELECT COUNT_BIG(*) FROM dbo.Claims WHERE CreatedDate >= DATEADD(HOUR, -1, GETDATE())),0), 0) * 100 as AlertValue,
        10.0 as Threshold WHERE NULLIF(ISNULL((SELECT COUNT_BIG(*) FROM dbo.Claims WHERE CreatedDate >= DATEADD(HOUR, -1, GETDATE())),0), 0) IS NOT NULL
    UNION ALL
    SELECT 'SLOW_PROCESSING' as AlertType, 'Average processing time exceeds threshold' as AlertMessage, 'MEDIUM' as Severity,
        (SELECT AVG(CAST(DATEDIFF(SECOND, CreatedDate, ProcessedDate) AS DECIMAL(10,2))) FROM dbo.Claims WHERE ProcessedDate >= DATEADD(HOUR, -1, GETDATE())) as AlertValue,
        300.0 as Threshold
    UNION ALL
    SELECT 'HIGH_PENDING_VOLUME' as AlertType, 'Large number of pending claims detected' as AlertMessage, 'MEDIUM' as Severity,
        CAST((SELECT COUNT_BIG(*) FROM dbo.Claims WHERE ProcessingStatus = 'PENDING') AS FLOAT) as AlertValue,
        1000.0 as Threshold
    UNION ALL
    SELECT 'DATABASE_SIZE_APPROACHING_LIMIT' as AlertType, 'Database size (used MB) approaching capacity for specific tables' as AlertMessage, 'LOW' as Severity,
        (SELECT SUM(UsedMB) FROM dbo.vw_CompressionStats) as AlertValue, 
        10000.0 as Threshold 
) alerts
WHERE AlertValue IS NOT NULL AND AlertValue > Threshold; 
GO

-- ===========================
-- PERMISSIONS AND SECURITY
-- ===========================
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'ClaimsProcessorApp' AND type = 'R') CREATE ROLE ClaimsProcessorApp;
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'ClaimsAnalyst' AND type = 'R') CREATE ROLE ClaimsAnalyst;
GO
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'ClaimsAdmin' AND type = 'R') CREATE ROLE ClaimsAdmin;
GO

GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Claims TO ClaimsProcessorApp;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Cms1500Diagnoses TO ClaimsProcessorApp;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Cms1500LineItems TO ClaimsProcessorApp;
GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.FailedClaimDetails TO ClaimsProcessorApp;
GRANT SELECT ON dbo.Facilities TO ClaimsProcessorApp;
GRANT SELECT ON dbo.ClinicalDepartments TO ClaimsProcessorApp;
GRANT SELECT ON dbo.FinancialClasses TO ClaimsProcessorApp;
GRANT SELECT ON dbo.Organizations TO ClaimsProcessorApp;
GRANT SELECT ON dbo.Regions TO ClaimsProcessorApp;
GRANT SELECT ON dbo.StandardPayers TO ClaimsProcessorApp;
GRANT REFERENCES ON TYPE::dbo.ClaimIdServiceDateList TO ClaimsProcessorApp; -- Corrected from EXECUTE to REFERENCES
GRANT EXECUTE ON dbo.sp_ValidateFacilityClaimAssignment TO ClaimsProcessorApp;
GRANT EXECUTE ON dbo.sp_BulkAssignClaimsToFacility TO ClaimsProcessorApp;
GO

GRANT SELECT ON SCHEMA::dbo TO ClaimsAnalyst; 
GRANT EXECUTE ON dbo.fn_GetFacilitySummary TO ClaimsAnalyst;
GO

GRANT CONTROL ON DATABASE::edi_production TO ClaimsAdmin; 
GO

-- ===========================
-- COMMENTS FOR DOCUMENTATION
-- ===========================
IF EXISTS (SELECT * FROM sys.extended_properties WHERE major_id = SCHEMA_ID('dbo') AND name = 'MS_Description' AND minor_id = 0)
    EXEC sp_dropextendedproperty 'MS_Description', 'SCHEMA', 'dbo';
EXEC sp_addextendedproperty 'MS_Description', 'Enhanced production database with columnstore indexes, partitioning, and compression for high-performance claims processing', 'SCHEMA', 'dbo';
GO

IF OBJECT_ID('dbo.Claims') IS NOT NULL AND NOT EXISTS (SELECT * FROM sys.extended_properties WHERE major_id = OBJECT_ID('dbo.Claims') AND name = 'MS_Description' AND minor_id = 0)
    EXEC sp_addextendedproperty 'MS_Description', 'Partitioned claims table with page compression for optimal performance', 'SCHEMA', 'dbo', 'TABLE', 'Claims';
GO
IF OBJECT_ID('dbo.Claims') IS NOT NULL AND COLUMNPROPERTY(OBJECT_ID('dbo.Claims'), 'ServiceDate', 'ColumnId') IS NOT NULL AND NOT EXISTS (SELECT * FROM sys.extended_properties ep JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id WHERE c.object_id = OBJECT_ID('dbo.Claims') AND c.name = 'ServiceDate' AND ep.name = 'MS_Description')
    EXEC sp_addextendedproperty 'MS_Description', 'Required for partitioning - links claim to service date partition', 'SCHEMA', 'dbo', 'TABLE', 'Claims', 'COLUMN', 'ServiceDate';
GO


PRINT 'Enhanced SQL Server Production Database Schema script execution attempted.';
PRINT '- Check for any errors above.';
PRINT '- Ensure all objects are created as expected.';
GO
-- End of Enhanced SQL Server Production Database Schema
