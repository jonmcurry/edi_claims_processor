-- SQL Server Production Database Schema with Comprehensive Facility Management
-- ================================================================================
-- Corrected version with GO batch separators, especially for sp_addextendedproperty.

-- Check if the database already exists and create it if not
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'edi_production')
BEGIN
    CREATE DATABASE edi_production;
END
GO

-- Switch to the newly created database context
USE edi_production;
GO

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
);
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
);
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
);
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
);
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
);
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
);
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
);
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
);
GO

-- Enhanced Claims table (production claims with facility relationships)
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
    ServiceDate DATE,

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
);
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
);
GO

-- CMS 1500 Line Items Table (Sections 24A-J)
CREATE TABLE dbo.Cms1500LineItems (
    LineItemId BIGINT IDENTITY(1,1) PRIMARY KEY,
    ClaimId NVARCHAR(50) NOT NULL REFERENCES dbo.Claims(ClaimId) ON DELETE CASCADE,
    LineNumber INT NOT NULL CHECK (LineNumber BETWEEN 1 AND 99),

    -- Section 24A: Date of Service (From/To)
    ServiceDateFrom DATE,
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
);
GO

-- ===========================
-- INDEXES FOR PERFORMANCE
-- ===========================

-- Organization indexes
CREATE INDEX IX_Organizations_Active ON dbo.Organizations(Active);
CREATE INDEX IX_Organizations_Code ON dbo.Organizations(OrganizationCode);
GO

-- Region indexes
CREATE INDEX IX_Regions_Active ON dbo.Regions(Active);
CREATE INDEX IX_Regions_Code ON dbo.Regions(RegionCode);
GO

-- Organization-Region mapping indexes
CREATE INDEX IX_OrganizationRegions_Org ON dbo.OrganizationRegions(OrganizationId);
CREATE INDEX IX_OrganizationRegions_Region ON dbo.OrganizationRegions(RegionId);
CREATE INDEX IX_OrganizationRegions_Primary ON dbo.OrganizationRegions(IsPrimary) WHERE IsPrimary = 1;
GO

-- Facility indexes
CREATE INDEX IX_Facilities_Org ON dbo.Facilities(OrganizationId);
CREATE INDEX IX_Facilities_Region ON dbo.Facilities(RegionId);
CREATE INDEX IX_Facilities_Active ON dbo.Facilities(Active);
CREATE INDEX IX_Facilities_Type ON dbo.Facilities(FacilityType);
CREATE INDEX IX_Facilities_State ON dbo.Facilities(StateCode);
GO

-- Standard Payer indexes
CREATE INDEX IX_StandardPayers_Active ON dbo.StandardPayers(Active);
CREATE INDEX IX_StandardPayers_Category ON dbo.StandardPayers(PayerCategory);
GO

-- Financial class indexes
CREATE INDEX IX_FinancialClasses_Facility ON dbo.FinancialClasses(FacilityId);
CREATE INDEX IX_FinancialClasses_Payer ON dbo.FinancialClasses(StandardPayerId);
CREATE INDEX IX_FinancialClasses_Active ON dbo.FinancialClasses(Active);
GO

-- Department indexes
CREATE INDEX IX_ClinicalDepartments_Facility ON dbo.ClinicalDepartments(FacilityId);
CREATE INDEX IX_ClinicalDepartments_Code ON dbo.ClinicalDepartments(ClinicalDepartmentCode);
CREATE INDEX IX_ClinicalDepartments_Active ON dbo.ClinicalDepartments(Active);
CREATE INDEX IX_ClinicalDepartments_Type ON dbo.ClinicalDepartments(DepartmentType);
GO

-- Enhanced claim indexes
CREATE INDEX IX_Claims_Facility ON dbo.Claims(FacilityId);
CREATE INDEX IX_Claims_Department ON dbo.Claims(DepartmentId);
CREATE INDEX IX_Claims_FinancialClass ON dbo.Claims(FinancialClassId);
CREATE INDEX IX_Claims_Status_Facility ON dbo.Claims(ProcessingStatus, FacilityId);
CREATE INDEX IX_Claims_ServiceDate_Facility ON dbo.Claims(ServiceDate, FacilityId);
CREATE INDEX IX_Claims_ValidationStatus ON dbo.Claims(ValidationStatus);
CREATE INDEX IX_Claims_ProcessingStatus ON dbo.Claims(ProcessingStatus);
GO

-- CMS 1500 indexes
CREATE INDEX IX_Cms1500Diagnoses_Claim ON dbo.Cms1500Diagnoses(ClaimId);
CREATE INDEX IX_Cms1500Diagnoses_IcdCode ON dbo.Cms1500Diagnoses(IcdCode);
CREATE INDEX IX_Cms1500LineItems_Claim ON dbo.Cms1500LineItems(ClaimId);
CREATE INDEX IX_Cms1500LineItems_CptCode ON dbo.Cms1500LineItems(CptCode);
CREATE INDEX IX_Cms1500LineItems_ServiceDate ON dbo.Cms1500LineItems(ServiceDateFrom);
GO

-- ===========================
-- VIEWS FOR REPORTING
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
-- STORED PROCEDURES
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
-- PERMISSIONS (Adjust as needed)
-- ===========================
/*
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'YourApplicationUser')
BEGIN
    CREATE USER [YourApplicationUser] WITHOUT LOGIN;
END
GO
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo TO [YourApplicationUser];
GRANT EXECUTE ON SCHEMA::dbo TO [YourApplicationUser];
GO
*/

-- ===========================
-- COMMENTS FOR DOCUMENTATION
-- ===========================
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
EXEC sp_addextendedproperty 'MS_Description', 'Standardized payer types for consistent categorization', 'SCHEMA', 'dbo', 'TABLE', 'StandardPayers';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Hierarchical Condition Categories (HCC) types for CMS and HHS', 'SCHEMA', 'dbo', 'TABLE', 'HccTypes';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Facility-specific financial classes linking to standard payers', 'SCHEMA', 'dbo', 'TABLE', 'FinancialClasses';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Clinical departments within facilities', 'SCHEMA', 'dbo', 'TABLE', 'ClinicalDepartments';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Production claims table with facility relationships and validation results', 'SCHEMA', 'dbo', 'TABLE', 'Claims';
GO
EXEC sp_addextendedproperty 'MS_Description', 'Links claim to the facility where service was provided', 'SCHEMA', 'dbo', 'TABLE', 'Claims', 'COLUMN', 'FacilityId';
GO

-- End of SQL Server Production Database Schema