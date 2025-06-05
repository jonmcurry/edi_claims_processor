# app/database/models/sqlserver_models.py
"""
SQLAlchemy ORM models for the SQL Server database (edi_production)
Reflects the schema defined in `database_scripts/sqlserver_create_results_database.sql`
and `database_scripts/failed_claims_table.sql`.
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Boolean, Text, ForeignKey,
    UniqueConstraint, Index, DECIMAL, CHAR, DATE, NVARCHAR # NVARCHAR for SQL Server strings
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import datetime

Base = declarative_base()

# --- DBO Schema (SQL Server) ---

class RvuData(Base):
    """ dbo.RvuData table """
    __tablename__ = 'RvuData'
    __table_args__ = {'schema': 'dbo'}

    RvuDataId = Column(Integer, primary_key=True, autoincrement=True)
    CptCode = Column(NVARCHAR(10), unique=True, nullable=False)
    Description = Column(NVARCHAR)
    RvuValue = Column(DECIMAL(10, 2), nullable=False)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)


class ProductionOrganization(Base):
    """ dbo.Organizations table """
    __tablename__ = 'Organizations'
    __table_args__ = {'schema': 'dbo'}

    OrganizationId = Column(Integer, primary_key=True, autoincrement=True)
    OrganizationName = Column(NVARCHAR(200), nullable=False)
    OrganizationCode = Column(NVARCHAR(20), unique=True)
    AddressLine1 = Column(NVARCHAR(255))
    AddressLine2 = Column(NVARCHAR(255))
    City = Column(NVARCHAR(100))
    StateCode = Column(CHAR(2))
    ZipCode = Column(NVARCHAR(10))
    CountryCode = Column(CHAR(3), default='USA')
    Phone = Column(NVARCHAR(20))
    Email = Column(NVARCHAR(255))
    Website = Column(NVARCHAR(255))
    TaxId = Column(NVARCHAR(20))
    Active = Column(Boolean, default=True)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facilities = relationship("ProductionFacility", back_populates="organization")
    organization_regions = relationship("ProductionOrganizationRegion", back_populates="organization")


class ProductionRegion(Base):
    """ dbo.Regions table """
    __tablename__ = 'Regions'
    __table_args__ = {'schema': 'dbo'}

    RegionId = Column(Integer, primary_key=True, autoincrement=True)
    RegionName = Column(NVARCHAR(200), nullable=False)
    RegionCode = Column(NVARCHAR(20), unique=True, nullable=False)
    Description = Column(NVARCHAR) # NVARCHAR(MAX) -> SQLAlchemy Text or NVARCHAR without length
    Active = Column(Boolean, default=True)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facilities = relationship("ProductionFacility", back_populates="region")
    organization_regions = relationship("ProductionOrganizationRegion", back_populates="region")

class ProductionOrganizationRegion(Base):
    """ dbo.OrganizationRegions table """
    __tablename__ = 'OrganizationRegions'
    __table_args__ = (
        UniqueConstraint('OrganizationId', 'RegionId', name='PK_OrganizationRegions'), # Name from SQL Server
        {'schema': 'dbo'}
    )

    OrganizationId = Column(Integer, ForeignKey('dbo.Organizations.OrganizationId', ondelete='CASCADE'), primary_key=True)
    RegionId = Column(Integer, ForeignKey('dbo.Regions.RegionId', ondelete='CASCADE'), primary_key=True)
    IsPrimary = Column(Boolean, default=False)
    EffectiveDate = Column(DATE, default=datetime.date.today)
    EndDate = Column(DATE)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)

    organization = relationship("ProductionOrganization", back_populates="organization_regions")
    region = relationship("ProductionRegion", back_populates="organization_regions")

class ProductionFacility(Base):
    """ dbo.Facilities table """
    __tablename__ = 'Facilities'
    __table_args__ = {'schema': 'dbo'}

    FacilityId = Column(NVARCHAR(50), primary_key=True) # User-defined facility ID
    FacilityName = Column(NVARCHAR(200), nullable=False)
    FacilityCode = Column(NVARCHAR(20), unique=True)
    OrganizationId = Column(Integer, ForeignKey('dbo.Organizations.OrganizationId'))
    RegionId = Column(Integer, ForeignKey('dbo.Regions.RegionId'))
    City = Column(NVARCHAR(100))
    StateCode = Column(CHAR(2))
    ZipCode = Column(NVARCHAR(10))
    AddressLine1 = Column(NVARCHAR(255))
    AddressLine2 = Column(NVARCHAR(255))
    BedSize = Column(Integer) # CHECK constraint in DB
    FiscalReportingMonth = Column(Integer) # CHECK constraint in DB
    EmrSystem = Column(NVARCHAR(100))
    FacilityType = Column(NVARCHAR(20)) # CHECK constraint in DB
    CriticalAccess = Column(CHAR(1)) # CHECK constraint in DB
    Phone = Column(NVARCHAR(20))
    Fax = Column(NVARCHAR(20))
    Email = Column(NVARCHAR(255))
    NpiNumber = Column(NVARCHAR(10))
    TaxId = Column(NVARCHAR(20))
    LicenseNumber = Column(NVARCHAR(50))
    Active = Column(Boolean, default=True)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    organization = relationship("ProductionOrganization", back_populates="facilities")
    region = relationship("ProductionRegion", back_populates="facilities")
    financial_classes = relationship("ProductionFinancialClass", back_populates="facility")
    clinical_departments = relationship("ProductionClinicalDepartment", back_populates="facility")
    claims = relationship("ProductionClaim", back_populates="facility")

class ProductionStandardPayer(Base):
    """ dbo.StandardPayers table """
    __tablename__ = 'StandardPayers'
    __table_args__ = {'schema': 'dbo'}

    StandardPayerId = Column(Integer, primary_key=True, autoincrement=True)
    StandardPayerCode = Column(NVARCHAR(20), unique=True, nullable=False)
    StandardPayerName = Column(NVARCHAR(100), nullable=False)
    PayerCategory = Column(NVARCHAR(50))
    Description = Column(NVARCHAR) # NVARCHAR(MAX)
    Active = Column(Boolean, default=True)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)

    financial_classes = relationship("ProductionFinancialClass", back_populates="standard_payer")

class ProductionHccType(Base):
    """ dbo.HccTypes table """
    __tablename__ = 'HccTypes'
    __table_args__ = {'schema': 'dbo'}

    HccTypeId = Column(Integer, primary_key=True, autoincrement=True)
    HccCode = Column(NVARCHAR(10), unique=True, nullable=False)
    HccName = Column(NVARCHAR(100), nullable=False)
    HccAgency = Column(NVARCHAR(10)) # CHECK constraint in DB
    Description = Column(NVARCHAR) # NVARCHAR(MAX)
    Active = Column(Boolean, default=True)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)

    financial_classes = relationship("ProductionFinancialClass", back_populates="hcc_type")

class ProductionFinancialClass(Base):
    """ dbo.FinancialClasses table """
    __tablename__ = 'FinancialClasses'
    __table_args__ = {'schema': 'dbo'}

    FinancialClassId = Column(NVARCHAR(20), primary_key=True) # User-defined ID
    FinancialClassDescription = Column(NVARCHAR(200), nullable=False)
    StandardPayerId = Column(Integer, ForeignKey('dbo.StandardPayers.StandardPayerId'))
    HccTypeId = Column(Integer, ForeignKey('dbo.HccTypes.HccTypeId'))
    FacilityId = Column(NVARCHAR(50), ForeignKey('dbo.Facilities.FacilityId'))
    PayerPriority = Column(Integer, default=1)
    RequiresAuthorization = Column(Boolean, default=False)
    AuthorizationRequiredServices = Column(NVARCHAR) # NVARCHAR(MAX)
    CopayAmount = Column(DECIMAL(8, 2))
    DeductibleAmount = Column(DECIMAL(10, 2))
    ClaimSubmissionFormat = Column(NVARCHAR(20))
    DaysToFileClaim = Column(Integer, default=365)
    AcceptsElectronicClaims = Column(Boolean, default=True)
    Active = Column(Boolean, default=True)
    EffectiveDate = Column(DATE, default=datetime.date.today)
    EndDate = Column(DATE)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facility = relationship("ProductionFacility", back_populates="financial_classes")
    standard_payer = relationship("ProductionStandardPayer", back_populates="financial_classes")
    hcc_type = relationship("ProductionHccType", back_populates="financial_classes")
    claims = relationship("ProductionClaim", back_populates="financial_class")

class ProductionClinicalDepartment(Base):
    """ dbo.ClinicalDepartments table """
    __tablename__ = 'ClinicalDepartments'
    __table_args__ = (
        UniqueConstraint('ClinicalDepartmentCode', 'FacilityId', name='UQ__ClinicalDepartments__YourHash'), # Name from SQL Server if specific
        {'schema': 'dbo'}
    )

    DepartmentId = Column(Integer, primary_key=True, autoincrement=True)
    ClinicalDepartmentCode = Column(NVARCHAR(20), nullable=False)
    DepartmentDescription = Column(NVARCHAR(200), nullable=False)
    FacilityId = Column(NVARCHAR(50), ForeignKey('dbo.Facilities.FacilityId'))
    DepartmentType = Column(NVARCHAR(50))
    RevenueCode = Column(NVARCHAR(10))
    CostCenter = Column(NVARCHAR(20))
    SpecialtyCode = Column(NVARCHAR(10))
    IsSurgical = Column(Boolean, default=False)
    IsEmergency = Column(Boolean, default=False)
    IsCriticalCare = Column(Boolean, default=False)
    RequiresPriorAuth = Column(Boolean, default=False)
    DefaultPlaceOfService = Column(NVARCHAR(2))
    BillingProviderNpi = Column(NVARCHAR(10))
    DepartmentManager = Column(NVARCHAR(100))
    PhoneExtension = Column(NVARCHAR(10))
    Active = Column(Boolean, default=True)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    facility = relationship("ProductionFacility", back_populates="clinical_departments")
    claims = relationship("ProductionClaim", back_populates="department")

class ProductionClaim(Base):
    """ dbo.Claims table """
    __tablename__ = 'Claims'
    __table_args__ = {'schema': 'dbo'}

    ClaimId = Column(NVARCHAR(50), primary_key=True)
    FacilityId = Column(NVARCHAR(50), ForeignKey('dbo.Facilities.FacilityId'))
    DepartmentId = Column(Integer, ForeignKey('dbo.ClinicalDepartments.DepartmentId'))
    FinancialClassId = Column(NVARCHAR(20), ForeignKey('dbo.FinancialClasses.FinancialClassId'))
    PatientId = Column(NVARCHAR(50))
    PatientAge = Column(Integer)
    PatientDob = Column(DATE)
    PatientSex = Column(CHAR(1))
    PatientAccountNumber = Column(NVARCHAR(50))
    ProviderId = Column(NVARCHAR(50))
    ProviderType = Column(NVARCHAR(100))
    RenderingProviderNpi = Column(NVARCHAR(10))
    PlaceOfService = Column(NVARCHAR(10))
    ServiceDate = Column(DATE)
    TotalChargeAmount = Column(DECIMAL(12, 2))
    TotalClaimCharges = Column(DECIMAL(10, 2))
    PayerName = Column(NVARCHAR(100))
    PrimaryInsuranceId = Column(NVARCHAR(50))
    SecondaryInsuranceId = Column(NVARCHAR(50))
    ProcessingStatus = Column(NVARCHAR(20), default='PENDING')
    ProcessedDate = Column(DateTime)
    ClaimData = Column(NVARCHAR) # NVARCHAR(MAX) - JSON storage
    ValidationStatus = Column(NVARCHAR(20))
    ValidationResults = Column(NVARCHAR) # NVARCHAR(MAX) - JSON storage
    ValidationDate = Column(DateTime)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    CreatedBy = Column(NVARCHAR(100))
    UpdatedBy = Column(NVARCHAR(100))

    facility = relationship("ProductionFacility", back_populates="claims")
    department = relationship("ProductionClinicalDepartment", back_populates="claims")
    financial_class = relationship("ProductionFinancialClass", back_populates="claims")
    cms1500_diagnoses = relationship("ProductionCMS1500Diagnosis", back_populates="claim", cascade="all, delete-orphan")
    cms1500_line_items = relationship("ProductionCMS1500LineItem", back_populates="claim", cascade="all, delete-orphan")

class ProductionCMS1500Diagnosis(Base):
    """ dbo.Cms1500Diagnoses table """
    __tablename__ = 'Cms1500Diagnoses'
    __table_args__ = (
        UniqueConstraint('ClaimId', 'DiagnosisSequence', name='UQ__Cms1500Diagnoses__YourHash'), # Name from SQL Server
        {'schema': 'dbo'}
    )

    DiagnosisEntryId = Column(Integer, primary_key=True, autoincrement=True) # BIGINT in SQL Server
    ClaimId = Column(NVARCHAR(50), ForeignKey('dbo.Claims.ClaimId', ondelete='CASCADE'), nullable=False)
    DiagnosisSequence = Column(Integer, nullable=False) # CHECK (1-12)
    IcdCode = Column(NVARCHAR(10), nullable=False)
    IcdCodeType = Column(NVARCHAR(10), default='ICD10')
    DiagnosisDescription = Column(NVARCHAR) # NVARCHAR(MAX)
    IsPrimary = Column(Boolean, default=False)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)

    claim = relationship("ProductionClaim", back_populates="cms1500_diagnoses")

class ProductionCMS1500LineItem(Base):
    """ dbo.Cms1500LineItems table """
    __tablename__ = 'Cms1500LineItems'
    __table_args__ = (
        UniqueConstraint('ClaimId', 'LineNumber', name='UQ__Cms1500LineItems__YourHash'), # Name from SQL Server
        {'schema': 'dbo'}
    )

    LineItemId = Column(Integer, primary_key=True, autoincrement=True) # BIGINT in SQL Server
    ClaimId = Column(NVARCHAR(50), ForeignKey('dbo.Claims.ClaimId', ondelete='CASCADE'), nullable=False)
    LineNumber = Column(Integer, nullable=False) # CHECK (1-99)
    ServiceDateFrom = Column(DATE)
    ServiceDateTo = Column(DATE)
    PlaceOfServiceCode = Column(NVARCHAR(2))
    EmergencyIndicator = Column(CHAR(1))
    CptCode = Column(NVARCHAR(10), nullable=False)
    Modifier1 = Column(NVARCHAR(2))
    Modifier2 = Column(NVARCHAR(2))
    Modifier3 = Column(NVARCHAR(2))
    Modifier4 = Column(NVARCHAR(2))
    DiagnosisPointers = Column(NVARCHAR(10))
    LineChargeAmount = Column(DECIMAL(10, 2), nullable=False)
    Units = Column(Integer, default=1)
    EpsdtFamilyPlan = Column(CHAR(1))
    RenderingProviderIdQualifier = Column(NVARCHAR(2))
    RenderingProviderId = Column(NVARCHAR(20))
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    claim = relationship("ProductionClaim", back_populates="cms1500_line_items")

class FailedClaimDetail(Base):
    """ dbo.FailedClaimDetails table """
    __tablename__ = 'FailedClaimDetails'
    __table_args__ = {'schema': 'dbo'}

    FailedClaimDetailId = Column(Integer, primary_key=True, autoincrement=True) # BIGINT in SQL Server
    OriginalClaimId = Column(NVARCHAR(50), nullable=False)
    StagingClaimId = Column(NVARCHAR(50), nullable=True)
    FacilityId = Column(NVARCHAR(50), nullable=True)
    PatientAccountNumber = Column(NVARCHAR(50), nullable=True)
    ServiceDate = Column(DATE, nullable=True)
    FailureTimestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    ProcessingStage = Column(NVARCHAR(100), nullable=False)
    ErrorCodes = Column(NVARCHAR) # NVARCHAR(MAX)
    ErrorMessages = Column(NVARCHAR, nullable=False) # NVARCHAR(MAX)
    ClaimDataSnapshot = Column(NVARCHAR) # NVARCHAR(MAX)
    Status = Column(NVARCHAR(50), nullable=False, default='New')
    ResolutionNotes = Column(NVARCHAR) # NVARCHAR(MAX)
    ResolvedBy = Column(NVARCHAR(100))
    ResolvedTimestamp = Column(DateTime)
    CreatedDate = Column(DateTime, default=datetime.datetime.utcnow)
    UpdatedDate = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

# Example of how to create an engine and tables (typically done in a db_setup script)
# if __name__ == '__main__':
#     # Replace with your actual connection string from config
#     # Ensure you have the correct ODBC driver installed and specified in the connection string
#     engine = create_engine('mssql+pyodbc://user:password@your_dsn_name')
#     Base.metadata.create_all(engine)
#     print("SQL Server tables created (if they didn't exist).")