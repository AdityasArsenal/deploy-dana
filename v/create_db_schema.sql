-- ESG Data Schema for Azure SQL Database
-- Optimized for PowerBI analytics

-- Companies table to store basic company information
CREATE TABLE Companies (
    CompanyID INT IDENTITY(1,1) PRIMARY KEY,
    CompanyName NVARCHAR(255) NOT NULL,
    CompanyIdentifier NVARCHAR(50) NOT NULL,
    IdentifierScheme NVARCHAR(255),
    ReportingPeriodStart DATE,
    ReportingPeriodEnd DATE,
    CONSTRAINT UQ_Companies_Identifier UNIQUE (CompanyIdentifier)
);

-- Units table to store measurement units
CREATE TABLE Units (
    UnitID INT IDENTITY(1,1) PRIMARY KEY,
    UnitRef NVARCHAR(50) NOT NULL,
    UnitType NVARCHAR(20) NOT NULL, -- 'measure' or 'divide'
    UnitValue NVARCHAR(100),
    Numerator NVARCHAR(100),
    Denominator NVARCHAR(100),
    CONSTRAINT UQ_Units_UnitRef UNIQUE (UnitRef)
);

-- Contexts table to store reporting contexts
CREATE TABLE Contexts (
    ContextID INT IDENTITY(1,1) PRIMARY KEY,
    ContextRef NVARCHAR(100) NOT NULL,
    EntityIdentifier NVARCHAR(100),
    EntityScheme NVARCHAR(255),
    PeriodType NVARCHAR(20), -- 'instant' or 'duration'
    PeriodStartDate DATE,
    PeriodEndDate DATE,
    PeriodInstantDate DATE,
    ScenarioDimensions NVARCHAR(MAX), -- Store dimensions as JSON
    CONSTRAINT UQ_Contexts_ContextRef UNIQUE (ContextRef)
);

-- KPI Categories table for organizing KPIs
CREATE TABLE KPICategories (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(255),
    CONSTRAINT UQ_KPICategories_CategoryName UNIQUE (CategoryName)
);

-- KPI Definitions table
CREATE TABLE KPIDefinitions (
    KPIID INT IDENTITY(1,1) PRIMARY KEY,
    KPIName NVARCHAR(255) NOT NULL,
    CategoryID INT,
    Description NVARCHAR(MAX),
    DataType NVARCHAR(50), -- 'numeric', 'text', 'boolean', etc.
    CONSTRAINT UQ_KPIDefinitions_KPIName UNIQUE (KPIName),
    CONSTRAINT FK_KPIDefinitions_CategoryID FOREIGN KEY (CategoryID) REFERENCES KPICategories(CategoryID)
);

-- KPI Facts table to store actual KPI values
CREATE TABLE KPIFacts (
    FactID INT IDENTITY(1,1) PRIMARY KEY,
    CompanyID INT NOT NULL,
    KPIID INT NOT NULL,
    ContextID INT NOT NULL,
    UnitID INT,
    RawValue NVARCHAR(MAX),
    NumericValue DECIMAL(28, 10),
    Decimals INT,
    PeriodStart DATE,
    PeriodEnd DATE,
    PeriodInstant DATE,
    ReportingYear INT,
    ReportingQuarter INT,
    CONSTRAINT FK_KPIFacts_CompanyID FOREIGN KEY (CompanyID) REFERENCES Companies(CompanyID),
    CONSTRAINT FK_KPIFacts_KPIID FOREIGN KEY (KPIID) REFERENCES KPIDefinitions(KPIID),
    CONSTRAINT FK_KPIFacts_ContextID FOREIGN KEY (ContextID) REFERENCES Contexts(ContextID),
    CONSTRAINT FK_KPIFacts_UnitID FOREIGN KEY (UnitID) REFERENCES Units(UnitID)
);

-- Create indexes for performance
CREATE INDEX IX_KPIFacts_CompanyID ON KPIFacts(CompanyID);
CREATE INDEX IX_KPIFacts_KPIID ON KPIFacts(KPIID);
CREATE INDEX IX_KPIFacts_PeriodStart_PeriodEnd ON KPIFacts(PeriodStart, PeriodEnd);
CREATE INDEX IX_KPIFacts_PeriodInstant ON KPIFacts(PeriodInstant);
CREATE INDEX IX_KPIFacts_ReportingYear_Quarter ON KPIFacts(ReportingYear, ReportingQuarter);

-- Common ESG Categories - Populate with basic categories
INSERT INTO KPICategories (CategoryName, Description)
VALUES 
('Environmental', 'Environmental metrics including emissions, energy, waste, etc.'),
('Social', 'Social metrics including employee data, community impact, etc.'),
('Governance', 'Governance metrics including board composition, ethics, etc.'),
('Economic', 'Economic and financial performance metrics');

-- Initial set of common Units
INSERT INTO Units (UnitRef, UnitType, UnitValue)
VALUES 
('Pure', 'measure', 'xbrli:pure'),
('Shares', 'measure', 'xbrli:shares'),
('USD', 'measure', 'iso4217:USD'),
('INR', 'measure', 'iso4217:INR'),
('Gigajoule', 'measure', 'Non-SI:GJ');

GO

-- Create view for PowerBI consumption that flattens the data
CREATE VIEW vw_ESG_KPI_Data AS
SELECT 
    c.CompanyName,
    c.CompanyIdentifier,
    c.ReportingPeriodStart AS CompanyReportingPeriodStart,
    c.ReportingPeriodEnd AS CompanyReportingPeriodEnd,
    kd.KPIName,
    kc.CategoryName AS KPICategory,
    kf.RawValue,
    kf.NumericValue,
    COALESCE(kf.PeriodStart, kf.PeriodInstant) AS EffectiveDate,
    kf.PeriodEnd,
    u.UnitRef,
    CASE 
        WHEN u.UnitType = 'measure' THEN u.UnitValue
        WHEN u.UnitType = 'divide' THEN CONCAT(u.Numerator, '/', u.Denominator)
        ELSE NULL
    END AS Unit,
    kf.ReportingYear,
    kf.ReportingQuarter,
    ctx.ScenarioDimensions
FROM 
    KPIFacts kf
JOIN 
    Companies c ON kf.CompanyID = c.CompanyID
JOIN 
    KPIDefinitions kd ON kf.KPIID = kd.KPIID
LEFT JOIN 
    KPICategories kc ON kd.CategoryID = kc.CategoryID
LEFT JOIN 
    Units u ON kf.UnitID = u.UnitID
JOIN 
    Contexts ctx ON kf.ContextID = ctx.ContextID; 