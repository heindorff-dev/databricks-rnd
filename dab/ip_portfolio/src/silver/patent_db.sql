-- Silver layer for the patent database. Bronze has mixed-case names, status codes and mixed date formats.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${patent_db_silver_schema}`.`02_patent_family` (
  familyId STRING COMMENT 'Invention family identifier.',
  title STRING COMMENT 'Invention title.',
  programId STRING COMMENT 'Canonical identifier of the related program.',
  techCategory STRING COMMENT 'Technology category: Compound, Formulation, Method or Device.',
  earliestFilingDate DATE COMMENT 'Earliest filing (priority) date in the family.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (familyId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Invention families from the patent database.'
AS SELECT
  FamilyNo AS familyId,
  Title AS title,
  concat('PG-', substring(program_ref, 4, 3)) AS programId,
  TechCategory AS techCategory,
  try_to_date(EarliestFilingDate, 'MM/dd/yyyy') AS earliestFilingDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${patent_db_bronze_schema}`.`01_patent_families`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${patent_db_silver_schema}`.`02_patent` (
  patentId STRING COMMENT 'Patent application identifier.',
  familyId STRING COMMENT 'Identifier of the invention family.',
  jurisdiction STRING COMMENT 'Filing jurisdiction code, for example US or EP.',
  applicationNumber STRING COMMENT 'Application number assigned by the patent office.',
  filingDate DATE COMMENT 'Date the application was filed.',
  grantDate DATE COMMENT 'Date the patent was granted. Null when not granted.',
  legalStatus STRING COMMENT 'Legal status: Granted, Pending, Abandoned, Lapsed or Withdrawn.',
  claimCount INT COMMENT 'Number of claims.',
  annualFeeUsd DECIMAL(12,2) COMMENT 'Annual maintenance fee in USD.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (patentId IS NOT NULL AND filingDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Patent applications and grants per jurisdiction with decoded legal status.'
AS SELECT
  PatentID AS patentId,
  FamilyNo AS familyId,
  Jurisdiction AS jurisdiction,
  ApplicationNumber AS applicationNumber,
  try_to_date(FilingDate, 'yyyyMMdd') AS filingDate,
  try_to_date(GrantDate, 'dd.MM.yyyy') AS grantDate,
  CASE LegalStatus
    WHEN 'G' THEN 'Granted' WHEN 'P' THEN 'Pending' WHEN 'A' THEN 'Abandoned'
    WHEN 'L' THEN 'Lapsed' WHEN 'W' THEN 'Withdrawn' ELSE 'Unknown'
  END AS legalStatus,
  try_cast(ClaimCount AS INT) AS claimCount,
  try_cast(AnnualFeeUSD AS DECIMAL(12,2)) AS annualFeeUsd,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${patent_db_bronze_schema}`.`01_patents`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${patent_db_silver_schema}`.`02_inventor` (
  inventorId STRING COMMENT 'Inventor identifier.',
  inventorName STRING COMMENT 'Inventor full name in First Last order. Personal data.',
  patentId STRING COMMENT 'Identifier of the patent the inventor is named on.',
  role STRING COMMENT 'Lead or Co-inventor.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (inventorId IS NOT NULL AND patentId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Inventors named on patents. Names are personal data.'
AS SELECT
  InventorID AS inventorId,
  concat(trim(split(InventorName, ',')[1]), ' ', trim(split(InventorName, ',')[0])) AS inventorName,
  PatentID AS patentId,
  Role AS role,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${patent_db_bronze_schema}`.`01_inventors`);
