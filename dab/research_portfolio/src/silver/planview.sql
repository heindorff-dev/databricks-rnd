-- Silver layer for Planview (portfolio, budget, resources). Streaming tables read from the bronze Delta tables.
-- Bronze uses PascalCase names and fiscal period integers; silver conforms them to typed camelCase columns and canonical keys.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${planview_silver_schema}`.`02_program` (
  programId STRING COMMENT 'Canonical program identifier (PG-001 format).',
  programName STRING COMMENT 'Name of the research program.',
  therapeuticArea STRING COMMENT 'Therapeutic area the program belongs to.',
  developmentPhase STRING COMMENT 'Current development phase of the program (Discovery, Preclinical, Phase 1, 2 or 3).',
  programManagerName STRING COMMENT 'Name of the program owner as First Last.',
  programStatus STRING COMMENT 'Portfolio status of the program (Active or On Hold).',
  startDate DATE COMMENT 'Date the program started.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (programId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Research programs from Planview, one row per program, with typed dates and a conformed program identifier.'
AS SELECT
  ProgramCode AS programId,
  ProgramName AS programName,
  TherapeuticArea AS therapeuticArea,
  Phase AS developmentPhase,
  concat_ws(' ', trim(split(ProgramOwner, ',')[1]), trim(split(ProgramOwner, ',')[0])) AS programManagerName,
  StatusDesc AS programStatus,
  try_to_date(StartDt, 'yyyy-MM-dd') AS startDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${planview_bronze_schema}`.`01_programs`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${planview_silver_schema}`.`02_project` (
  projectId STRING COMMENT 'Canonical project identifier (PRJ-0001 format).',
  programId STRING COMMENT 'Canonical identifier of the program the project belongs to.',
  projectName STRING COMMENT 'Title of the project.',
  projectType STRING COMMENT 'Type of the project, for example Lead Optimization or Clinical Study.',
  projectStatus STRING COMMENT 'Status of the project (Planned, Active, On Hold, At Risk, Completed or Cancelled).',
  startDate DATE COMMENT 'Planned or actual start date of the project.',
  plannedEndDate DATE COMMENT 'Planned finish date of the project.',
  actualEndDate DATE COMMENT 'Actual finish date. Null while the project is not completed or cancelled.',
  projectManagerName STRING COMMENT 'Name of the project manager as First Last.',
  priority STRING COMMENT 'Priority of the project (Critical, High, Medium or Low).',
  plannedBudgetEur DECIMAL(18,2) COMMENT 'Total approved budget of the project in EUR.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (projectId IS NOT NULL AND programId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Research projects from Planview, one row per project, with typed dates and total budget in EUR.'
AS SELECT
  ProjectCode AS projectId,
  ProgramCode AS programId,
  ProjectTitle AS projectName,
  ProjectType AS projectType,
  ProjectStatus AS projectStatus,
  CAST(StartDate AS DATE) AS startDate,
  CAST(PlannedFinish AS DATE) AS plannedEndDate,
  CAST(ActualFinish AS DATE) AS actualEndDate,
  concat_ws(' ', trim(split(ProjectManager, ',')[1]), trim(split(ProjectManager, ',')[0])) AS projectManagerName,
  PriorityLevel AS priority,
  CAST(TotalBudgetEUR AS DECIMAL(18,2)) AS plannedBudgetEur,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${planview_bronze_schema}`.`01_projects`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${planview_silver_schema}`.`02_milestone` (
  milestoneId BIGINT COMMENT 'Planview milestone identifier.',
  projectId STRING COMMENT 'Canonical identifier of the project the milestone belongs to.',
  milestoneName STRING COMMENT 'Name of the milestone, for example Data Readout.',
  plannedDate DATE COMMENT 'Date the milestone was planned for.',
  actualDate DATE COMMENT 'Date the milestone was achieved. Null while open.',
  milestoneStatus STRING COMMENT 'Achieved or Open.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (milestoneId IS NOT NULL AND projectId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Project milestones from Planview with planned and actual dates.'
AS SELECT
  CAST(MilestoneId AS BIGINT) AS milestoneId,
  ProjectCode AS projectId,
  MilestoneName AS milestoneName,
  CAST(PlannedDate AS DATE) AS plannedDate,
  CAST(ActualDate AS DATE) AS actualDate,
  MilestoneStatus AS milestoneStatus,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${planview_bronze_schema}`.`01_milestones`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${planview_silver_schema}`.`02_budget_line` (
  budgetLineId STRING COMMENT 'Planview budget line identifier.',
  projectId STRING COMMENT 'Canonical identifier of the project the budget line belongs to.',
  fiscalYear INT COMMENT 'Fiscal year of the budget line.',
  fiscalMonth INT COMMENT 'Fiscal month (1-12) of the budget line.',
  periodStartDate DATE COMMENT 'First day of the fiscal period.',
  costCategory STRING COMMENT 'Cost category: Personnel, Materials, CRO Services, Equipment or Travel.',
  plannedAmountEur DECIMAL(18,2) COMMENT 'Planned amount in EUR for the period.',
  actualAmountEur DECIMAL(18,2) COMMENT 'Actual amount in EUR for the period.',
  forecastAmountEur DECIMAL(18,2) COMMENT 'Forecast amount in EUR for the period.',
  localCurrency STRING COMMENT 'ISO currency code of the local-currency amount.',
  actualAmountLocal DECIMAL(18,2) COMMENT 'Actual amount in the local currency of the performing site.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (budgetLineId IS NOT NULL AND projectId IS NOT NULL AND periodStartDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Monthly budget lines per project and cost category from Planview, with the fiscal period converted to a date.'
AS SELECT
  BudgetLineId AS budgetLineId,
  ProjectCode AS projectId,
  CAST(FiscalPeriod DIV 100 AS INT) AS fiscalYear,
  CAST(pmod(FiscalPeriod, 100) AS INT) AS fiscalMonth,
  try_to_date(concat(CAST(FiscalPeriod AS STRING), '01'), 'yyyyMMdd') AS periodStartDate,
  CostCategory AS costCategory,
  CAST(PlannedEUR AS DECIMAL(18,2)) AS plannedAmountEur,
  CAST(ActualEUR AS DECIMAL(18,2)) AS actualAmountEur,
  CAST(ForecastEUR AS DECIMAL(18,2)) AS forecastAmountEur,
  LocalCurrency AS localCurrency,
  CAST(ActualLocal AS DECIMAL(18,2)) AS actualAmountLocal,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${planview_bronze_schema}`.`01_budget_lines`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${planview_silver_schema}`.`02_resource_allocation` (
  allocationId BIGINT COMMENT 'Planview allocation identifier.',
  projectId STRING COMMENT 'Canonical identifier of the project the scientist is allocated to.',
  scientistId STRING COMMENT 'Canonical scientist identifier (SCI-0001 format).',
  periodStartDate DATE COMMENT 'First day of the fiscal period.',
  allocatedFte DECIMAL(5,2) COMMENT 'Planned full-time equivalents allocated in the period.',
  actualFte DECIMAL(5,2) COMMENT 'Actual full-time equivalents worked in the period.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (allocationId IS NOT NULL AND projectId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Monthly planned versus actual scientist effort per project from Planview.'
AS SELECT
  CAST(AllocId AS BIGINT) AS allocationId,
  ProjectCode AS projectId,
  concat('SCI-', substring(ResourceId, 3, 4)) AS scientistId,
  try_to_date(concat(CAST(FiscalPeriod AS STRING), '01'), 'yyyyMMdd') AS periodStartDate,
  CAST(AllocFTE AS DECIMAL(5,2)) AS allocatedFte,
  CAST(ActualFTE AS DECIMAL(5,2)) AS actualFte,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${planview_bronze_schema}`.`01_resource_allocations`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${planview_silver_schema}`.`02_risk` (
  riskId BIGINT COMMENT 'Planview risk identifier.',
  projectId STRING COMMENT 'Canonical identifier of the project the risk belongs to.',
  riskTitle STRING COMMENT 'Short title of the risk.',
  probabilityScore INT COMMENT 'Likelihood of the risk on a scale of 1 (low) to 5 (high).',
  impactScore INT COMMENT 'Impact of the risk on a scale of 1 (low) to 5 (high).',
  riskScore INT COMMENT 'Probability multiplied by impact (1-25).',
  riskStatus STRING COMMENT 'Open, Mitigated or Closed.',
  raisedDate DATE COMMENT 'Date the risk was raised.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (riskId IS NOT NULL AND projectId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Project risk register from Planview with a derived risk score.'
AS SELECT
  CAST(RiskId AS BIGINT) AS riskId,
  ProjectCode AS projectId,
  RiskTitle AS riskTitle,
  CAST(Probability AS INT) AS probabilityScore,
  CAST(Impact AS INT) AS impactScore,
  CAST(Probability * Impact AS INT) AS riskScore,
  RiskStatus AS riskStatus,
  CAST(RaisedDate AS DATE) AS raisedDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${planview_bronze_schema}`.`01_risks`);
