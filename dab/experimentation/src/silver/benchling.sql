-- Silver layer for Benchling ELN. Bronze uses snake_case, UUIDs, ISO strings and JSON; silver conforms them.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${benchling_silver_schema}`.`02_scientist` (
  scientistId STRING COMMENT 'Canonical scientist identifier (SCI-0001 format).',
  displayName STRING COMMENT 'Full name of the scientist.',
  email STRING COMMENT 'Work email address of the scientist.',
  labId STRING COMMENT 'Canonical identifier of the scientist home laboratory.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (scientistId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Scientists registered in Benchling. Contains personal data (name, email).'
AS SELECT
  concat('SCI-', substring(id, 5, 4)) AS scientistId,
  display_name AS displayName,
  email,
  concat('LAB-', substring(lab_ref, 5, 2)) AS labId,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${benchling_bronze_schema}`.`01_users`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${benchling_silver_schema}`.`02_compound` (
  compoundId STRING COMMENT 'Compound registry identifier, for example CMP-000123.',
  compoundName STRING COMMENT 'Name of the compound.',
  molecularWeight DECIMAL(10,3) COMMENT 'Molecular weight in g/mol.',
  programId STRING COMMENT 'Canonical identifier of the program the compound belongs to.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (compoundId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Registered compounds from Benchling.'
AS SELECT
  registry_id AS compoundId,
  name AS compoundName,
  CAST(molecular_weight AS DECIMAL(10,3)) AS molecularWeight,
  concat('PG-', substring(program_ref, 4, 3)) AS programId,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${benchling_bronze_schema}`.`01_compounds`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${benchling_silver_schema}`.`02_experiment` (
  experimentId STRING COMMENT 'Benchling experiment UUID.',
  projectId STRING COMMENT 'Canonical identifier of the project the experiment belongs to.',
  experimentName STRING COMMENT 'Experiment name.',
  scientistId STRING COMMENT 'Canonical identifier of the scientist who created the experiment.',
  status STRING COMMENT 'Planned, In Progress, Completed or Failed.',
  assayType STRING COMMENT 'Assay type taken from the custom fields, for example binding_assay.',
  replicateCount INT COMMENT 'Number of replicates, taken from the custom fields.',
  createdAt TIMESTAMP COMMENT 'Timestamp when the experiment was created.',
  completedAt TIMESTAMP COMMENT 'Timestamp when the experiment finished. Null while not finished.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (experimentId IS NOT NULL AND createdAt IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Experiments from Benchling with the JSON custom fields parsed and timestamps converted.'
AS SELECT
  id AS experimentId,
  concat('PRJ-', substring(project_ref, 5, 4)) AS projectId,
  name AS experimentName,
  concat('SCI-', substring(creator_ref, 5, 4)) AS scientistId,
  initcap(replace(status, '_', ' ')) AS status,
  get_json_object(fields_json, '$.assay_type') AS assayType,
  try_cast(get_json_object(fields_json, '$.replicates') AS INT) AS replicateCount,
  try_to_timestamp(created_at) AS createdAt,
  try_to_timestamp(completed_at) AS completedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${benchling_bronze_schema}`.`01_experiments`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${benchling_silver_schema}`.`02_experiment_result` (
  resultId STRING COMMENT 'Benchling result UUID.',
  experimentId STRING COMMENT 'Identifier of the experiment.',
  compoundId STRING COMMENT 'Registry identifier of the tested compound.',
  outcome STRING COMMENT 'Success, Partial or Failure.',
  measuredValue DECIMAL(18,4) COMMENT 'Measured value of the experiment.',
  measuredAt TIMESTAMP COMMENT 'Timestamp of the measurement.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (resultId IS NOT NULL AND experimentId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Experiment results per compound from Benchling.'
AS SELECT
  id AS resultId,
  experiment_id AS experimentId,
  compound_registry_id AS compoundId,
  initcap(outcome) AS outcome,
  CAST(measured_value AS DECIMAL(18,4)) AS measuredValue,
  try_to_timestamp(measured_at) AS measuredAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${benchling_bronze_schema}`.`01_experiment_results`);
