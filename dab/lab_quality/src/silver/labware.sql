-- Silver layer for LabWare LIMS. Bronze uses UPPER_CASE codes, YYYYMMDD strings and string numerics; silver conforms them.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${labware_silver_schema}`.`02_lab` (
  labId STRING COMMENT 'Canonical laboratory identifier (LAB-01 format).',
  labName STRING COMMENT 'Name of the laboratory in title case.',
  siteName STRING COMMENT 'Site where the laboratory is located.',
  countryCode STRING COMMENT 'ISO 3166-1 alpha-2 country code.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (labId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Laboratories from LabWare with conformed identifiers and title-case names.'
AS SELECT
  concat('LAB-', substring(LAB_CODE, 2, 2)) AS labId,
  initcap(LAB_DESC) AS labName,
  initcap(SITE_NAME) AS siteName,
  COUNTRY_CD AS countryCode,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${labware_bronze_schema}`.`01_labs`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${labware_silver_schema}`.`02_instrument` (
  instrumentId STRING COMMENT 'Canonical instrument identifier (INS-001 format).',
  labId STRING COMMENT 'Canonical identifier of the laboratory housing the instrument.',
  instrumentName STRING COMMENT 'Display name of the instrument.',
  instrumentType STRING COMMENT 'Instrument type, for example HPLC or LC-MS.',
  vendor STRING COMMENT 'Instrument manufacturer.',
  installDate DATE COMMENT 'Date the instrument was installed.',
  status STRING COMMENT 'Active, Maintenance or Retired.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (instrumentId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Laboratory instrument master from LabWare with decoded status.'
AS SELECT
  concat('INS-', substring(INSTR_NO, 3, 3)) AS instrumentId,
  concat('LAB-', substring(LAB_CODE, 2, 2)) AS labId,
  INSTR_NAME AS instrumentName,
  INSTR_TYPE AS instrumentType,
  VENDOR AS vendor,
  try_to_date(INSTALL_DT, 'yyyyMMdd') AS installDate,
  CASE STATUS_CD WHEN 'A' THEN 'Active' WHEN 'M' THEN 'Maintenance' WHEN 'R' THEN 'Retired' ELSE 'Unknown' END AS status,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${labware_bronze_schema}`.`01_instruments`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${labware_silver_schema}`.`02_sample` (
  sampleId STRING COMMENT 'LabWare sample identifier.',
  projectId STRING COMMENT 'Canonical identifier of the project the sample belongs to.',
  labId STRING COMMENT 'Canonical identifier of the receiving laboratory.',
  sampleType STRING COMMENT 'Sample matrix, for example Plasma or Api Batch.',
  receivedDate DATE COMMENT 'Date the sample was received by the laboratory.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (sampleId IS NOT NULL AND receivedDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Samples received by the laboratories from LabWare.'
AS SELECT
  SAMPLE_ID AS sampleId,
  concat('PRJ-', substring(PROJECT_NO, 2, 4)) AS projectId,
  concat('LAB-', substring(LAB_CODE, 2, 2)) AS labId,
  initcap(SAMPLE_TYPE) AS sampleType,
  try_to_date(RECEIVED_DT, 'yyyyMMdd') AS receivedDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${labware_bronze_schema}`.`01_samples`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${labware_silver_schema}`.`02_result` (
  resultId BIGINT COMMENT 'LabWare result identifier.',
  sampleId STRING COMMENT 'Identifier of the tested sample.',
  assayCode STRING COMMENT 'Assay code, for example ASY-POTENCY.',
  instrumentId STRING COMMENT 'Canonical identifier of the instrument used.',
  analystId STRING COMMENT 'Canonical scientist identifier (SCI-0001 format) of the analyst.',
  resultValue DECIMAL(18,4) COMMENT 'Measured value.',
  unit STRING COMMENT 'Unit of the measured value.',
  specLow DECIMAL(18,4) COMMENT 'Lower specification limit.',
  specHigh DECIMAL(18,4) COMMENT 'Upper specification limit.',
  isPass BOOLEAN COMMENT 'True when the result is within specification.',
  completedDate DATE COMMENT 'Date the test was completed.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (resultId IS NOT NULL AND sampleId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Laboratory test results from LabWare with numeric values, boolean pass flag and conformed keys.'
AS SELECT
  CAST(RESULT_ID AS BIGINT) AS resultId,
  SAMPLE_ID AS sampleId,
  ASSAY_CODE AS assayCode,
  concat('INS-', substring(INSTR_NO, 3, 3)) AS instrumentId,
  concat('SCI-', substring(ANALYST_ID, 2, 4)) AS analystId,
  try_cast(RESULT_VAL AS DECIMAL(18,4)) AS resultValue,
  UNIT AS unit,
  try_cast(SPEC_LOW AS DECIMAL(18,4)) AS specLow,
  try_cast(SPEC_HIGH AS DECIMAL(18,4)) AS specHigh,
  PASS_FLAG = 'Y' AS isPass,
  try_to_date(COMPLETED_DT, 'yyyyMMdd') AS completedDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${labware_bronze_schema}`.`01_results`);
