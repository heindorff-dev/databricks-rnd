-- Silver layer for Medidata Rave EDC. Bronze uses CDISC abbreviations, dd-MMM-yyyy dates and Y/N flags; silver conforms them.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${medidata_rave_silver_schema}`.`02_trial` (
  trialId STRING COMMENT 'Study identifier, for example RD-ST-001.',
  trialTitle STRING COMMENT 'Protocol title of the trial.',
  phase STRING COMMENT 'Clinical phase in title case, for example Phase Ii.',
  programId STRING COMMENT 'Canonical identifier of the program the trial belongs to.',
  startDate DATE COMMENT 'Date the trial started.',
  trialStatus STRING COMMENT 'Ongoing, Recruiting or Completed.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (trialId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Clinical trials from Medidata Rave with conformed program identifiers and parsed start dates.'
AS SELECT
  STUDYID AS trialId,
  STUDYTITLE AS trialTitle,
  initcap(PHASE) AS phase,
  concat('PG-', substring(PRGCODE, 3, 3)) AS programId,
  try_to_date(STARTDT, 'dd-MMM-yyyy') AS startDate,
  initcap(STUDYSTATUS) AS trialStatus,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${medidata_rave_bronze_schema}`.`01_studies`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${medidata_rave_silver_schema}`.`02_site` (
  siteId STRING COMMENT 'Site identifier.',
  trialId STRING COMMENT 'Identifier of the trial the site participates in.',
  siteName STRING COMMENT 'Name of the clinical site.',
  countryCode STRING COMMENT 'ISO 3166-1 alpha-2 country code of the site.',
  principalInvestigatorName STRING COMMENT 'Name of the principal investigator.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (siteId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Clinical sites per trial from Medidata Rave. Contains the principal investigator name (personal data).'
AS SELECT
  SITEID AS siteId,
  STUDYID AS trialId,
  SITENAME AS siteName,
  COUNTRY AS countryCode,
  PINAME AS principalInvestigatorName,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${medidata_rave_bronze_schema}`.`01_sites`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${medidata_rave_silver_schema}`.`02_subject` (
  subjectId STRING COMMENT 'Subject identifier unique across trials (study id and subject number).',
  trialId STRING COMMENT 'Identifier of the trial.',
  siteId STRING COMMENT 'Identifier of the enrolling site.',
  birthYear INT COMMENT 'Year of birth of the subject.',
  sex STRING COMMENT 'Sex of the subject: Male or Female.',
  enrollmentDate DATE COMMENT 'Date the subject was enrolled (reference start date).',
  armName STRING COMMENT 'Treatment arm: Placebo or Treatment.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (subjectId IS NOT NULL AND enrollmentDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Trial subjects (demographics) from Medidata Rave. Subject-level personal data.'
AS SELECT
  concat(STUDYID, '-', SUBJID) AS subjectId,
  STUDYID AS trialId,
  SITEID AS siteId,
  CAST(BRTHYR AS INT) AS birthYear,
  CASE SEX WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' ELSE 'Unknown' END AS sex,
  try_to_date(RFSTDTC, 'yyyy-MM-dd') AS enrollmentDate,
  initcap(ARM) AS armName,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${medidata_rave_bronze_schema}`.`01_subjects`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${medidata_rave_silver_schema}`.`02_adverse_event` (
  adverseEventId STRING COMMENT 'Adverse event identifier (subject id and sequence number).',
  subjectId STRING COMMENT 'Identifier of the subject the event was reported for.',
  trialId STRING COMMENT 'Identifier of the trial.',
  eventTerm STRING COMMENT 'Reported adverse event term in title case.',
  severity STRING COMMENT 'Mild, Moderate or Severe.',
  isSerious BOOLEAN COMMENT 'True when the event is classified as a serious adverse event.',
  onsetDate DATE COMMENT 'Date the event started.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (adverseEventId IS NOT NULL AND subjectId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Adverse events per subject from Medidata Rave. Subject-level personal data.'
AS SELECT
  concat(STUDYID, '-', SUBJID, '-', CAST(AESEQ AS STRING)) AS adverseEventId,
  concat(STUDYID, '-', SUBJID) AS subjectId,
  STUDYID AS trialId,
  initcap(AETERM) AS eventTerm,
  initcap(AESEV) AS severity,
  AESER = 'Y' AS isSerious,
  try_to_date(AESTDTC, 'yyyy-MM-dd') AS onsetDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${medidata_rave_bronze_schema}`.`01_adverse_events`);
