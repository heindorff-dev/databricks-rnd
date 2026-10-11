-- Silver layer for Veeva Vault. Bronze uses __v/__c field names and comma-separated picklists; silver conforms them.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${veeva_vault_silver_schema}`.`02_document` (
  documentId STRING COMMENT 'Vault document record identifier (15 characters).',
  documentName STRING COMMENT 'Document title including version.',
  documentType STRING COMMENT 'Document type: Submission, Protocol, Investigator Brochure or SOP.',
  status STRING COMMENT 'Lifecycle state: Draft, In Review, Approved or Expired.',
  programId STRING COMMENT 'Canonical identifier of the related program.',
  createdAt TIMESTAMP COMMENT 'Timestamp when the document was created.',
  effectiveDate DATE COMMENT 'Date the document became effective.',
  expirationDate DATE COMMENT 'Date the document expires. Null when it does not expire.',
  targetCountries ARRAY<STRING> COMMENT 'ISO country codes the document applies to.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (documentId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Regulatory documents from Veeva Vault with parsed dates and the country picklist split into an array.'
AS SELECT
  id AS documentId,
  name__v AS documentName,
  type__v AS documentType,
  status__v AS status,
  concat('PG-', substring(program_code__c, 3, 3)) AS programId,
  try_to_timestamp(created_date__v) AS createdAt,
  try_to_date(effective_date__v, 'yyyy-MM-dd') AS effectiveDate,
  try_to_date(expiration_date__v, 'yyyy-MM-dd') AS expirationDate,
  split(countries__c, ',') AS targetCountries,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${veeva_vault_bronze_schema}`.`01_documents`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${veeva_vault_silver_schema}`.`02_submission` (
  submissionId STRING COMMENT 'Vault submission record identifier (15 characters).',
  submissionName STRING COMMENT 'Submission reference, for example SUB-00001.',
  programId STRING COMMENT 'Canonical identifier of the related program.',
  submissionType STRING COMMENT 'IND, NDA, MAA, CTA or Amendment.',
  agency STRING COMMENT 'Health authority the submission was sent to.',
  submittedDate DATE COMMENT 'Date the submission was filed.',
  decisionDate DATE COMMENT 'Date of the agency decision. Null while under review.',
  status STRING COMMENT 'Under Review, Approved or Rejected.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (submissionId IS NOT NULL AND submittedDate IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Regulatory submissions to health authorities from Veeva Vault.'
AS SELECT
  id AS submissionId,
  name__v AS submissionName,
  concat('PG-', substring(program_code__c, 3, 3)) AS programId,
  submission_type__c AS submissionType,
  agency__c AS agency,
  try_to_date(submitted_date__c, 'yyyy-MM-dd') AS submittedDate,
  try_to_date(decision_date__c, 'yyyy-MM-dd') AS decisionDate,
  status__c AS status,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${veeva_vault_bronze_schema}`.`01_submissions`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${veeva_vault_silver_schema}`.`02_agency_query` (
  queryId STRING COMMENT 'Vault agency query record identifier (15 characters).',
  submissionId STRING COMMENT 'Identifier of the submission the query relates to.',
  queryCategory STRING COMMENT 'Query category: CMC, Clinical, Non-clinical, Labeling or Quality.',
  receivedDate DATE COMMENT 'Date the query was received.',
  dueDate DATE COMMENT 'Date the response is due.',
  respondedDate DATE COMMENT 'Date the response was sent. Null while open.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (queryId IS NOT NULL AND submissionId IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Health authority queries per submission from Veeva Vault.'
AS SELECT
  id AS queryId,
  submission_id__c AS submissionId,
  query_category__c AS queryCategory,
  try_to_date(received_date__c, 'yyyy-MM-dd') AS receivedDate,
  try_to_date(due_date__c, 'yyyy-MM-dd') AS dueDate,
  try_to_date(responded_date__c, 'yyyy-MM-dd') AS respondedDate,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${veeva_vault_bronze_schema}`.`01_agency_queries`);
