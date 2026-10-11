-- Databricks notebook source
-- Column descriptions owned by the regulatory_affairs bundle. No personal data in this product.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:regulatory_affairs_gold_schema);

COMMENT ON COLUMN `03_fact_submissions`.submissionId IS 'Vault submission record identifier.';
COMMENT ON COLUMN `03_fact_submissions`.submissionName IS 'Submission reference.';
COMMENT ON COLUMN `03_fact_submissions`.programId IS 'Identifier of the related program.';
COMMENT ON COLUMN `03_fact_submissions`.submissionType IS 'IND, NDA, MAA, CTA or Amendment.';
COMMENT ON COLUMN `03_fact_submissions`.agency IS 'Health authority.';
COMMENT ON COLUMN `03_fact_submissions`.status IS 'Under Review, Approved or Rejected.';
COMMENT ON COLUMN `03_fact_submissions`.submittedDate IS 'Date the submission was filed.';
COMMENT ON COLUMN `03_fact_submissions`.decisionDate IS 'Date of the agency decision.';
COMMENT ON COLUMN `03_fact_submissions`.cycleTimeDays IS 'Days from submission to decision.';
COMMENT ON COLUMN `03_fact_submissions`.queryCount IS 'Number of agency queries received.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_approval_cycle_time`.agency IS 'Health authority.';
COMMENT ON COLUMN `03_agg_approval_cycle_time`.submissionType IS 'Submission type.';
COMMENT ON COLUMN `03_agg_approval_cycle_time`.submissionCount IS 'Number of submissions.';
COMMENT ON COLUMN `03_agg_approval_cycle_time`.decidedCount IS 'Submissions with an agency decision.';
COMMENT ON COLUMN `03_agg_approval_cycle_time`.approvedCount IS 'Approved submissions.';
COMMENT ON COLUMN `03_agg_approval_cycle_time`.approvalRate IS 'Approved share of decided submissions (0-1).';
COMMENT ON COLUMN `03_agg_approval_cycle_time`.avgCycleTimeDays IS 'Average days from submission to decision.';
COMMENT ON COLUMN `03_agg_approval_cycle_time`.medianCycleTimeDays IS 'Median days from submission to decision.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_document_compliance`.programId IS 'Identifier of the related program.';
COMMENT ON COLUMN `03_agg_document_compliance`.documentType IS 'Document type.';
COMMENT ON COLUMN `03_agg_document_compliance`.documentCount IS 'Number of documents.';
COMMENT ON COLUMN `03_agg_document_compliance`.validCount IS 'Approved documents that have not expired.';
COMMENT ON COLUMN `03_agg_document_compliance`.expiredCount IS 'Expired documents.';
COMMENT ON COLUMN `03_agg_document_compliance`.expiringWithin90DaysCount IS 'Documents expiring within 90 days of the refresh.';
COMMENT ON COLUMN `03_agg_document_compliance`.complianceRate IS 'Valid share of all documents (0-1).';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_agency_query_response`.agency IS 'Health authority.';
COMMENT ON COLUMN `03_agg_agency_query_response`.monthStart IS 'First day of the month the queries were received.';
COMMENT ON COLUMN `03_agg_agency_query_response`.queryCategory IS 'Query category.';
COMMENT ON COLUMN `03_agg_agency_query_response`.queryCount IS 'Number of queries received.';
COMMENT ON COLUMN `03_agg_agency_query_response`.openCount IS 'Queries without a response.';
COMMENT ON COLUMN `03_agg_agency_query_response`.avgResponseDays IS 'Average days from receipt to response.';
COMMENT ON COLUMN `03_agg_agency_query_response`.lateCount IS 'Queries answered after the due date.';
