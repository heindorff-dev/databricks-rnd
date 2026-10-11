-- Gold data product: Regulatory affairs. Materialized views over the Veeva Vault silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${regulatory_affairs_gold_schema}`.`03_fact_submissions`
COMMENT 'Submission fact: one row per regulatory submission with agency decision, cycle time and number of agency queries.'
AS
SELECT
  s.submissionId,
  s.submissionName,
  s.programId,
  s.submissionType,
  s.agency,
  s.status,
  s.submittedDate,
  s.decisionDate,
  datediff(s.decisionDate, s.submittedDate) AS cycleTimeDays,
  coalesce(q.queryCount, 0) AS queryCount
FROM `${catalog}`.`${veeva_vault_silver_schema}`.`02_submission` s
LEFT JOIN (
  SELECT submissionId, count(*) AS queryCount
  FROM `${catalog}`.`${veeva_vault_silver_schema}`.`02_agency_query`
  GROUP BY submissionId
) q ON q.submissionId = s.submissionId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${regulatory_affairs_gold_schema}`.`03_agg_approval_cycle_time`
COMMENT 'Approval cycle time and approval rate per agency and submission type.'
AS
SELECT
  agency,
  submissionType,
  count(*) AS submissionCount,
  count_if(decisionDate IS NOT NULL) AS decidedCount,
  count_if(status = 'Approved') AS approvedCount,
  round(count_if(status = 'Approved') / nullif(count_if(decisionDate IS NOT NULL), 0), 4) AS approvalRate,
  round(avg(cycleTimeDays), 1) AS avgCycleTimeDays,
  percentile_approx(cycleTimeDays, 0.5) AS medianCycleTimeDays
FROM `${catalog}`.`${regulatory_affairs_gold_schema}`.`03_fact_submissions`
GROUP BY agency, submissionType;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${regulatory_affairs_gold_schema}`.`03_agg_document_compliance`
COMMENT 'Document compliance per program and document type: approved, expired and soon-to-expire documents.'
AS
SELECT
  programId,
  documentType,
  count(*) AS documentCount,
  count_if(status = 'Approved' AND (expirationDate IS NULL OR expirationDate >= current_date())) AS validCount,
  count_if(status = 'Expired' OR expirationDate < current_date()) AS expiredCount,
  count_if(expirationDate BETWEEN current_date() AND date_add(current_date(), 90)) AS expiringWithin90DaysCount,
  round(count_if(status = 'Approved' AND (expirationDate IS NULL OR expirationDate >= current_date())) / count(*), 4) AS complianceRate
FROM `${catalog}`.`${veeva_vault_silver_schema}`.`02_document`
GROUP BY programId, documentType;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${regulatory_affairs_gold_schema}`.`03_agg_agency_query_response`
COMMENT 'Agency query response performance per agency and month: response time and share answered after the due date.'
AS
SELECT
  s.agency,
  trunc(q.receivedDate, 'MM') AS monthStart,
  q.queryCategory,
  count(*) AS queryCount,
  count_if(q.respondedDate IS NULL) AS openCount,
  round(avg(datediff(q.respondedDate, q.receivedDate)), 1) AS avgResponseDays,
  count_if(q.respondedDate > q.dueDate) AS lateCount
FROM `${catalog}`.`${veeva_vault_silver_schema}`.`02_agency_query` q
JOIN `${catalog}`.`${veeva_vault_silver_schema}`.`02_submission` s ON s.submissionId = q.submissionId
GROUP BY s.agency, trunc(q.receivedDate, 'MM'), q.queryCategory;
