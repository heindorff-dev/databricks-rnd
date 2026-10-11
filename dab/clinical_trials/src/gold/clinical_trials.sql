-- Gold data product: Clinical trials. Materialized views over the Medidata Rave silver tables and the Planview program context.
-- Reads planview silver owned by research_portfolio, so run rnd_research_portfolio first.
-- Subject-level data is not exposed in gold; only aggregates and investigator names.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${clinical_trials_gold_schema}`.`03_dim_trial`
COMMENT 'Trial dimension: one row per clinical trial with program, phase, status and site and subject counts.'
AS
SELECT
  t.trialId,
  t.trialTitle,
  t.phase,
  t.trialStatus,
  t.startDate,
  t.programId,
  g.programName,
  g.therapeuticArea,
  count(DISTINCT s.siteId) AS siteCount,
  count(DISTINCT sub.subjectId) AS subjectCount
FROM `${catalog}`.`${medidata_rave_silver_schema}`.`02_trial` t
LEFT JOIN `${catalog}`.`${planview_silver_schema}`.`02_program` g ON g.programId = t.programId
LEFT JOIN `${catalog}`.`${medidata_rave_silver_schema}`.`02_site` s ON s.trialId = t.trialId
LEFT JOIN `${catalog}`.`${medidata_rave_silver_schema}`.`02_subject` sub ON sub.trialId = t.trialId
GROUP BY t.trialId, t.trialTitle, t.phase, t.trialStatus, t.startDate, t.programId, g.programName, g.therapeuticArea;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${clinical_trials_gold_schema}`.`03_fact_enrollment_monthly`
COMMENT 'Monthly enrollment fact: subjects enrolled per trial, site and month, split by treatment arm, with cumulative enrollment per trial.'
AS
WITH monthly AS (
  SELECT
    trialId,
    siteId,
    trunc(enrollmentDate, 'MM') AS monthStart,
    count(*) AS subjectsEnrolled,
    count_if(armName = 'Treatment') AS treatmentSubjects,
    count_if(armName = 'Placebo') AS placeboSubjects
  FROM `${catalog}`.`${medidata_rave_silver_schema}`.`02_subject`
  GROUP BY trialId, siteId, trunc(enrollmentDate, 'MM')
)
SELECT
  m.*,
  sum(subjectsEnrolled) OVER (PARTITION BY trialId ORDER BY monthStart ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulativeTrialEnrollment
FROM monthly m;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${clinical_trials_gold_schema}`.`03_agg_site_performance`
COMMENT 'Site performance per site: enrolled subjects, adverse event rate and serious adverse events. Contains the principal investigator name (personal data).'
AS
WITH enrolled AS (
  SELECT siteId, count(*) AS subjectsEnrolled, min(enrollmentDate) AS firstEnrollmentDate
  FROM `${catalog}`.`${medidata_rave_silver_schema}`.`02_subject`
  GROUP BY siteId
),
events AS (
  SELECT sub.siteId, count(*) AS adverseEvents, count_if(ae.isSerious) AS seriousEvents
  FROM `${catalog}`.`${medidata_rave_silver_schema}`.`02_adverse_event` ae
  JOIN `${catalog}`.`${medidata_rave_silver_schema}`.`02_subject` sub ON sub.subjectId = ae.subjectId
  GROUP BY sub.siteId
)
SELECT
  s.siteId,
  s.trialId,
  s.siteName,
  s.countryCode,
  s.principalInvestigatorName,
  coalesce(e.subjectsEnrolled, 0) AS subjectsEnrolled,
  e.firstEnrollmentDate,
  coalesce(v.adverseEvents, 0) AS adverseEvents,
  coalesce(v.seriousEvents, 0) AS seriousEvents,
  round(coalesce(v.adverseEvents, 0) / nullif(e.subjectsEnrolled, 0), 3) AS adverseEventsPerSubject
FROM `${catalog}`.`${medidata_rave_silver_schema}`.`02_site` s
LEFT JOIN enrolled e ON e.siteId = s.siteId
LEFT JOIN events v ON v.siteId = s.siteId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${clinical_trials_gold_schema}`.`03_agg_adverse_events`
COMMENT 'Adverse events per trial, event term and severity: event counts, serious events and affected subjects.'
AS
SELECT
  trialId,
  eventTerm,
  severity,
  count(*) AS eventCount,
  count_if(isSerious) AS seriousEventCount,
  count(DISTINCT subjectId) AS affectedSubjects
FROM `${catalog}`.`${medidata_rave_silver_schema}`.`02_adverse_event`
GROUP BY trialId, eventTerm, severity;
