-- Gold data product: Experimentation. Materialized views over the Benchling silver tables and the Planview program context.
-- Reads planview silver owned by research_portfolio, so run rnd_research_portfolio first.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${experimentation_gold_schema}`.`03_dim_experiment`
COMMENT 'Experiment dimension: one row per experiment with project, program, assay type, status and duration. Contains the scientist identifier (personal data).'
AS
SELECT
  e.experimentId,
  e.experimentName,
  e.projectId,
  p.programId,
  g.programName,
  e.assayType,
  e.replicateCount,
  e.status,
  e.scientistId,
  e.createdAt,
  e.completedAt,
  datediff(e.completedAt, e.createdAt) AS durationDays
FROM `${catalog}`.`${benchling_silver_schema}`.`02_experiment` e
LEFT JOIN `${catalog}`.`${planview_silver_schema}`.`02_project` p ON p.projectId = e.projectId
LEFT JOIN `${catalog}`.`${planview_silver_schema}`.`02_program` g ON g.programId = p.programId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${experimentation_gold_schema}`.`03_fact_experiment_outcomes`
COMMENT 'Experiment outcome fact: one row per result with experiment, program and compound context.'
AS
SELECT
  r.resultId,
  r.experimentId,
  e.projectId,
  e.programId,
  e.programName,
  e.assayType,
  r.compoundId,
  c.compoundName,
  r.outcome,
  r.outcome = 'Success' AS isSuccess,
  r.measuredValue,
  r.measuredAt
FROM `${catalog}`.`${benchling_silver_schema}`.`02_experiment_result` r
JOIN `${catalog}`.`${experimentation_gold_schema}`.`03_dim_experiment` e ON e.experimentId = r.experimentId
LEFT JOIN `${catalog}`.`${benchling_silver_schema}`.`02_compound` c ON c.compoundId = r.compoundId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${experimentation_gold_schema}`.`03_agg_success_rate_by_program`
COMMENT 'Experiment success rate per program and assay type.'
AS
SELECT
  programId,
  programName,
  assayType,
  count(*) AS resultCount,
  count_if(isSuccess) AS successCount,
  round(count_if(isSuccess) / count(*), 4) AS successRate
FROM `${catalog}`.`${experimentation_gold_schema}`.`03_fact_experiment_outcomes`
GROUP BY programId, programName, assayType;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${experimentation_gold_schema}`.`03_agg_scientist_productivity`
COMMENT 'Scientist productivity per month: experiments started and finished and success rate. Contains scientist name and email (personal data).'
AS
WITH started AS (
  SELECT scientistId, trunc(createdAt, 'MM') AS monthStart, count(*) AS experimentsStarted
  FROM `${catalog}`.`${benchling_silver_schema}`.`02_experiment`
  GROUP BY scientistId, trunc(createdAt, 'MM')
),
finished AS (
  SELECT scientistId, trunc(completedAt, 'MM') AS monthStart,
    count(*) AS experimentsFinished,
    count_if(status = 'Completed') AS experimentsCompleted
  FROM `${catalog}`.`${benchling_silver_schema}`.`02_experiment`
  WHERE completedAt IS NOT NULL
  GROUP BY scientistId, trunc(completedAt, 'MM')
)
SELECT
  s.scientistId,
  s.displayName,
  s.email,
  s.labId,
  coalesce(a.monthStart, f.monthStart) AS monthStart,
  coalesce(a.experimentsStarted, 0) AS experimentsStarted,
  coalesce(f.experimentsFinished, 0) AS experimentsFinished,
  coalesce(f.experimentsCompleted, 0) AS experimentsCompleted,
  round(coalesce(f.experimentsCompleted, 0) / nullif(f.experimentsFinished, 0), 4) AS completionRate
FROM started a
FULL OUTER JOIN finished f ON f.scientistId = a.scientistId AND f.monthStart = a.monthStart
JOIN `${catalog}`.`${benchling_silver_schema}`.`02_scientist` s ON s.scientistId = coalesce(a.scientistId, f.scientistId);
