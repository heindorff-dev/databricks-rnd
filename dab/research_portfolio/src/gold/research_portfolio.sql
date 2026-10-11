-- Gold data product: Research portfolio. Materialized views over the Planview and Jira R&D silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${research_portfolio_gold_schema}`.`03_dim_program`
COMMENT 'Program dimension: one row per research program with project counts and total budget. Contains the program manager name (personal data).'
AS
SELECT
  g.programId,
  g.programName,
  g.therapeuticArea,
  g.developmentPhase,
  g.programStatus,
  g.programManagerName,
  g.startDate,
  count(p.projectId) AS projectCount,
  count_if(p.projectStatus IN ('Active', 'At Risk', 'On Hold')) AS openProjectCount,
  coalesce(sum(p.plannedBudgetEur), 0) AS totalBudgetEur
FROM `${catalog}`.`${planview_silver_schema}`.`02_program` g
LEFT JOIN `${catalog}`.`${planview_silver_schema}`.`02_project` p ON p.programId = g.programId
GROUP BY g.programId, g.programName, g.therapeuticArea, g.developmentPhase, g.programStatus, g.programManagerName, g.startDate;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${research_portfolio_gold_schema}`.`03_dim_project`
COMMENT 'Project dimension: one row per project with program context, schedule and budget. Contains the project manager name (personal data).'
AS
SELECT
  p.projectId,
  p.projectName,
  p.projectType,
  p.projectStatus,
  p.priority,
  p.programId,
  g.programName,
  g.therapeuticArea,
  g.developmentPhase,
  p.projectManagerName,
  p.startDate,
  p.plannedEndDate,
  p.actualEndDate,
  datediff(coalesce(p.actualEndDate, p.plannedEndDate), p.startDate) AS plannedDurationDays,
  p.plannedBudgetEur
FROM `${catalog}`.`${planview_silver_schema}`.`02_project` p
JOIN `${catalog}`.`${planview_silver_schema}`.`02_program` g ON g.programId = p.programId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${research_portfolio_gold_schema}`.`03_fact_project_milestones`
COMMENT 'Milestone fact: one row per project milestone with planned and actual dates and schedule slip in days.'
AS
SELECT
  m.milestoneId,
  m.projectId,
  p.programId,
  p.programName,
  m.milestoneName,
  m.milestoneStatus,
  m.plannedDate,
  m.actualDate,
  datediff(m.actualDate, m.plannedDate) AS slipDays,
  m.actualDate IS NOT NULL AND m.actualDate <= m.plannedDate AS isOnTime
FROM `${catalog}`.`${planview_silver_schema}`.`02_milestone` m
JOIN `${catalog}`.`${research_portfolio_gold_schema}`.`03_dim_project` p ON p.projectId = m.projectId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${research_portfolio_gold_schema}`.`03_agg_portfolio_status`
COMMENT 'Portfolio status per program: project counts by status, milestone schedule performance and budget.'
AS
WITH ms AS (
  SELECT programId,
    count(*) AS achievedMilestones,
    avg(slipDays) AS avgSlipDays,
    avg(CASE WHEN isOnTime THEN 1.0 ELSE 0.0 END) AS onTimeRate
  FROM `${catalog}`.`${research_portfolio_gold_schema}`.`03_fact_project_milestones`
  WHERE milestoneStatus = 'Achieved'
  GROUP BY programId
)
SELECT
  p.programId,
  p.programName,
  p.therapeuticArea,
  p.developmentPhase,
  count(*) AS projectCount,
  count_if(p.projectStatus = 'Active') AS activeProjects,
  count_if(p.projectStatus = 'At Risk') AS atRiskProjects,
  count_if(p.projectStatus = 'On Hold') AS onHoldProjects,
  count_if(p.projectStatus = 'Completed') AS completedProjects,
  count_if(p.projectStatus = 'Cancelled') AS cancelledProjects,
  sum(p.plannedBudgetEur) AS totalBudgetEur,
  max(ms.achievedMilestones) AS achievedMilestones,
  round(max(ms.avgSlipDays), 1) AS avgMilestoneSlipDays,
  round(max(ms.onTimeRate), 3) AS milestoneOnTimeRate
FROM `${catalog}`.`${research_portfolio_gold_schema}`.`03_dim_project` p
LEFT JOIN ms ON ms.programId = p.programId
GROUP BY p.programId, p.programName, p.therapeuticArea, p.developmentPhase;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${research_portfolio_gold_schema}`.`03_agg_task_throughput`
COMMENT 'Task throughput per project and month from Jira: issues created and resolved, average cycle time and story points delivered.'
AS
WITH created AS (
  SELECT projectId, trunc(createdAt, 'MM') AS monthStart, count(*) AS issuesCreated
  FROM `${catalog}`.`${jira_rd_silver_schema}`.`02_issue`
  GROUP BY projectId, trunc(createdAt, 'MM')
),
resolved AS (
  SELECT projectId, trunc(resolvedAt, 'MM') AS monthStart,
    count(*) AS issuesResolved,
    avg((unix_timestamp(resolvedAt) - unix_timestamp(createdAt)) / 86400.0) AS avgCycleTimeDays,
    sum(storyPoints) AS storyPointsDelivered
  FROM `${catalog}`.`${jira_rd_silver_schema}`.`02_issue`
  WHERE resolvedAt IS NOT NULL
  GROUP BY projectId, trunc(resolvedAt, 'MM')
)
SELECT
  coalesce(c.projectId, r.projectId) AS projectId,
  coalesce(c.monthStart, r.monthStart) AS monthStart,
  p.programId,
  p.programName,
  coalesce(c.issuesCreated, 0) AS issuesCreated,
  coalesce(r.issuesResolved, 0) AS issuesResolved,
  round(r.avgCycleTimeDays, 2) AS avgCycleTimeDays,
  coalesce(r.storyPointsDelivered, 0) AS storyPointsDelivered
FROM created c
FULL OUTER JOIN resolved r ON r.projectId = c.projectId AND r.monthStart = c.monthStart
JOIN `${catalog}`.`${research_portfolio_gold_schema}`.`03_dim_project` p ON p.projectId = coalesce(c.projectId, r.projectId);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${research_portfolio_gold_schema}`.`03_agg_risk_exposure`
COMMENT 'Open risk exposure per program: number of open risks and their score distribution.'
AS
SELECT
  p.programId,
  p.programName,
  count(*) AS openRisks,
  count_if(r.riskScore >= 15) AS highRisks,
  round(avg(r.riskScore), 2) AS avgRiskScore,
  max(r.riskScore) AS maxRiskScore
FROM `${catalog}`.`${planview_silver_schema}`.`02_risk` r
JOIN `${catalog}`.`${research_portfolio_gold_schema}`.`03_dim_project` p ON p.projectId = r.projectId
WHERE r.riskStatus = 'Open'
GROUP BY p.programId, p.programName;
