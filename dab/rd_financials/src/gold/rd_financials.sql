-- Gold data product: R&D financials. Materialized views over the Planview silver tables owned by research_portfolio.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${rd_financials_gold_schema}`.`03_fact_rd_spend_monthly`
COMMENT 'Monthly R&D spend fact: planned, actual and forecast amounts in EUR per project and cost category, with program context.'
AS
SELECT
  b.budgetLineId,
  b.projectId,
  p.projectName,
  p.programId,
  g.programName,
  g.therapeuticArea,
  b.periodStartDate,
  b.fiscalYear,
  b.fiscalMonth,
  b.costCategory,
  b.plannedAmountEur,
  b.actualAmountEur,
  b.forecastAmountEur,
  b.actualAmountEur - b.plannedAmountEur AS varianceEur
FROM `${catalog}`.`${planview_silver_schema}`.`02_budget_line` b
JOIN `${catalog}`.`${planview_silver_schema}`.`02_project` p ON p.projectId = b.projectId
JOIN `${catalog}`.`${planview_silver_schema}`.`02_program` g ON g.programId = p.programId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${rd_financials_gold_schema}`.`03_agg_budget_vs_actual`
COMMENT 'Budget versus actual spend per program and fiscal year in EUR, with variance and forecast.'
AS
SELECT
  programId,
  programName,
  therapeuticArea,
  fiscalYear,
  sum(plannedAmountEur) AS plannedAmountEur,
  sum(actualAmountEur) AS actualAmountEur,
  sum(forecastAmountEur) AS forecastAmountEur,
  sum(actualAmountEur) - sum(plannedAmountEur) AS varianceEur,
  round((sum(actualAmountEur) - sum(plannedAmountEur)) / nullif(sum(plannedAmountEur), 0), 4) AS variancePct
FROM `${catalog}`.`${rd_financials_gold_schema}`.`03_fact_rd_spend_monthly`
GROUP BY programId, programName, therapeuticArea, fiscalYear;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${rd_financials_gold_schema}`.`03_agg_spend_by_program`
COMMENT 'Actual spend per program and cost category in EUR with each category share of the program total.'
AS
SELECT
  programId,
  programName,
  costCategory,
  sum(actualAmountEur) AS actualAmountEur,
  round(sum(actualAmountEur) / sum(sum(actualAmountEur)) OVER (PARTITION BY programId), 4) AS shareOfProgramSpend
FROM `${catalog}`.`${rd_financials_gold_schema}`.`03_fact_rd_spend_monthly`
GROUP BY programId, programName, costCategory;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${rd_financials_gold_schema}`.`03_agg_resource_cost_fte`
COMMENT 'Resource effort versus personnel cost per project and month: planned and actual FTE, personnel cost in EUR and cost per actual FTE.'
AS
WITH fte AS (
  SELECT projectId, periodStartDate, sum(allocatedFte) AS allocatedFte, sum(actualFte) AS actualFte
  FROM `${catalog}`.`${planview_silver_schema}`.`02_resource_allocation`
  GROUP BY projectId, periodStartDate
),
cost AS (
  SELECT projectId, periodStartDate, sum(actualAmountEur) AS personnelCostEur
  FROM `${catalog}`.`${planview_silver_schema}`.`02_budget_line`
  WHERE costCategory = 'Personnel'
  GROUP BY projectId, periodStartDate
)
SELECT
  f.projectId,
  p.programId,
  f.periodStartDate,
  f.allocatedFte,
  f.actualFte,
  coalesce(c.personnelCostEur, 0) AS personnelCostEur,
  round(coalesce(c.personnelCostEur, 0) / nullif(f.actualFte, 0), 2) AS costPerFteEur
FROM fte f
JOIN `${catalog}`.`${planview_silver_schema}`.`02_project` p ON p.projectId = f.projectId
LEFT JOIN cost c ON c.projectId = f.projectId AND c.periodStartDate = f.periodStartDate;
