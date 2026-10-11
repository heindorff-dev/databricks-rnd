-- Databricks notebook source
-- Column descriptions owned by the rd_financials bundle. No personal data in this product.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:rd_financials_gold_schema);

COMMENT ON COLUMN `03_fact_rd_spend_monthly`.budgetLineId IS 'Planview budget line identifier.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.projectId IS 'Identifier of the project.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.projectName IS 'Title of the project.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.therapeuticArea IS 'Therapeutic area of the program.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.periodStartDate IS 'First day of the fiscal month.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.fiscalYear IS 'Fiscal year.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.fiscalMonth IS 'Fiscal month (1-12).';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.costCategory IS 'Cost category: Personnel, Materials, CRO Services, Equipment or Travel.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.plannedAmountEur IS 'Planned amount in EUR.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.actualAmountEur IS 'Actual amount in EUR.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.forecastAmountEur IS 'Forecast amount in EUR.';
COMMENT ON COLUMN `03_fact_rd_spend_monthly`.varianceEur IS 'Actual minus planned amount in EUR; positive means overspend.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_budget_vs_actual`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.therapeuticArea IS 'Therapeutic area of the program.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.fiscalYear IS 'Fiscal year.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.plannedAmountEur IS 'Planned amount in EUR.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.actualAmountEur IS 'Actual amount in EUR.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.forecastAmountEur IS 'Forecast amount in EUR.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.varianceEur IS 'Actual minus planned in EUR.';
COMMENT ON COLUMN `03_agg_budget_vs_actual`.variancePct IS 'Variance as a fraction of planned amount.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_spend_by_program`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_agg_spend_by_program`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_agg_spend_by_program`.costCategory IS 'Cost category.';
COMMENT ON COLUMN `03_agg_spend_by_program`.actualAmountEur IS 'Actual spend in EUR.';
COMMENT ON COLUMN `03_agg_spend_by_program`.shareOfProgramSpend IS 'Share of the program total actual spend (0-1).';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_resource_cost_fte`.projectId IS 'Identifier of the project.';
COMMENT ON COLUMN `03_agg_resource_cost_fte`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_agg_resource_cost_fte`.periodStartDate IS 'First day of the fiscal month.';
COMMENT ON COLUMN `03_agg_resource_cost_fte`.allocatedFte IS 'Planned full-time equivalents.';
COMMENT ON COLUMN `03_agg_resource_cost_fte`.actualFte IS 'Actual full-time equivalents.';
COMMENT ON COLUMN `03_agg_resource_cost_fte`.personnelCostEur IS 'Actual personnel cost in EUR.';
COMMENT ON COLUMN `03_agg_resource_cost_fte`.costPerFteEur IS 'Personnel cost per actual FTE in EUR.';
