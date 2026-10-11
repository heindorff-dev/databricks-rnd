-- Databricks notebook source
-- Column tags (pii) and descriptions owned by the experimentation bundle.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:benchling_silver_schema);

SET TAG ON COLUMN `02_scientist`.displayName pii;
SET TAG ON COLUMN `02_scientist`.email pii;
SET TAG ON COLUMN `02_experiment`.scientistId pii;

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:experimentation_gold_schema);

COMMENT ON COLUMN `03_dim_experiment`.experimentId IS 'Benchling experiment UUID.';
COMMENT ON COLUMN `03_dim_experiment`.experimentName IS 'Experiment name.';
COMMENT ON COLUMN `03_dim_experiment`.projectId IS 'Identifier of the project.';
COMMENT ON COLUMN `03_dim_experiment`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_dim_experiment`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_dim_experiment`.assayType IS 'Assay type.';
COMMENT ON COLUMN `03_dim_experiment`.replicateCount IS 'Number of replicates.';
COMMENT ON COLUMN `03_dim_experiment`.status IS 'Planned, In Progress, Completed or Failed.';
COMMENT ON COLUMN `03_dim_experiment`.scientistId IS 'Identifier of the scientist who created the experiment.';
COMMENT ON COLUMN `03_dim_experiment`.createdAt IS 'Creation timestamp.';
COMMENT ON COLUMN `03_dim_experiment`.completedAt IS 'Completion timestamp, null while not finished.';
COMMENT ON COLUMN `03_dim_experiment`.durationDays IS 'Days from creation to completion.';
SET TAG ON COLUMN `03_dim_experiment`.scientistId pii;

-- COMMAND ----------
COMMENT ON COLUMN `03_fact_experiment_outcomes`.resultId IS 'Benchling result UUID.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.experimentId IS 'Identifier of the experiment.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.projectId IS 'Identifier of the project.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.assayType IS 'Assay type.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.compoundId IS 'Registry identifier of the compound.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.compoundName IS 'Name of the compound.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.outcome IS 'Success, Partial or Failure.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.isSuccess IS 'True when the outcome is Success.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.measuredValue IS 'Measured value.';
COMMENT ON COLUMN `03_fact_experiment_outcomes`.measuredAt IS 'Timestamp of the measurement.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_success_rate_by_program`.programId IS 'Identifier of the program.';
COMMENT ON COLUMN `03_agg_success_rate_by_program`.programName IS 'Name of the program.';
COMMENT ON COLUMN `03_agg_success_rate_by_program`.assayType IS 'Assay type.';
COMMENT ON COLUMN `03_agg_success_rate_by_program`.resultCount IS 'Number of results.';
COMMENT ON COLUMN `03_agg_success_rate_by_program`.successCount IS 'Number of successful results.';
COMMENT ON COLUMN `03_agg_success_rate_by_program`.successRate IS 'Share of successful results (0-1).';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_scientist_productivity`.scientistId IS 'Identifier of the scientist.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.displayName IS 'Full name of the scientist.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.email IS 'Work email address of the scientist.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.labId IS 'Identifier of the home laboratory.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.monthStart IS 'First day of the month.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.experimentsStarted IS 'Experiments created in the month.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.experimentsFinished IS 'Experiments completed or failed in the month.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.experimentsCompleted IS 'Experiments finished successfully in the month.';
COMMENT ON COLUMN `03_agg_scientist_productivity`.completionRate IS 'Completed share of finished experiments (0-1).';
SET TAG ON COLUMN `03_agg_scientist_productivity`.scientistId pii;
SET TAG ON COLUMN `03_agg_scientist_productivity`.displayName pii;
SET TAG ON COLUMN `03_agg_scientist_productivity`.email pii;
