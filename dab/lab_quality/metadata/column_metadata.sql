-- Databricks notebook source
-- Column tags (pii) and descriptions owned by the lab_quality bundle.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:labware_silver_schema);

SET TAG ON COLUMN `02_result`.analystId pii;

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:lab_quality_gold_schema);

COMMENT ON COLUMN `03_fact_sample_results`.resultId IS 'LabWare result identifier.';
COMMENT ON COLUMN `03_fact_sample_results`.sampleId IS 'Identifier of the tested sample.';
COMMENT ON COLUMN `03_fact_sample_results`.projectId IS 'Identifier of the project the sample belongs to.';
COMMENT ON COLUMN `03_fact_sample_results`.labId IS 'Identifier of the laboratory.';
COMMENT ON COLUMN `03_fact_sample_results`.labName IS 'Name of the laboratory.';
COMMENT ON COLUMN `03_fact_sample_results`.siteName IS 'Site of the laboratory.';
COMMENT ON COLUMN `03_fact_sample_results`.instrumentId IS 'Identifier of the instrument used.';
COMMENT ON COLUMN `03_fact_sample_results`.instrumentType IS 'Type of the instrument used.';
COMMENT ON COLUMN `03_fact_sample_results`.assayCode IS 'Assay code.';
COMMENT ON COLUMN `03_fact_sample_results`.sampleType IS 'Sample matrix.';
COMMENT ON COLUMN `03_fact_sample_results`.analystId IS 'Identifier of the analyst who ran the test.';
COMMENT ON COLUMN `03_fact_sample_results`.resultValue IS 'Measured value.';
COMMENT ON COLUMN `03_fact_sample_results`.unit IS 'Unit of the measured value.';
COMMENT ON COLUMN `03_fact_sample_results`.specLow IS 'Lower specification limit.';
COMMENT ON COLUMN `03_fact_sample_results`.specHigh IS 'Upper specification limit.';
COMMENT ON COLUMN `03_fact_sample_results`.isPass IS 'True when the result is within specification.';
COMMENT ON COLUMN `03_fact_sample_results`.receivedDate IS 'Date the sample was received.';
COMMENT ON COLUMN `03_fact_sample_results`.completedDate IS 'Date the test was completed.';
COMMENT ON COLUMN `03_fact_sample_results`.turnaroundDays IS 'Days from sample receipt to test completion.';
SET TAG ON COLUMN `03_fact_sample_results`.analystId pii;

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_assay_pass_rate`.labId IS 'Identifier of the laboratory.';
COMMENT ON COLUMN `03_agg_assay_pass_rate`.labName IS 'Name of the laboratory.';
COMMENT ON COLUMN `03_agg_assay_pass_rate`.assayCode IS 'Assay code.';
COMMENT ON COLUMN `03_agg_assay_pass_rate`.monthStart IS 'First day of the month the tests were completed.';
COMMENT ON COLUMN `03_agg_assay_pass_rate`.testCount IS 'Number of tests completed.';
COMMENT ON COLUMN `03_agg_assay_pass_rate`.passedCount IS 'Number of tests within specification.';
COMMENT ON COLUMN `03_agg_assay_pass_rate`.passRate IS 'Share of tests within specification (0-1).';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_turnaround_time`.labId IS 'Identifier of the laboratory.';
COMMENT ON COLUMN `03_agg_turnaround_time`.labName IS 'Name of the laboratory.';
COMMENT ON COLUMN `03_agg_turnaround_time`.sampleType IS 'Sample matrix.';
COMMENT ON COLUMN `03_agg_turnaround_time`.sampleCount IS 'Number of distinct samples.';
COMMENT ON COLUMN `03_agg_turnaround_time`.avgTurnaroundDays IS 'Average days from receipt to completion.';
COMMENT ON COLUMN `03_agg_turnaround_time`.p90TurnaroundDays IS '90th percentile of turnaround days.';
COMMENT ON COLUMN `03_agg_turnaround_time`.maxTurnaroundDays IS 'Longest turnaround in days.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_out_of_spec`.instrumentId IS 'Identifier of the instrument.';
COMMENT ON COLUMN `03_agg_out_of_spec`.instrumentType IS 'Type of the instrument.';
COMMENT ON COLUMN `03_agg_out_of_spec`.labId IS 'Identifier of the laboratory.';
COMMENT ON COLUMN `03_agg_out_of_spec`.labName IS 'Name of the laboratory.';
COMMENT ON COLUMN `03_agg_out_of_spec`.assayCode IS 'Assay code.';
COMMENT ON COLUMN `03_agg_out_of_spec`.outOfSpecCount IS 'Number of out-of-specification results.';
COMMENT ON COLUMN `03_agg_out_of_spec`.avgDeviation IS 'Average distance outside the specification limits.';
COMMENT ON COLUMN `03_agg_out_of_spec`.lastOutOfSpecDate IS 'Date of the latest out-of-specification result.';
