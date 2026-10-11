-- Gold data product: Lab quality. Materialized views over the LabWare silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${lab_quality_gold_schema}`.`03_fact_sample_results`
COMMENT 'Test result fact: one row per laboratory result with sample, lab, instrument, specification and turnaround time. Contains the analyst identifier (personal data).'
AS
SELECT
  r.resultId,
  r.sampleId,
  s.projectId,
  s.labId,
  l.labName,
  l.siteName,
  r.instrumentId,
  i.instrumentType,
  r.assayCode,
  s.sampleType,
  r.analystId,
  r.resultValue,
  r.unit,
  r.specLow,
  r.specHigh,
  r.isPass,
  s.receivedDate,
  r.completedDate,
  datediff(r.completedDate, s.receivedDate) AS turnaroundDays
FROM `${catalog}`.`${labware_silver_schema}`.`02_result` r
JOIN `${catalog}`.`${labware_silver_schema}`.`02_sample` s ON s.sampleId = r.sampleId
JOIN `${catalog}`.`${labware_silver_schema}`.`02_lab` l ON l.labId = s.labId
LEFT JOIN `${catalog}`.`${labware_silver_schema}`.`02_instrument` i ON i.instrumentId = r.instrumentId;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${lab_quality_gold_schema}`.`03_agg_assay_pass_rate`
COMMENT 'Assay pass rate per laboratory, assay and month.'
AS
SELECT
  labId,
  labName,
  assayCode,
  trunc(completedDate, 'MM') AS monthStart,
  count(*) AS testCount,
  count_if(isPass) AS passedCount,
  round(count_if(isPass) / count(*), 4) AS passRate
FROM `${catalog}`.`${lab_quality_gold_schema}`.`03_fact_sample_results`
GROUP BY labId, labName, assayCode, trunc(completedDate, 'MM');

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${lab_quality_gold_schema}`.`03_agg_turnaround_time`
COMMENT 'Sample turnaround time (received to completed) per laboratory and sample type.'
AS
SELECT
  labId,
  labName,
  sampleType,
  count(DISTINCT sampleId) AS sampleCount,
  round(avg(turnaroundDays), 2) AS avgTurnaroundDays,
  percentile_approx(turnaroundDays, 0.9) AS p90TurnaroundDays,
  max(turnaroundDays) AS maxTurnaroundDays
FROM `${catalog}`.`${lab_quality_gold_schema}`.`03_fact_sample_results`
GROUP BY labId, labName, sampleType;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${lab_quality_gold_schema}`.`03_agg_out_of_spec`
COMMENT 'Out-of-specification results per instrument and assay, to spot instruments that drift.'
AS
SELECT
  instrumentId,
  instrumentType,
  labId,
  labName,
  assayCode,
  count(*) AS outOfSpecCount,
  round(avg(CASE WHEN resultValue < specLow THEN specLow - resultValue ELSE resultValue - specHigh END), 4) AS avgDeviation,
  max(completedDate) AS lastOutOfSpecDate
FROM `${catalog}`.`${lab_quality_gold_schema}`.`03_fact_sample_results`
WHERE NOT isPass
GROUP BY instrumentId, instrumentType, labId, labName, assayCode;
