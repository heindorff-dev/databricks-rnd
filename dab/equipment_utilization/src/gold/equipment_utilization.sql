-- Gold data product: Equipment utilization. Materialized views over the equipment IoT silver tables.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${equipment_utilization_gold_schema}`.`03_fact_instrument_usage_daily`
COMMENT 'Daily instrument usage: run minutes, average temperature and downtime per instrument and day.'
AS
WITH usage AS (
  SELECT
    assetTag,
    readingDate AS usageDate,
    sum(CASE WHEN metric = 'run_minutes' THEN value END) AS runMinutes,
    round(avg(CASE WHEN metric = 'temp_c' THEN value END), 2) AS avgTempC
  FROM `${catalog}`.`${equipment_iot_silver_schema}`.`02_reading`
  GROUP BY assetTag, readingDate
),
down AS (
  SELECT assetTag, CAST(startAt AS DATE) AS usageDate, sum(durationSeconds) / 60.0 AS downtimeMinutes
  FROM `${catalog}`.`${equipment_iot_silver_schema}`.`02_downtime_event`
  GROUP BY assetTag, CAST(startAt AS DATE)
)
SELECT
  a.instrumentId,
  a.labId,
  a.equipmentClass,
  u.usageDate,
  u.runMinutes,
  u.avgTempC,
  round(coalesce(d.downtimeMinutes, 0), 1) AS downtimeMinutes,
  round(u.runMinutes / 1440, 4) AS utilizationRate
FROM usage u
JOIN `${catalog}`.`${equipment_iot_silver_schema}`.`02_asset` a ON a.assetTag = u.assetTag
LEFT JOIN down d ON d.assetTag = u.assetTag AND d.usageDate = u.usageDate;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${equipment_utilization_gold_schema}`.`03_agg_utilization_by_lab`
COMMENT 'Monthly utilization per lab and equipment class.'
AS
SELECT
  labId,
  equipmentClass,
  trunc(usageDate, 'MM') AS monthStart,
  count(DISTINCT instrumentId) AS instrumentCount,
  round(sum(runMinutes) / 60, 1) AS runHours,
  round(avg(utilizationRate), 4) AS avgUtilizationRate,
  round(sum(downtimeMinutes) / 60, 1) AS downtimeHours
FROM `${catalog}`.`${equipment_utilization_gold_schema}`.`03_fact_instrument_usage_daily`
GROUP BY labId, equipmentClass, trunc(usageDate, 'MM');

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${equipment_utilization_gold_schema}`.`03_agg_downtime_events`
COMMENT 'Downtime per instrument and reason: event count, total hours and average duration.'
AS
SELECT
  a.instrumentId,
  a.labId,
  a.equipmentClass,
  e.reason,
  count(*) AS eventCount,
  round(sum(e.durationSeconds) / 3600, 1) AS downtimeHours,
  round(avg(e.durationSeconds) / 3600, 2) AS avgEventHours,
  max(e.startAt) AS lastEventAt
FROM `${catalog}`.`${equipment_iot_silver_schema}`.`02_downtime_event` e
JOIN `${catalog}`.`${equipment_iot_silver_schema}`.`02_asset` a ON a.assetTag = e.assetTag
GROUP BY a.instrumentId, a.labId, a.equipmentClass, e.reason;

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.`${equipment_utilization_gold_schema}`.`03_agg_booking_demand`
COMMENT 'Booking demand per equipment class and lab and month: booked hours, cancellations and no-shows.'
AS
SELECT
  a.labId,
  a.equipmentClass,
  trunc(b.startAt, 'MM') AS monthStart,
  count(*) AS bookingCount,
  round(sum(CASE WHEN b.status = 'Confirmed' THEN b.durationHours ELSE 0 END), 1) AS bookedHours,
  count_if(b.status = 'Cancelled') AS cancelledCount,
  count_if(b.status = 'No Show') AS noShowCount,
  count(DISTINCT b.scientistId) AS distinctBookers
FROM `${catalog}`.`${equipment_iot_silver_schema}`.`02_booking` b
JOIN `${catalog}`.`${equipment_iot_silver_schema}`.`02_asset` a ON a.assetTag = b.assetTag
GROUP BY a.labId, a.equipmentClass, trunc(b.startAt, 'MM');
