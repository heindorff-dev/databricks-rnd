-- Databricks notebook source
-- Column tags and descriptions owned by the equipment_utilization bundle.
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:equipment_iot_silver_schema);

SET TAG ON COLUMN `02_booking`.scientistId pii;

-- COMMAND ----------
USE SCHEMA IDENTIFIER(:equipment_utilization_gold_schema);

COMMENT ON COLUMN `03_fact_instrument_usage_daily`.instrumentId IS 'Canonical instrument identifier.';
COMMENT ON COLUMN `03_fact_instrument_usage_daily`.labId IS 'Canonical lab identifier.';
COMMENT ON COLUMN `03_fact_instrument_usage_daily`.equipmentClass IS 'Instrument type.';
COMMENT ON COLUMN `03_fact_instrument_usage_daily`.usageDate IS 'Calendar date.';
COMMENT ON COLUMN `03_fact_instrument_usage_daily`.runMinutes IS 'Minutes the instrument was running.';
COMMENT ON COLUMN `03_fact_instrument_usage_daily`.avgTempC IS 'Average temperature in degrees Celsius.';
COMMENT ON COLUMN `03_fact_instrument_usage_daily`.downtimeMinutes IS 'Minutes of downtime starting on the date.';
COMMENT ON COLUMN `03_fact_instrument_usage_daily`.utilizationRate IS 'Run minutes divided by 1440 (0-1).';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_utilization_by_lab`.labId IS 'Canonical lab identifier.';
COMMENT ON COLUMN `03_agg_utilization_by_lab`.equipmentClass IS 'Instrument type.';
COMMENT ON COLUMN `03_agg_utilization_by_lab`.monthStart IS 'First day of the month.';
COMMENT ON COLUMN `03_agg_utilization_by_lab`.instrumentCount IS 'Instruments with readings in the month.';
COMMENT ON COLUMN `03_agg_utilization_by_lab`.runHours IS 'Total run hours.';
COMMENT ON COLUMN `03_agg_utilization_by_lab`.avgUtilizationRate IS 'Average daily utilization rate (0-1).';
COMMENT ON COLUMN `03_agg_utilization_by_lab`.downtimeHours IS 'Total downtime hours.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_downtime_events`.instrumentId IS 'Canonical instrument identifier.';
COMMENT ON COLUMN `03_agg_downtime_events`.labId IS 'Canonical lab identifier.';
COMMENT ON COLUMN `03_agg_downtime_events`.equipmentClass IS 'Instrument type.';
COMMENT ON COLUMN `03_agg_downtime_events`.reason IS 'Preventive Maintenance, Failure or Calibration.';
COMMENT ON COLUMN `03_agg_downtime_events`.eventCount IS 'Number of downtime events.';
COMMENT ON COLUMN `03_agg_downtime_events`.downtimeHours IS 'Total downtime hours.';
COMMENT ON COLUMN `03_agg_downtime_events`.avgEventHours IS 'Average downtime hours per event.';
COMMENT ON COLUMN `03_agg_downtime_events`.lastEventAt IS 'Start of the most recent downtime event.';

-- COMMAND ----------
COMMENT ON COLUMN `03_agg_booking_demand`.labId IS 'Canonical lab identifier.';
COMMENT ON COLUMN `03_agg_booking_demand`.equipmentClass IS 'Instrument type.';
COMMENT ON COLUMN `03_agg_booking_demand`.monthStart IS 'First day of the booking month.';
COMMENT ON COLUMN `03_agg_booking_demand`.bookingCount IS 'Number of bookings.';
COMMENT ON COLUMN `03_agg_booking_demand`.bookedHours IS 'Confirmed booked hours.';
COMMENT ON COLUMN `03_agg_booking_demand`.cancelledCount IS 'Cancelled bookings.';
COMMENT ON COLUMN `03_agg_booking_demand`.noShowCount IS 'Bookings where the user did not show.';
COMMENT ON COLUMN `03_agg_booking_demand`.distinctBookers IS 'Distinct scientists who booked.';
