-- Silver layer for equipment IoT. Bronze has narrow metric/value readings and epoch timestamps; silver conforms them.

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${equipment_iot_silver_schema}`.`02_asset` (
  instrumentId STRING COMMENT 'Canonical instrument identifier, for example INS-001.',
  assetTag STRING COMMENT 'Asset tag in the IoT platform.',
  labId STRING COMMENT 'Canonical identifier of the lab the instrument is located in.',
  equipmentClass STRING COMMENT 'Instrument type, for example HPLC.',
  manufacturer STRING COMMENT 'Instrument vendor.',
  installedAt TIMESTAMP COMMENT 'Timestamp when the instrument was installed.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_key EXPECT (assetTag IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Instrument registry from the equipment IoT platform.'
AS SELECT
  concat('INS-', substring(asset_tag, 4, 4)) AS instrumentId,
  asset_tag AS assetTag,
  concat('LAB-', substring(location_code, 5, 2)) AS labId,
  equipment_class AS equipmentClass,
  manufacturer,
  timestamp_micros(installed_us) AS installedAt,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${equipment_iot_bronze_schema}`.`01_assets`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${equipment_iot_silver_schema}`.`02_reading` (
  assetTag STRING COMMENT 'Asset tag in the IoT platform.',
  readingAt TIMESTAMP COMMENT 'Timestamp of the reading.',
  readingDate DATE COMMENT 'Calendar date of the reading.',
  metric STRING COMMENT 'Metric name: run_minutes or temp_c.',
  value DECIMAL(10,2) COMMENT 'Reading value. Minutes for run_minutes, degrees Celsius for temp_c.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (assetTag IS NOT NULL AND readingAt IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_value EXPECT (value IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Sensor readings per instrument with microsecond epoch timestamps converted to timestamps and noisy floats rounded.'
AS SELECT
  asset_tag AS assetTag,
  timestamp_micros(ts_us) AS readingAt,
  CAST(timestamp_micros(ts_us) AS DATE) AS readingDate,
  metric,
  try_cast(round(value, 2) AS DECIMAL(10,2)) AS value,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${equipment_iot_bronze_schema}`.`01_readings`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${equipment_iot_silver_schema}`.`02_booking` (
  bookingId STRING COMMENT 'Booking reference.',
  assetTag STRING COMMENT 'Asset tag of the booked instrument.',
  scientistId STRING COMMENT 'Canonical identifier of the scientist who booked the instrument. Personal data.',
  startAt TIMESTAMP COMMENT 'Booking start time.',
  endAt TIMESTAMP COMMENT 'Booking end time.',
  durationHours DECIMAL(5,1) COMMENT 'Booked duration in hours.',
  status STRING COMMENT 'Confirmed, Cancelled or No Show.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (bookingId IS NOT NULL AND startAt IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Instrument bookings from the equipment booking system.'
AS SELECT
  booking_ref AS bookingId,
  asset_tag AS assetTag,
  concat('SCI-', substring(user_ref, 2, 4)) AS scientistId,
  timestamp_seconds(start_epoch_s) AS startAt,
  timestamp_seconds(start_epoch_s + CAST(duration_h * 3600 AS BIGINT)) AS endAt,
  try_cast(duration_h AS DECIMAL(5,1)) AS durationHours,
  CASE booking_state WHEN 'CONFIRMED' THEN 'Confirmed' WHEN 'CANCELLED' THEN 'Cancelled' WHEN 'NO_SHOW' THEN 'No Show' ELSE 'Unknown' END AS status,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${equipment_iot_bronze_schema}`.`01_bookings`);

-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE `${catalog}`.`${equipment_iot_silver_schema}`.`02_downtime_event` (
  eventId STRING COMMENT 'Downtime event identifier.',
  assetTag STRING COMMENT 'Asset tag of the instrument.',
  startAt TIMESTAMP COMMENT 'Downtime start time.',
  durationSeconds INT COMMENT 'Downtime duration in seconds.',
  reason STRING COMMENT 'Preventive Maintenance, Failure or Calibration.',
  ingestedAt TIMESTAMP COMMENT 'Timestamp when the row was written to bronze.',
  CONSTRAINT valid_keys EXPECT (eventId IS NOT NULL AND startAt IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Instrument downtime events with decoded reason codes.'
AS SELECT
  event_id AS eventId,
  asset_tag AS assetTag,
  timestamp_micros(start_us) AS startAt,
  try_cast(duration_s AS INT) AS durationSeconds,
  CASE reason_code WHEN 'PM' THEN 'Preventive Maintenance' WHEN 'FAIL' THEN 'Failure' WHEN 'CAL' THEN 'Calibration' ELSE 'Unknown' END AS reason,
  CAST(_ingested_at AS TIMESTAMP) AS ingestedAt
FROM STREAM(`${catalog}`.`${equipment_iot_bronze_schema}`.`01_downtime_events`);
