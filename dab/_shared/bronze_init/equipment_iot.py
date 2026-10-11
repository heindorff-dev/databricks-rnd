# Databricks notebook source
# Equipment IoT bronze generator: narrow key-value readings, microsecond epoch timestamps, float noise.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

DAYS = 180
ref = F.lit(c.REF_DATE).cast("date")
inst = c.instruments(spark)
ik = F.col("i")
tag = F.concat(F.lit("EQ_"), F.lpad(ik.cast("string"), 4, "0"))

# COMMAND ----------

assets = inst.select(
    tag.alias("asset_tag"),
    F.concat(F.lit("lab_"), F.lpad(F.col("lab_idx").cast("string"), 2, "0")).alias("location_code"),
    c.lookup(c.INSTRUMENT_TYPES, "type_idx", 0).alias("equipment_class"),
    c.lookup(c.INSTRUMENT_TYPES, "type_idx", 1).alias("manufacturer"),
    (F.unix_timestamp(c.rdate(ik, "in", "2016-01-01", "2023-12-31").cast("timestamp")) * 1000000).alias("installed_us"),
)
c.write_bronze(spark, catalog, schema, "01_assets", assets, "Equipment IoT asset registry: EQ_0001 asset tags, lab_01 location codes and microsecond epoch install timestamps.")

# COMMAND ----------

# Two readings per asset per day in a narrow metric/value layout.
r = (
    inst.crossJoin(spark.range(0, DAYS).withColumnRenamed("id", "d"))
    .crossJoin(spark.range(0, 2).withColumnRenamed("id", "m"))
    .withColumn("rk", ik * 1000 + F.col("d"))
    .withColumn("day", F.date_sub(ref, F.col("d").cast("int")))
)
rk = F.col("rk")
is_run = F.col("m") == 0
readings = r.select(
    tag.alias("asset_tag"),
    (F.unix_timestamp(F.col("day").cast("timestamp")) * 1000000 + c.rint(rk, "tu", 0, 86399).cast("bigint") * 1000000 + c.rint(rk, "tm", 0, 999999)).alias("ts_us"),
    F.when(is_run, F.lit("run_minutes")).otherwise(F.lit("temp_c")).alias("metric"),
    F.when(
        is_run,
        F.when(F.dayofweek("day").isin(1, 7), c.rint(rk, "rw", 0, 120)).otherwise(c.rint(rk, "rd", 60, 840)) + c.rfrac(rk, "rn"),
    ).otherwise(20 + c.rfrac(rk, "tn") * 6).alias("value"),
)
c.write_bronze(spark, catalog, schema, "01_readings", readings, "Equipment IoT readings in narrow key-value form: run_minutes per day and temp_c, with microsecond epoch timestamps.")

# COMMAND ----------

b = spark.range(1, 4001).withColumnRenamed("id", "b")
bk = F.col("b")
start_day = F.date_sub(ref, c.rint(bk, "bd", 0, DAYS - 1))
bookings = b.select(
    F.concat(F.lit("BK"), F.lpad(bk.cast("string"), 6, "0")).alias("booking_ref"),
    F.concat(F.lit("EQ_"), F.lpad(c.rint(bk, "bi", 1, c.N_INST).cast("string"), 4, "0")).alias("asset_tag"),
    F.concat(F.lit("U"), F.lpad(c.rint(bk, "bu", 1, c.N_SCI).cast("string"), 4, "0")).alias("user_ref"),
    (F.unix_timestamp(start_day.cast("timestamp")) + c.rint(bk, "bh", 8, 16) * 3600).alias("start_epoch_s"),
    F.round(c.rfrac(bk, "bl") * 6 + 0.5, 1).alias("duration_h"),
    c.pick(bk, "bs", ["CONFIRMED", "CONFIRMED", "CONFIRMED", "CANCELLED", "NO_SHOW"]).alias("booking_state"),
)
c.write_bronze(spark, catalog, schema, "01_bookings", bookings, "Equipment IoT booking system: bookings per asset with user reference U0001, epoch-second start and duration in hours.")

# COMMAND ----------

e = spark.range(1, 501).withColumnRenamed("id", "e")
ek = F.col("e")
ev_start = F.date_sub(ref, c.rint(ek, "ed", 1, DAYS)).cast("timestamp")
downtime = e.select(
    F.concat(F.lit("EVT"), F.lpad(ek.cast("string"), 6, "0")).alias("event_id"),
    F.concat(F.lit("EQ_"), F.lpad(c.rint(ek, "ea", 1, c.N_INST).cast("string"), 4, "0")).alias("asset_tag"),
    (F.unix_timestamp(ev_start) * 1000000 + c.rint(ek, "eh", 0, 80000).cast("bigint") * 1000000).alias("start_us"),
    c.rint(ek, "el", 600, 172800).alias("duration_s"),
    c.pick(ek, "ec", ["PM", "PM", "FAIL", "FAIL", "CAL"]).alias("reason_code"),
)
c.write_bronze(spark, catalog, schema, "01_downtime_events", downtime, "Equipment IoT downtime events with reason codes (PM preventive maintenance, FAIL failure, CAL calibration), microsecond epoch start and duration in seconds.")
