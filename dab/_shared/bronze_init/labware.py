# Databricks notebook source
# LabWare LIMS bronze generator: UPPER_CASE Oracle-style columns, YYYYMMDD string dates, numeric values as strings, Y/N flags.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_SAMPLES = 20000
ASSAYS = [("ASY-POTENCY", "ng/mL"), ("ASY-PURITY", "%"), ("ASY-STABILITY", "%"), ("ASY-SOLUBILITY", "mg/mL"), ("ASY-IMPURITY", "ppm")]
SAMPLE_TYPES = ["PLASMA", "SERUM", "TISSUE", "API BATCH", "FORMULATION"]

# COMMAND ----------

labs = spark.createDataFrame(
    [(f"L{i + 1:02d}", l[0].upper(), l[1].upper(), l[2]) for i, l in enumerate(c.LABS)],
    "LAB_CODE string, LAB_DESC string, SITE_NAME string, COUNTRY_CD string",
)
c.write_bronze(spark, catalog, schema, "01_labs", labs, "LabWare laboratories. Upper-case codes and descriptions.")

# COMMAND ----------

ins = c.instruments(spark)
i = F.col("i")
instruments = ins.select(
    F.concat(F.lit("I-"), F.lpad(i.cast("string"), 3, "0")).alias("INSTR_NO"),
    F.concat(F.lit("L"), F.lpad(F.col("lab_idx").cast("string"), 2, "0")).alias("LAB_CODE"),
    F.concat(c.lookup(c.INSTRUMENT_TYPES, "type_idx", 0), F.lit(" #"), i.cast("string")).alias("INSTR_NAME"),
    c.lookup(c.INSTRUMENT_TYPES, "type_idx", 0).alias("INSTR_TYPE"),
    c.lookup(c.INSTRUMENT_TYPES, "type_idx", 1).alias("VENDOR"),
    F.date_format(c.rdate(i, "inst", "2015-01-01", "2024-12-31"), "yyyyMMdd").alias("INSTALL_DT"),
    c.pick(i, "stat", ["A", "A", "A", "A", "M", "R"]).alias("STATUS_CD"),
)
c.write_bronze(spark, catalog, schema, "01_instruments", instruments, "LabWare instrument master. Status codes A (active), M (maintenance), R (retired); install date as YYYYMMDD string.")

# COMMAND ----------

sm = spark.range(1, N_SAMPLES + 1).withColumnRenamed("id", "sm")
k = F.col("sm")
samples_df = (
    sm.withColumn("proj", c.rint(k, "sp", 1, c.N_PROJ))
    .withColumn("lab", c.rint(k, "sl", 1, c.N_LAB))
    .withColumn("received", c.rdate(k, "sr", c.DATA_START, c.REF_DATE))
)
samples = samples_df.select(
    F.concat(F.lit("S"), F.lpad(k.cast("string"), 7, "0")).alias("SAMPLE_ID"),
    F.concat(F.lit("P"), F.lpad(F.col("proj").cast("string"), 4, "0")).alias("PROJECT_NO"),
    F.concat(F.lit("L"), F.lpad(F.col("lab").cast("string"), 2, "0")).alias("LAB_CODE"),
    c.pick(k, "stype", SAMPLE_TYPES).alias("SAMPLE_TYPE"),
    F.date_format("received", "yyyyMMdd").alias("RECEIVED_DT"),
)
c.write_bronze(spark, catalog, schema, "01_samples", samples, "LabWare samples received per laboratory. Project number P0001 format, dates as YYYYMMDD strings.")

# COMMAND ----------

assay_arr = F.array(*[F.lit(a[0]) for a in ASSAYS])
unit_arr = F.array(*[F.lit(a[1]) for a in ASSAYS])
res = (
    samples_df.crossJoin(spark.range(1, 4).withColumnRenamed("id", "kk"))
    .withColumn("rid", k * 10 + F.col("kk"))
    .withColumn("ai", c.rint(F.col("rid"), "as", 1, len(ASSAYS)))
    .withColumn("instr", F.col("lab") + c.N_LAB * c.rint(F.col("rid"), "ri", 0, c.N_INST // c.N_LAB - 1))
    .withColumn("val", F.lit(75.0) + c.rfrac(F.col("rid"), "rv") * F.lit(50.0))
    .withColumn("completed", F.least(F.date_add("received", c.rint(F.col("rid"), "cd", 1, 14)), F.lit(c.REF_DATE).cast("date")))
)
results = res.select(
    F.col("rid").alias("RESULT_ID"),
    F.concat(F.lit("S"), F.lpad(k.cast("string"), 7, "0")).alias("SAMPLE_ID"),
    F.element_at(assay_arr, F.col("ai")).alias("ASSAY_CODE"),
    F.concat(F.lit("I-"), F.lpad(F.col("instr").cast("string"), 3, "0")).alias("INSTR_NO"),
    F.concat(F.lit("S"), F.lpad(c.rint(F.col("rid"), "an", 1, c.N_SCI).cast("string"), 4, "0")).alias("ANALYST_ID"),
    F.round("val", 3).cast("string").alias("RESULT_VAL"),
    F.element_at(unit_arr, F.col("ai")).alias("UNIT"),
    F.lit("80.0").alias("SPEC_LOW"),
    F.lit("120.0").alias("SPEC_HIGH"),
    F.when((F.col("val") >= 80) & (F.col("val") <= 120), F.lit("Y")).otherwise(F.lit("N")).alias("PASS_FLAG"),
    F.date_format("completed", "yyyyMMdd").alias("COMPLETED_DT"),
)
c.write_bronze(spark, catalog, schema, "01_results", results, "LabWare test results: values as strings, specification limits, Y/N pass flag, analyst id as S0001.")
