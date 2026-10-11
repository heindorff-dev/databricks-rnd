# Databricks notebook source
# Benchling ELN bronze generator: snake_case columns, UUID keys, ISO-8601 string timestamps, JSON string fields, 'prj_0001' style references.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_EXPERIMENTS = 8000
ASSAYS = ["binding_assay", "cell_viability", "pk_study", "western_blot", "elisa"]
ref_ts = F.lit(c.REF_DATE).cast("timestamp")

# COMMAND ----------

sci = c.scientists(spark)
s = F.col("s")
users = sci.select(
    F.concat(F.lit("sci_"), F.lpad(s.cast("string"), 4, "0")).alias("id"),
    F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("display_name"),
    F.lower(F.concat(F.col("first_name"), F.lit("."), F.regexp_replace("last_name", " ", ""), F.lit("@example-pharma.com"))).alias("email"),
    F.concat(F.lit("lab_"), F.lpad(F.col("lab_idx").cast("string"), 2, "0")).alias("lab_ref"),
)
c.write_bronze(spark, catalog, schema, "01_users", users, "Benchling users (scientists). snake_case columns, 'sci_0001' style ids, name and email.")

# COMMAND ----------

cp = spark.range(1, c.N_COMPOUND + 1).withColumnRenamed("id", "cp")
k = F.col("cp")
compounds = cp.select(
    c.guid(k, "cmp").alias("id"),
    F.concat(F.lit("CMP-"), F.lpad(k.cast("string"), 6, "0")).alias("registry_id"),
    F.concat(F.lit("Compound-"), c.pick(k, "cn", ["Alpha", "Beta", "Gamma", "Delta", "Sigma"]), F.lit("-"), k.cast("string")).alias("name"),
    F.round(F.lit(180.0) + c.rfrac(k, "mw") * F.lit(620.0), 3).alias("molecular_weight"),
    F.concat(F.lit("pg_"), F.lpad(c.rint(k, "cpg", 1, c.N_PROG).cast("string"), 3, "0")).alias("program_ref"),
)
c.write_bronze(spark, catalog, schema, "01_compounds", compounds, "Benchling registered compounds with registry id, molecular weight and program reference.")

# COMMAND ----------

ex = (
    spark.range(1, N_EXPERIMENTS + 1).withColumnRenamed("id", "e")
    .withColumn("created", c.rts(F.col("e"), "ec", c.DATA_START, c.REF_DATE))
    .withColumn("status", c.pick(F.col("e"), "es", ["completed", "completed", "completed", "failed", "in_progress", "planned"]))
    .withColumn("done_ts", F.least(F.timestamp_seconds(F.unix_timestamp("created") + c.rint(F.col("e"), "ed", 1, 60) * 86400), ref_ts))
    .withColumn("proj", c.rint(F.col("e"), "ep", 1, c.N_PROJ))
    .withColumn("sci", c.rint(F.col("e"), "esc", 1, c.N_SCI))
    .withColumn("assay", c.pick(F.col("e"), "ea", ASSAYS))
)
e = F.col("e")
experiments = ex.select(
    c.guid(e, "exp").alias("id"),
    F.concat(F.lit("prj_"), F.lpad(F.col("proj").cast("string"), 4, "0")).alias("project_ref"),
    F.concat(F.upper(F.substring("assay", 1, 3)), F.lit("-"), F.lpad(e.cast("string"), 5, "0")).alias("name"),
    F.concat(F.lit("sci_"), F.lpad(F.col("sci").cast("string"), 4, "0")).alias("creator_ref"),
    F.col("status"),
    c.iso(F.col("created")).alias("created_at"),
    F.when(F.col("status").isin("completed", "failed"), c.iso(F.col("done_ts"))).alias("completed_at"),
    F.to_json(F.struct(F.col("assay").alias("assay_type"), c.rint(e, "er", 1, 3).alias("replicates"))).alias("fields_json"),
)
c.write_bronze(spark, catalog, schema, "01_experiments", experiments, "Benchling experiments: UUID ids, ISO-8601 timestamps, project and creator references, custom fields as a JSON string.")

# COMMAND ----------

res = (
    ex.filter(F.col("status").isin("completed", "failed"))
    .crossJoin(spark.range(1, 3).withColumnRenamed("id", "kk"))
    .withColumn("rid", e * 10 + F.col("kk"))
    .withColumn("cmp", c.rint(F.col("rid"), "rc", 1, c.N_COMPOUND))
)
results = res.select(
    c.guid(F.col("rid"), "res").alias("id"),
    c.guid(e, "exp").alias("experiment_id"),
    F.concat(F.lit("CMP-"), F.lpad(F.col("cmp").cast("string"), 6, "0")).alias("compound_registry_id"),
    F.when(F.col("status") == "failed", F.lit("failure"))
    .otherwise(c.pick(F.col("rid"), "ro", ["success", "success", "success", "partial", "failure"])).alias("outcome"),
    F.round(c.rfrac(F.col("rid"), "rv") * F.lit(100.0), 4).alias("measured_value"),
    c.iso(F.col("done_ts")).alias("measured_at"),
)
c.write_bronze(spark, catalog, schema, "01_experiment_results", results, "Benchling experiment results per compound: outcome label, measured value and ISO-8601 timestamp.")
