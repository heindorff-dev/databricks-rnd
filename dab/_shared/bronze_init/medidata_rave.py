# Databricks notebook source
# Medidata Rave EDC bronze generator: CDISC-style abbreviated upper-case columns (STUDYID, SUBJID, AETERM...), dd-MMM-yyyy and ISO date strings, Y/N flags.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_TRIALS = 30
SITES_PER_TRIAL = 6
SUBJECTS_PER_SITE = 25
COUNTRIES = ["SE", "DE", "GB", "US", "NL"]
AE_TERMS = ["HEADACHE", "NAUSEA", "FATIGUE", "RASH", "DIZZINESS", "ANAEMIA", "NEUTROPENIA", "HYPERTENSION", "DIARRHOEA", "PYREXIA"]
ref = F.lit(c.REF_DATE).cast("date")

t = F.col("t")
trial_df = (
    spark.range(1, N_TRIALS + 1).withColumnRenamed("id", "t")
    .withColumn("p", (F.pmod(t - 1, F.lit(c.N_PROG)) + 1).cast("int"))
    .withColumn("start", c.rdate(t, "ts", "2021-01-01", "2025-06-30"))
    .withColumn("studyid", F.concat(F.lit("RD-ST-"), F.lpad(t.cast("string"), 3, "0")))
)

# COMMAND ----------

studies = trial_df.select(
    F.col("studyid").alias("STUDYID"),
    F.concat(F.lit("Protocol "), F.lpad(t.cast("string"), 3, "0"), F.lit(": "), c.pick(t, "tt", ["Safety and Efficacy", "Dose Escalation", "Long-term Extension", "Pharmacokinetics"])).alias("STUDYTITLE"),
    c.pick(t, "ph", ["PHASE I", "PHASE II", "PHASE II", "PHASE III"]).alias("PHASE"),
    F.concat(F.lit("PG"), F.lpad(F.col("p").cast("string"), 3, "0")).alias("PRGCODE"),
    F.date_format("start", "dd-MMM-yyyy").alias("STARTDT"),
    c.pick(t, "tst", ["ONGOING", "ONGOING", "COMPLETED", "RECRUITING"]).alias("STUDYSTATUS"),
)
c.write_bronze(spark, catalog, schema, "01_studies", studies, "Medidata Rave studies. CDISC-style upper-case columns, start date as dd-MMM-yyyy.")

# COMMAND ----------

site_df = (
    trial_df.crossJoin(spark.range(1, SITES_PER_TRIAL + 1).withColumnRenamed("id", "j"))
    .withColumn("sitekey", t * 100 + F.col("j"))
    .withColumn("siteid", F.concat(F.lit("S"), F.lpad(t.cast("string"), 3, "0"), F.lpad(F.col("j").cast("string"), 2, "0")))
)
sk = F.col("sitekey")
sites = site_df.select(
    F.col("studyid").alias("STUDYID"),
    F.col("siteid").alias("SITEID"),
    F.concat(c.pick(sk, "sn", ["University Hospital", "Medical Center", "Clinical Research Unit", "Oncology Institute"]), F.lit(" "), c.rint(sk, "snn", 1, 99).cast("string")).alias("SITENAME"),
    c.pick(sk, "sc", COUNTRIES).alias("COUNTRY"),
    F.concat(F.lit("Dr "), c.pick(sk, "pf", c.FIRST_NAMES), F.lit(" "), c.pick(sk, "pl", c.LAST_NAMES)).alias("PINAME"),
)
c.write_bronze(spark, catalog, schema, "01_sites", sites, "Medidata Rave sites per study with country and principal investigator name.")

# COMMAND ----------

subj_df = (
    site_df.crossJoin(spark.range(1, SUBJECTS_PER_SITE + 1).withColumnRenamed("id", "q"))
    .withColumn("subkey", sk * 1000 + F.col("q"))
    .withColumn("enrolled", F.least(F.date_add("start", c.rint(F.col("subkey"), "en", 0, 700)), ref))
)
u = F.col("subkey")
subjects = subj_df.select(
    F.col("studyid").alias("STUDYID"),
    F.concat(F.lpad(t.cast("string"), 3, "0"), F.lit("-"), F.lpad(F.col("j").cast("string"), 2, "0"), F.lit("-"), F.lpad(F.col("q").cast("string"), 3, "0")).alias("SUBJID"),
    F.col("siteid").alias("SITEID"),
    c.rint(u, "by", 1945, 2000).alias("BRTHYR"),
    c.pick(u, "sx", ["M", "F"]).alias("SEX"),
    F.date_format("enrolled", "yyyy-MM-dd").alias("RFSTDTC"),
    c.pick(u, "arm", ["PLACEBO", "TREATMENT", "TREATMENT"]).alias("ARM"),
)
c.write_bronze(spark, catalog, schema, "01_subjects", subjects, "Medidata Rave demographics (DM): subject id, birth year, sex, enrollment date and treatment arm. Subject-level personal data.")

# COMMAND ----------

ae_df = (
    subj_df.crossJoin(spark.range(1, 3).withColumnRenamed("id", "k"))
    .withColumn("akey", u * 10 + F.col("k"))
    .filter(c.rfrac(F.col("akey"), "ap") < 0.3)
    .withColumn("onset", F.date_add("enrolled", c.rint(F.col("akey"), "ao", 1, 120)))
    .filter(F.col("onset") <= ref)
)
a = F.col("akey")
adverse = ae_df.select(
    F.col("studyid").alias("STUDYID"),
    F.concat(F.lpad(t.cast("string"), 3, "0"), F.lit("-"), F.lpad(F.col("j").cast("string"), 2, "0"), F.lit("-"), F.lpad(F.col("q").cast("string"), 3, "0")).alias("SUBJID"),
    F.col("k").cast("int").alias("AESEQ"),
    c.pick(a, "at", AE_TERMS).alias("AETERM"),
    c.pick(a, "av", ["MILD", "MILD", "MODERATE", "MODERATE", "SEVERE"]).alias("AESEV"),
    c.pick(a, "as", ["N", "N", "N", "N", "Y"]).alias("AESER"),
    F.date_format("onset", "yyyy-MM-dd").alias("AESTDTC"),
)
c.write_bronze(spark, catalog, schema, "01_adverse_events", adverse, "Medidata Rave adverse events (AE): CDISC terms, severity, serious flag Y/N and start date.")
