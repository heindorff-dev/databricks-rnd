# Databricks notebook source
# Veeva Vault bronze generator: __v / __c field suffixes, 15-character record ids, comma-separated multi-value picklists.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_DOCS = 3000
N_SUBMISSIONS = 240
AGENCIES = ["FDA", "EMA", "MHRA", "PMDA"]
ref = F.lit(c.REF_DATE).cast("date")


def vault_id(prefix, n):
    """15-character Vault style record id."""
    return F.concat(F.lit(prefix), F.lpad(n.cast("string"), 12, "0"))


# COMMAND ----------

d = spark.range(1, N_DOCS + 1).withColumnRenamed("id", "d")
k = F.col("d")
doc_df = (
    d.withColumn("created", c.rdate(k, "dc", "2019-01-01", c.REF_DATE))
    .withColumn("eff", F.date_add("created", c.rint(k, "de", 5, 60)))
    .withColumn("exp", F.date_add("eff", c.rint(k, "dx", 365, 1500)))
)
documents = doc_df.select(
    vault_id("0DC", k).alias("id"),
    F.concat(c.pick(k, "dt", ["Clinical Study Report", "Investigator Brochure", "Protocol", "SOP", "CMC Module"]), F.lit(" v"), c.rint(k, "dv", 1, 9).cast("string")).alias("name__v"),
    c.pick(k, "dty", ["Submission", "Protocol", "Investigator Brochure", "SOP"]).alias("type__v"),
    c.pick(k, "ds", ["Approved", "Approved", "Approved", "In Review", "Draft", "Expired"]).alias("status__v"),
    F.concat(F.lit("PG"), F.lpad(c.rint(k, "dp", 1, c.N_PROG).cast("string"), 3, "0")).alias("program_code__c"),
    c.iso(F.col("created").cast("timestamp")).alias("created_date__v"),
    F.date_format("eff", "yyyy-MM-dd").alias("effective_date__v"),
    F.when(c.rfrac(k, "dxn") < 0.85, F.date_format("exp", "yyyy-MM-dd")).alias("expiration_date__v"),
    F.concat_ws(",", c.pick(k, "c1", ["SE", "DE"]), c.pick(k, "c2", ["US", "GB"]), c.pick(k, "c3", ["NL", "JP"])).alias("countries__c"),
)
c.write_bronze(spark, catalog, schema, "01_documents", documents, "Veeva Vault regulatory documents: __v/__c field names, 15-character ids, countries as a comma-separated picklist.")

# COMMAND ----------

s = spark.range(1, N_SUBMISSIONS + 1).withColumnRenamed("id", "s")
sk = F.col("s")
sub_df = (
    s.withColumn("submitted", c.rdate(sk, "ss", "2020-01-01", c.REF_DATE))
    .withColumn("decision", F.date_add("submitted", c.rint(sk, "sd", 60, 420)))
    .withColumn("decided", (F.col("decision") <= ref) & (c.rfrac(sk, "sdn") < 0.85))
)
submissions = sub_df.select(
    vault_id("0SB", sk).alias("id"),
    F.concat(F.lit("SUB-"), F.lpad(sk.cast("string"), 5, "0")).alias("name__v"),
    F.concat(F.lit("PG"), F.lpad(c.rint(sk, "sp", 1, c.N_PROG).cast("string"), 3, "0")).alias("program_code__c"),
    c.pick(sk, "st", ["IND", "NDA", "MAA", "CTA", "Amendment"]).alias("submission_type__c"),
    c.pick(sk, "sa", AGENCIES).alias("agency__c"),
    F.date_format("submitted", "yyyy-MM-dd").alias("submitted_date__c"),
    F.when(F.col("decided"), F.date_format("decision", "yyyy-MM-dd")).alias("decision_date__c"),
    F.when(F.col("decided"), c.pick(sk, "sr", ["Approved", "Approved", "Approved", "Rejected"])).otherwise(F.lit("Under Review")).alias("status__c"),
)
c.write_bronze(spark, catalog, schema, "01_submissions", submissions, "Veeva Vault regulatory submissions to health agencies: submission type, agency, submitted and decision dates.")

# COMMAND ----------

q = (
    sub_df.crossJoin(spark.range(1, 4).withColumnRenamed("id", "kk"))
    .withColumn("qid", sk * 10 + F.col("kk"))
    .filter(c.rfrac(F.col("qid"), "qp") < 0.7)
    .withColumn("received", F.date_add("submitted", c.rint(F.col("qid"), "qr", 10, 120)))
    .filter(F.col("received") <= ref)
    .withColumn("due", F.date_add("received", 30))
    .withColumn("responded", F.date_add("received", c.rint(F.col("qid"), "qa", 5, 45)))
)
queries = q.select(
    vault_id("0QR", F.col("qid")).alias("id"),
    vault_id("0SB", sk).alias("submission_id__c"),
    c.pick(F.col("qid"), "qc", ["CMC", "Clinical", "Non-clinical", "Labeling", "Quality"]).alias("query_category__c"),
    F.date_format("received", "yyyy-MM-dd").alias("received_date__c"),
    F.date_format("due", "yyyy-MM-dd").alias("due_date__c"),
    F.when(F.col("responded") <= ref, F.date_format("responded", "yyyy-MM-dd")).alias("responded_date__c"),
)
c.write_bronze(spark, catalog, schema, "01_agency_queries", queries, "Veeva Vault health authority queries per submission with received, due and responded dates.")
