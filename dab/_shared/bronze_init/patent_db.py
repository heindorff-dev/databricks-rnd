# Databricks notebook source
# Patent database bronze generator: mixed-case column names, legal status codes, several date formats.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_FAMILIES = 150
JURISDICTIONS = ["US", "EP", "JP", "CN", "GB", "DE"]

# COMMAND ----------

f = spark.range(1, N_FAMILIES + 1).withColumnRenamed("id", "f")
fk = F.col("f")
families = f.select(
    F.concat(F.lit("FAM"), F.lpad(fk.cast("string"), 5, "0")).alias("FamilyNo"),
    F.concat(c.pick(fk, "t1", ["Novel", "Improved", "Stable", "Oral", "Injectable"]), F.lit(" "), c.pick(fk, "t2", ["kinase inhibitor", "antibody conjugate", "formulation", "assay method", "delivery device", "biomarker panel"])).alias("Title"),
    F.concat(F.lit("pg_"), F.lpad(c.rint(fk, "pg", 1, c.N_PROG).cast("string"), 3, "0")).alias("program_ref"),
    c.pick(fk, "tc", ["Compound", "Formulation", "Method", "Device"]).alias("TechCategory"),
    F.date_format(c.rdate(fk, "fd", "2012-01-01", "2025-06-30"), "MM/dd/yyyy").alias("EarliestFilingDate"),
)
c.write_bronze(spark, catalog, schema, "01_patent_families", families, "Patent database invention families with MM/dd/yyyy filing dates and program reference in pg_001 format.")

# COMMAND ----------

p = f.crossJoin(spark.range(1, 5).withColumnRenamed("id", "j")).withColumn("pn", fk * 10 + F.col("j"))
pk = F.col("pn")
p = p.filter(c.rfrac(pk, "keep") < 0.55)
filed = c.rdate(pk, "pf", "2012-01-01", "2025-06-30")
patents = p.select(
    F.concat(F.lit("PAT"), F.lpad(pk.cast("string"), 6, "0")).alias("PatentID"),
    F.concat(F.lit("FAM"), F.lpad(fk.cast("string"), 5, "0")).alias("FamilyNo"),
    F.element_at(F.array(*[F.lit(x) for x in JURISDICTIONS]), (F.col("j") + c.rint(fk, "jo", 0, 2)).cast("int")).alias("Jurisdiction"),
    F.concat(F.element_at(F.array(*[F.lit(x) for x in JURISDICTIONS]), (F.col("j") + c.rint(fk, "jo", 0, 2)).cast("int")), F.lit("-"), F.lpad(pk.cast("string"), 7, "0")).alias("ApplicationNumber"),
    F.date_format(filed, "yyyyMMdd").alias("FilingDate"),
    F.when(c.rfrac(pk, "gr") < 0.6, F.date_format(F.date_add(filed, c.rint(pk, "gd", 400, 1500)), "dd.MM.yyyy")).alias("GrantDate"),
    c.pick(pk, "ls", ["G", "G", "G", "P", "A", "L", "W"]).alias("LegalStatus"),
    c.rint(pk, "cl", 5, 40).alias("ClaimCount"),
    F.round(c.rfrac(pk, "ac") * 25000 + 1500, 2).alias("AnnualFeeUSD"),
)
c.write_bronze(spark, catalog, schema, "01_patents", patents, "Patent database patent applications per jurisdiction: LegalStatus codes (G granted, P pending, A abandoned, L lapsed, W withdrawn), yyyyMMdd filing and dd.MM.yyyy grant dates.")

# COMMAND ----------

i = spark.range(1, 321).withColumnRenamed("id", "i")
ik = F.col("i")
inventors = i.select(
    F.concat(F.lit("INV"), F.lpad(ik.cast("string"), 5, "0")).alias("InventorID"),
    F.concat(c.pick(ik, "ln", c.LAST_NAMES), F.lit(", "), c.pick(ik, "fn", c.FIRST_NAMES)).alias("InventorName"),
    F.concat(F.lit("PAT"), F.lpad((c.rint(ik, "ip", 1, N_FAMILIES) * 10 + c.rint(ik, "ij", 1, 4)).cast("string"), 6, "0")).alias("PatentID"),
    c.pick(ik, "ro", ["Lead", "Co-inventor", "Co-inventor"]).alias("Role"),
)
c.write_bronze(spark, catalog, schema, "01_inventors", inventors, "Patent database inventors linked to patents; names in 'Last, First' format.")
