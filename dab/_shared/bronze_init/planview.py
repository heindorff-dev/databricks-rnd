# Databricks notebook source
# Planview (portfolio and budget) bronze generator: PascalCase columns, fiscal period integers, EUR and local amounts.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

COST_CATEGORIES = ["Personnel", "Materials", "CRO Services", "Equipment", "Travel"]
CATEGORY_SHARE = [0.45, 0.15, 0.25, 0.10, 0.05]
LOCAL_FX = {"SE": ("SEK", 11.2), "DE": ("EUR", 1.0), "GB": ("GBP", 0.86), "US": ("USD", 1.08), "NL": ("EUR", 1.0)}
PRIORITIES = ["Critical", "High", "Medium", "Low"]


def display_name(s):
    """Planview shows people as 'Last, First'."""
    return F.concat(c.pick(s, "ln", c.LAST_NAMES), F.lit(", "), c.pick(s, "fn", c.FIRST_NAMES))


proj = c.projects(spark)
n = F.col("n")

# COMMAND ----------

programs = (
    c.programs(spark)
    .select(
        c.program_id(F.col("p")).alias("ProgramCode"),
        F.col("name").alias("ProgramName"),
        F.col("area").alias("TherapeuticArea"),
        F.col("phase").alias("Phase"),
        display_name(F.col("manager_s")).alias("ProgramOwner"),
        c.pick(F.col("p"), "pst", ["Active", "Active", "Active", "On Hold"]).alias("StatusDesc"),
        F.date_format("start_date", "yyyy-MM-dd").alias("StartDt"),
    )
)
c.write_bronze(spark, catalog, schema, "01_programs", programs, "Planview programs (therapeutic area programs). PascalCase columns, dates as ISO strings, owner as 'Last, First'.")

# COMMAND ----------

projects = proj.select(
    c.project_id(n).alias("ProjectCode"),
    c.program_id(F.col("p")).alias("ProgramCode"),
    F.concat(F.col("ptype"), F.lit(" - "), F.lit("Project "), F.lpad(n.cast("string"), 3, "0")).alias("ProjectTitle"),
    F.col("ptype").alias("ProjectType"),
    F.col("status").alias("ProjectStatus"),
    c.pick(n, "pr", PRIORITIES).alias("PriorityLevel"),
    F.col("start_date").alias("StartDate"),
    F.col("planned_end").alias("PlannedFinish"),
    F.col("end_date").alias("ActualFinish"),
    display_name(F.col("manager_s")).alias("ProjectManager"),
    F.col("budget_eur").cast("decimal(18,2)").alias("TotalBudgetEUR"),
)
c.write_bronze(spark, catalog, schema, "01_projects", projects, "Planview projects with schedule, manager and total budget in EUR.")

# COMMAND ----------

milestone_names = ["Kick-off", "Design Freeze", "Mid-point Review", "Data Readout", "Final Report"]
ms = (
    proj.crossJoin(spark.range(1, 6).withColumnRenamed("id", "k"))
    .withColumn("pos", F.col("k").cast("int"))
    .withColumn("planned", F.date_add(F.col("start_date"), (F.datediff("planned_end", "start_date") * F.col("pos") / 5).cast("int")))
    .withColumn("slip", c.rint(n + F.col("pos"), "ms", -10, 45))
    .withColumn("actual_raw", F.date_add(F.col("planned"), F.col("slip")))
    .withColumn("done", F.col("actual_raw") <= F.lit(c.REF_DATE).cast("date"))
)
milestones = ms.select(
    (n * 10 + F.col("pos")).alias("MilestoneId"),
    c.project_id(n).alias("ProjectCode"),
    c.lookup([(x,) for x in milestone_names], "pos", 0).alias("MilestoneName"),
    F.col("planned").alias("PlannedDate"),
    F.when(F.col("done") & (F.col("status") != "Cancelled"), F.col("actual_raw")).alias("ActualDate"),
    F.when(F.col("done"), F.lit("Achieved")).otherwise(F.lit("Open")).alias("MilestoneStatus"),
)
c.write_bronze(spark, catalog, schema, "01_milestones", milestones, "Planview project milestones: planned and actual dates.")

# COMMAND ----------

# One budget line per project, active month and cost category.
active = (
    proj.filter(F.col("start_date") <= F.lit(c.REF_DATE).cast("date"))
    .crossJoin(c.months(spark))
    .filter((F.col("month_start") >= F.trunc("start_date", "month")) & (F.col("month_start") <= F.least(F.col("planned_end"), F.lit(c.REF_DATE).cast("date"))))
)
cat = spark.createDataFrame(list(zip(range(5), COST_CATEGORIES, CATEGORY_SHARE)), "ci int, CostCategory string, share double")
fx_map = F.create_map(*[x for k, v in LOCAL_FX.items() for x in (F.lit(k), F.lit(v[1]))])
cur_map = F.create_map(*[x for k, v in LOCAL_FX.items() for x in (F.lit(k), F.lit(v[0]))])
lines = (
    active.crossJoin(cat)
    .withColumn("key", n * 100 + F.month("month_start") * 10 + F.col("ci"))
    .withColumn("country", c.pick(n, "cty", [x[2] for x in c.LABS]))
    .withColumn("planned", F.col("budget_eur") / F.lit(18.0) * F.col("share"))
    .withColumn("actual", F.col("planned") * (F.lit(0.8) + c.rfrac(F.col("key"), "ac") * F.lit(0.45)))
    .withColumn("forecast", F.col("planned") * (F.lit(0.9) + c.rfrac(F.col("key"), "fc") * F.lit(0.3)))
)
budget_lines = lines.select(
    F.concat(F.lit("BL"), F.col("key").cast("string")).alias("BudgetLineId"),
    c.project_id(n).alias("ProjectCode"),
    (F.year("month_start") * 100 + F.month("month_start")).alias("FiscalPeriod"),
    F.col("CostCategory"),
    F.round("planned", 2).cast("decimal(18,2)").alias("PlannedEUR"),
    F.round("actual", 2).cast("decimal(18,2)").alias("ActualEUR"),
    F.round("forecast", 2).cast("decimal(18,2)").alias("ForecastEUR"),
    F.element_at(cur_map, F.col("country")).alias("LocalCurrency"),
    F.round(F.col("actual") * F.element_at(fx_map, F.col("country")), 2).cast("decimal(18,2)").alias("ActualLocal"),
)
c.write_bronze(spark, catalog, schema, "01_budget_lines", budget_lines, "Planview monthly budget lines per project and cost category. Fiscal period as yyyymm integer, amounts in EUR and local currency.")

# COMMAND ----------

alloc = (
    active.crossJoin(spark.range(1, 4).withColumnRenamed("id", "slot"))
    .withColumn("sci", c.rint(n * 7 + F.col("slot"), "rs", 1, c.N_SCI))
    .withColumn("key", n * 1000 + F.month("month_start") * 10 + F.col("slot").cast("int"))
)
allocations = alloc.select(
    F.col("key").alias("AllocId"),
    c.project_id(n).alias("ProjectCode"),
    F.concat(F.lit("S-"), F.lpad(F.col("sci").cast("string"), 4, "0")).alias("ResourceId"),
    (F.year("month_start") * 100 + F.month("month_start")).alias("FiscalPeriod"),
    F.round(F.lit(0.2) + c.rfrac(F.col("key"), "af") * F.lit(0.8), 2).cast("decimal(5,2)").alias("AllocFTE"),
    F.round(F.lit(0.1) + c.rfrac(F.col("key"), "ax") * F.lit(0.9), 2).cast("decimal(5,2)").alias("ActualFTE"),
)
c.write_bronze(spark, catalog, schema, "01_resource_allocations", allocations, "Planview resource allocations: planned and actual FTE per scientist, project and fiscal period.")

# COMMAND ----------

risks = (
    proj.crossJoin(spark.range(1, 5).withColumnRenamed("id", "k"))
    .withColumn("key", n * 10 + F.col("k").cast("int"))
    .select(
        F.col("key").alias("RiskId"),
        c.project_id(n).alias("ProjectCode"),
        c.pick(F.col("key"), "rt", ["Supplier delay", "Key person dependency", "Assay variability", "Regulatory feedback", "Budget overrun", "Enrollment shortfall"]).alias("RiskTitle"),
        c.rint(F.col("key"), "rp", 1, 5).alias("Probability"),
        c.rint(F.col("key"), "ri", 1, 5).alias("Impact"),
        c.pick(F.col("key"), "rs", ["Open", "Open", "Mitigated", "Closed"]).alias("RiskStatus"),
        c.rdate(F.col("key"), "rd", "2023-01-01", c.REF_DATE).alias("RaisedDate"),
    )
)
c.write_bronze(spark, catalog, schema, "01_risks", risks, "Planview project risk register with probability and impact scores (1-5).")
