# Databricks notebook source
# Jira R&D (project tasks) bronze generator: camelCase columns, epoch-millisecond timestamps, nested struct fields.
from pyspark.sql import functions as F

import common as c

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

N_ISSUES = 36000
REF = F.lit(c.REF_DATE).cast("timestamp")


def ms(ts):
    return (F.unix_timestamp(ts) * 1000).cast("long")


def project_key(n):
    return F.concat(F.lit("RD"), F.lpad(n.cast("string"), 4, "0"))


# Issue i belongs to project n; the sequence number within the project makes up the issue key.
base = (
    spark.range(1, N_ISSUES + 1).withColumnRenamed("id", "i")
    .withColumn("n", (F.pmod(F.col("i") - 1, F.lit(c.N_PROJ)) + 1).cast("int"))
    .withColumn("seq", ((F.col("i") - 1) / c.N_PROJ).cast("int") + 1)
    .withColumn("s", c.rint(F.col("i"), "as", 1, c.N_SCI))
    .withColumn("created", c.rts(F.col("i"), "cr", "2023-01-01", "2026-09-01"))
    .withColumn("cycle_days", c.rint(F.col("i"), "cy", 1, 45))
    .withColumn("done_ts", F.timestamp_seconds(F.unix_timestamp("created") + F.col("cycle_days") * 86400))
    .withColumn("is_done", (F.col("done_ts") <= REF) & (c.rfrac(F.col("i"), "dn") < 0.85))
    .withColumn("first", c.pick(F.col("s"), "fn", c.FIRST_NAMES))
    .withColumn("last", c.pick(F.col("s"), "ln", c.LAST_NAMES))
    .withColumn("current_status", F.when(F.col("is_done"), F.lit("Done")).otherwise(c.pick(F.col("i"), "cs", ["To Do", "In Progress", "In Review", "Blocked"])))
    .withColumn("issue_key", F.concat(project_key(F.col("n")), F.lit("-"), F.col("seq").cast("string")))
)

# COMMAND ----------

issues = base.select(
    F.col("issue_key").alias("issueKey"),
    project_key(F.col("n")).alias("projectKey"),
    F.struct(
        c.pick(F.col("i"), "su", ["Prepare assay plate", "Review stability data", "Update protocol", "Order reagents", "Analyse results", "Write study report", "Calibrate instrument", "Validate method"]).alias("summary"),
        F.struct(c.pick(F.col("i"), "ty", ["Task", "Task", "Bug", "Story", "Sub-task"]).alias("name")).alias("issueType"),
        F.struct(
            F.col("current_status").alias("name"),
            F.when(F.col("is_done"), F.lit("done")).otherwise(F.lit("indeterminate")).alias("statusCategory"),
        ).alias("status"),
        F.struct(c.pick(F.col("i"), "pr", ["Highest", "High", "Medium", "Medium", "Low"]).alias("name")).alias("priority"),
        F.struct(
            F.concat(F.col("first"), F.lit(" "), F.col("last")).alias("displayName"),
            F.lower(F.concat(F.col("first"), F.lit("."), F.col("last"), F.lit("@rnd.example.com"))).alias("emailAddress"),
            F.concat(F.lit("acc-"), F.lpad(F.col("s").cast("string"), 4, "0")).alias("accountId"),
        ).alias("assignee"),
        c.rint(F.col("i"), "sp", 1, 13).cast("double").alias("storyPoints"),
    ).alias("fields"),
    ms(F.col("created")).alias("createdMs"),
    ms(F.when(F.col("is_done"), F.col("done_ts"))).alias("resolvedMs"),
    ms(F.coalesce(F.when(F.col("is_done"), F.col("done_ts")), F.col("created"))).alias("updatedMs"),
)
c.write_bronze(spark, catalog, schema, "01_issues", issues, "Jira R&D issues. camelCase columns, nested fields struct (status, assignee, priority) and epoch-millisecond timestamps.")

# COMMAND ----------

def transition(df, frm, to, offset_days):
    return df.select(
        F.col("issue_key").alias("issueKey"),
        F.lit(frm).alias("fromStatus"),
        F.lit(to).alias("toStatus"),
        ms(F.timestamp_seconds(F.unix_timestamp("created") + F.lit(offset_days * 86400))).alias("changedAtMs"),
    )

started = base.filter(F.col("current_status") != "To Do")
transitions = (
    transition(started, "To Do", "In Progress", 1)
    .unionByName(transition(base.filter(F.col("current_status").isin("In Review", "Done")), "In Progress", "In Review", 2))
    .unionByName(
        base.filter("is_done").select(
            F.col("issue_key").alias("issueKey"), F.lit("In Review").alias("fromStatus"), F.lit("Done").alias("toStatus"), ms(F.col("done_ts")).alias("changedAtMs")
        )
    )
)
c.write_bronze(spark, catalog, schema, "01_issue_transitions", transitions, "Jira R&D status changes per issue with epoch-millisecond change time.")

# COMMAND ----------

sprints = (
    spark.range(1, c.N_PROJ * 12 + 1).withColumnRenamed("id", "k")
    .withColumn("n", (F.pmod(F.col("k") - 1, F.lit(c.N_PROJ)) + 1).cast("int"))
    .withColumn("num", ((F.col("k") - 1) / c.N_PROJ).cast("int") + 1)
    .withColumn("start", F.date_add(F.lit("2024-01-01").cast("date"), (F.col("num") - 1) * 28))
    .select(
        F.col("k").alias("sprintId"),
        project_key(F.col("n")).alias("projectKey"),
        F.concat(F.lit("Sprint "), F.col("num").cast("string")).alias("name"),
        F.when(F.col("start") < F.lit("2025-12-01").cast("date"), F.lit("closed")).otherwise(F.lit("active")).alias("state"),
        ms(F.col("start").cast("timestamp")).alias("startMs"),
        ms(F.date_add("start", 27).cast("timestamp")).alias("endMs"),
    )
)
c.write_bronze(spark, catalog, schema, "01_sprints", sprints, "Jira R&D sprints per project with epoch-millisecond start and end.")

# COMMAND ----------

worklogs = (
    base.sample(0.7, seed=7)
    .select(
        F.col("i").alias("worklogId"),
        F.col("issue_key").alias("issueKey"),
        F.struct(
            F.concat(F.col("first"), F.lit(" "), F.col("last")).alias("displayName"),
            F.lower(F.concat(F.col("first"), F.lit("."), F.col("last"), F.lit("@rnd.example.com"))).alias("emailAddress"),
        ).alias("author"),
        (c.rint(F.col("i"), "ts", 1, 16) * 1800).alias("timeSpentSeconds"),
        ms(F.timestamp_seconds(F.unix_timestamp("created") + 86400)).alias("startedMs"),
    )
)
c.write_bronze(spark, catalog, schema, "01_worklogs", worklogs, "Jira R&D worklogs: time spent in seconds per issue and author.")
