"""Shared helpers for the R&D bronze data generators.

All data is derived deterministically from integer keys using hashes, so every
source system describes the same fictional R&D organisation (program ``p``,
project ``n``, scientist ``s``, lab ``l``, instrument ``i``) while using its own
key formats, naming conventions and data types.

Canonical business keys (what silver must produce, regardless of the source format):
    programId     PG-001   (p = 1..N_PROG)
    projectId     PRJ-0001 (n = 1..N_PROJ)
    scientistId   SCI-0001 (s = 1..N_SCI)
    labId         LAB-01   (l = 1..N_LAB)
    instrumentId  INS-001  (i = 1..N_INST)
"""
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

GOVERNED_TAG = "Research and Development"
REF_DATE = "2026-09-30"  # "today" for the generated data set
DATA_START = "2023-01-01"

N_PROG = 12
N_PROJ = 120
N_SCI = 200
N_LAB = 8
N_INST = 64
N_COMPOUND = 300

FIRST_NAMES = [
    "Anna", "Lars", "Maria", "Johan", "Sofia", "Erik", "Emma", "Oskar", "Ida", "Nils",
    "Linnea", "Anders", "Elin", "Karl", "Sara", "Mats", "Julia", "Jonas", "Hanna", "Peter",
    "Amira", "Chen", "Priya", "Luca", "Chloe", "Marco", "Ingrid", "Tobias", "Fatima", "Daniel",
]
LAST_NAMES = [
    "Andersson", "Johansson", "Karlsson", "Nilsson", "Eriksson", "Larsson", "Olsson", "Persson",
    "Svensson", "Gustafsson", "Schmidt", "Muller", "Smith", "Jones", "Brown", "de Vries",
    "Jansen", "Rossi", "Dubois", "Nowak", "Khan", "Patel", "Wang", "Garcia", "Hansen",
]

# (name, therapeutic area, current phase)
PROGRAMS = [
    ("Aurora", "Oncology", "Phase 2"), ("Borealis", "Oncology", "Phase 1"),
    ("Cascade", "Immunology", "Phase 3"), ("Delta", "Immunology", "Preclinical"),
    ("Ember", "Neurology", "Phase 2"), ("Fjord", "Neurology", "Discovery"),
    ("Glacier", "Cardiology", "Phase 1"), ("Horizon", "Cardiology", "Phase 3"),
    ("Iris", "Rare Disease", "Phase 2"), ("Juniper", "Rare Disease", "Preclinical"),
    ("Kestrel", "Infectious Disease", "Phase 1"), ("Lumen", "Infectious Disease", "Discovery"),
]

# (name, site, country iso2, timezone)
LABS = [
    ("Discovery Chemistry", "Gothenburg", "SE", "Europe/Stockholm"),
    ("Biologics Lab", "Gothenburg", "SE", "Europe/Stockholm"),
    ("Analytical Development", "Munich", "DE", "Europe/Berlin"),
    ("Formulation Lab", "Munich", "DE", "Europe/Berlin"),
    ("Cell Biology Lab", "Cambridge", "GB", "Europe/London"),
    ("Pharmacology Lab", "Boston", "US", "America/New_York"),
    ("Toxicology Lab", "Boston", "US", "America/New_York"),
    ("Bioanalysis Lab", "Amsterdam", "NL", "Europe/Amsterdam"),
]

# (instrument type, vendor)
INSTRUMENT_TYPES = [
    ("HPLC", "Agilent"), ("LC-MS", "Waters"), ("NMR Spectrometer", "Bruker"),
    ("Plate Reader", "BioTek"), ("Flow Cytometer", "BD Biosciences"), ("PCR Thermocycler", "Bio-Rad"),
    ("Centrifuge", "Eppendorf"), ("Incubator", "Thermo Fisher"),
]
PROJECT_TYPES = ["Target Discovery", "Lead Optimization", "Preclinical Study", "Clinical Study", "Formulation", "Biomarker"]


# ---------------------------------------------------------------- randomness
def _h(key: Column, salt: str) -> Column:
    return F.hash(key, F.lit(salt))


def pick(key: Column, salt: str, values) -> Column:
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.pmod(_h(key, salt), F.lit(len(values))) + 1).cast("int"))


def rint(key: Column, salt: str, lo: int, hi: int) -> Column:
    return (F.pmod(_h(key, salt), F.lit(hi - lo + 1)) + F.lit(lo)).cast("int")


def rfrac(key: Column, salt: str) -> Column:
    return F.pmod(_h(key, salt), F.lit(1000000)) / F.lit(1000000.0)


def rdate(key: Column, salt: str, start: str, end: str) -> Column:
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days
    return F.date_add(F.lit(start).cast("date"), rint(key, salt, 0, days))


def rts(key: Column, salt: str, start: str, end: str) -> Column:
    """Random timestamp between two dates (second resolution)."""
    d = rdate(key, salt, start, end).cast("timestamp")
    return F.timestamp_seconds(F.unix_timestamp(d) + rint(key, salt + "s", 0, 86399))


def iso(ts: Column) -> Column:
    return F.date_format(ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")


def guid(key: Column, salt: str) -> Column:
    m = F.md5(F.concat(F.lit(salt), key.cast("string")))
    return F.concat_ws("-", m.substr(1, 8), m.substr(9, 4), m.substr(13, 4), m.substr(17, 4), m.substr(21, 12))


def lookup(values, idx_col: str, pos: int) -> Column:
    """Pick field ``pos`` of the tuple at 1-based ``idx_col`` from a python list of tuples."""
    arr = F.array(*[F.lit(v[pos]) for v in values])
    return F.element_at(arr, F.col(idx_col).cast("int"))


# ------------------------------------------------------------ canonical keys
def program_id(p: Column) -> Column:
    return F.concat(F.lit("PG-"), F.lpad(p.cast("string"), 3, "0"))


def project_id(n: Column) -> Column:
    return F.concat(F.lit("PRJ-"), F.lpad(n.cast("string"), 4, "0"))


def scientist_id(s: Column) -> Column:
    return F.concat(F.lit("SCI-"), F.lpad(s.cast("string"), 4, "0"))


def lab_id(lab: Column) -> Column:
    return F.concat(F.lit("LAB-"), F.lpad(lab.cast("string"), 2, "0"))


def instrument_id(i: Column) -> Column:
    return F.concat(F.lit("INS-"), F.lpad(i.cast("string"), 3, "0"))


# ------------------------------------------------------------------ universe
def programs(spark: SparkSession) -> DataFrame:
    """Columns: p, name, area, phase, start_date, manager_s."""
    p = F.col("p")
    return (
        spark.range(1, N_PROG + 1).withColumnRenamed("id", "p")
        .withColumn("name", lookup(PROGRAMS, "p", 0))
        .withColumn("area", lookup(PROGRAMS, "p", 1))
        .withColumn("phase", lookup(PROGRAMS, "p", 2))
        .withColumn("start_date", rdate(p, "pgs", "2018-01-01", "2023-06-30"))
        .withColumn("manager_s", rint(p, "pgm", 1, 40))
    )


def projects(spark: SparkSession) -> DataFrame:
    """Columns: n, p (program), ptype, start_date, planned_end, end_date, status, manager_s, budget_eur."""
    n = F.col("n")
    ref = F.lit(REF_DATE).cast("date")
    return (
        spark.range(1, N_PROJ + 1).withColumnRenamed("id", "n")
        .withColumn("p", (F.pmod(n - 1, F.lit(N_PROG)) + 1).cast("int"))
        .withColumn("ptype", pick(n, "pt", PROJECT_TYPES))
        .withColumn("start_date", rdate(n, "ps", "2022-01-01", "2026-03-31"))
        .withColumn("planned_end", F.date_add(F.col("start_date"), rint(n, "pe", 240, 900)))
        .withColumn("manager_s", rint(n, "pm", 1, 40))
        .withColumn("budget_eur", (rint(n, "pb", 200, 6000) * 1000).cast("int"))
        .withColumn(
            "status",
            F.when(F.col("planned_end") < ref, pick(n, "st1", ["Completed", "Completed", "Completed", "Cancelled"]))
            .when(F.col("start_date") > ref, F.lit("Planned"))
            .otherwise(pick(n, "st2", ["Active", "Active", "Active", "On Hold", "At Risk"])),
        )
        .withColumn("end_date", F.when(F.col("status").isin("Completed", "Cancelled"), F.col("planned_end")))
    )


def scientists(spark: SparkSession) -> DataFrame:
    """Columns: s, first_name, last_name, lab_idx, hire_date."""
    s = F.col("s")
    return (
        spark.range(1, N_SCI + 1).withColumnRenamed("id", "s")
        .withColumn("first_name", pick(s, "fn", FIRST_NAMES))
        .withColumn("last_name", pick(s, "ln", LAST_NAMES))
        .withColumn("lab_idx", rint(s, "lb", 1, N_LAB))
        .withColumn("hire_date", rdate(s, "hd", "2012-01-01", "2025-12-31"))
    )


def instruments(spark: SparkSession) -> DataFrame:
    """Columns: i, lab_idx, type_idx."""
    i = F.col("i")
    return (
        spark.range(1, N_INST + 1).withColumnRenamed("id", "i")
        .withColumn("lab_idx", (F.pmod(i - 1, F.lit(N_LAB)) + 1).cast("int"))
        .withColumn("type_idx", rint(i, "it", 1, len(INSTRUMENT_TYPES)))
    )


def months(spark: SparkSession) -> DataFrame:
    """One row per calendar month from DATA_START to REF_DATE."""
    return spark.sql(
        f"SELECT explode(sequence(DATE'{DATA_START}', DATE'{REF_DATE}', INTERVAL 1 MONTH)) AS month_start"
    ).withColumn("month_end", F.last_day("month_start"))


# -------------------------------------------------------------------- writes
def write_bronze(spark: SparkSession, catalog: str, schema: str, table: str, df: DataFrame, comment: str) -> None:
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    df.withColumn("_ingested_at", F.current_timestamp()).write.format("delta").saveAsTable(
        f"`{catalog}`.`{schema}`.`{table}`"
    )
    safe_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {fq} IS '{safe_comment}'")
    spark.sql(f"ALTER TABLE {fq} SET TAGS ('{GOVERNED_TAG}')")
    print(f"wrote {fq}: {spark.table(fq).count()} rows")
