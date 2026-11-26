# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_JOBS
# MAGIC Generated from Informatica mapping `PG_MAP_JOBS` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `JOBS` | `{catalog}.{src_schema}.jobs` |
# MAGIC | Target `JOBS1` | `{catalog}.{tgt_schema}.jobs`, truncate + insert (overwrite) |

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("tgt_schema", "datalake")
dbutils.widgets.text("src_schema", "oltp_user")
catalog = dbutils.widgets.get("catalog")
tgt_schema = dbutils.widgets.get("tgt_schema")
src_schema = dbutils.widgets.get("src_schema")

# COMMAND ----------

# Target DDL from the Informatica TARGET definition (created once, never dropped)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{tgt_schema}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.jobs (
    JOB_ID DECIMAL(38,0) NOT NULL,
    JOB_DESC VARCHAR(72) NOT NULL
)""")

# COMMAND ----------

# Source JOBS
df_jobs = spark.read.table(f"{catalog}.{src_schema}.jobs")

# Source Qualifier SQ_JOBS
df_sq_jobs = df_jobs.select(
    F.col("JOB_ID"),
    F.col("JOB_DESC"),
)

# Target JOBS1: Truncate target = YES -> overwrite
df_jobs1 = df_sq_jobs.select(
    F.col("JOB_ID"),
    F.col("JOB_DESC"),
)
df_jobs1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.jobs")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.jobs")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.jobs")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["JOB_ID", "JOB_DESC"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
