# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_PLAN_RATE
# MAGIC Generated from Informatica mapping `PG_MAP_PLAN_RATE` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_PLAN_RATE` | `{catalog}.{src_schema}.tm_plan_rate` |
# MAGIC | Target `TM_PLAN_RATE1` | `{catalog}.{tgt_schema}.tm_plan_rate`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_plan_rate (
    PLAN_RATE_ID DOUBLE NOT NULL,
    PLAN_ID DOUBLE NOT NULL,
    RATE_TYPE VARCHAR(20) NOT NULL,
    INCLUDED_QTY DECIMAL(12,2) NOT NULL,
    OVERAGE_RATE DECIMAL(10,4) NOT NULL
)""")

# COMMAND ----------

# Source TM_PLAN_RATE
df_tm_plan_rate = spark.read.table(f"{catalog}.{src_schema}.tm_plan_rate")

# Source Qualifier SQ_TM_PLAN_RATE
df_sq_tm_plan_rate = df_tm_plan_rate.select(
    F.col("PLAN_RATE_ID"),
    F.col("PLAN_ID"),
    F.col("RATE_TYPE"),
    F.col("INCLUDED_QTY"),
    F.col("OVERAGE_RATE"),
)

# Target TM_PLAN_RATE1: Truncate target = YES -> overwrite
df_tm_plan_rate1 = df_sq_tm_plan_rate.select(
    F.col("PLAN_RATE_ID"),
    F.col("PLAN_ID"),
    F.col("RATE_TYPE"),
    F.col("INCLUDED_QTY"),
    F.col("OVERAGE_RATE"),
)
df_tm_plan_rate1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_plan_rate")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_plan_rate")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_plan_rate")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["PLAN_RATE_ID", "PLAN_ID", "RATE_TYPE", "INCLUDED_QTY", "OVERAGE_RATE"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
