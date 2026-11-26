# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_PLAN
# MAGIC Generated from Informatica mapping `PG_MAP_TM_PLAN` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_PLAN` | `{catalog}.{src_schema}.tm_plan` |
# MAGIC | Target `TM_PLAN1` | `{catalog}.{tgt_schema}.tm_plan`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_plan (
    PLAN_ID DOUBLE NOT NULL,
    PLAN_CODE VARCHAR(30) NOT NULL,
    PLAN_NAME VARCHAR(100) NOT NULL,
    PLAN_TYPE VARCHAR(20) NOT NULL,
    MONTHLY_FEE DECIMAL(10,2) NOT NULL,
    CURRENCY VARCHAR(3) NOT NULL,
    STATUS VARCHAR(10) NOT NULL,
    CREATED_DT TIMESTAMP
)""")

# COMMAND ----------

# Source TM_PLAN
df_tm_plan = spark.read.table(f"{catalog}.{src_schema}.tm_plan")

# Source Qualifier SQ_TM_PLAN
df_sq_tm_plan = df_tm_plan.select(
    F.col("PLAN_ID"),
    F.col("PLAN_CODE"),
    F.col("PLAN_NAME"),
    F.col("PLAN_TYPE"),
    F.col("MONTHLY_FEE"),
    F.col("CURRENCY"),
    F.col("STATUS"),
    F.col("CREATED_DT"),
)

# Target TM_PLAN1: Truncate target = YES -> overwrite
df_tm_plan1 = df_sq_tm_plan.select(
    F.col("PLAN_ID"),
    F.col("PLAN_CODE"),
    F.col("PLAN_NAME"),
    F.col("PLAN_TYPE"),
    F.col("MONTHLY_FEE"),
    F.col("CURRENCY"),
    F.col("STATUS"),
    F.col("CREATED_DT"),
)
df_tm_plan1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_plan")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_plan")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_plan")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["PLAN_ID", "PLAN_CODE", "PLAN_NAME", "PLAN_TYPE", "MONTHLY_FEE", "CURRENCY", "STATUS", "CREATED_DT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
