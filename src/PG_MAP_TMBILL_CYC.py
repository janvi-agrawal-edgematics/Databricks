# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMBILL_CYC
# MAGIC Generated from Informatica mapping `PG_MAP_TMBILL_CYC` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_BILLING_CYCLE` | `{catalog}.{src_schema}.tm_billing_cycle` |
# MAGIC | Target `TM_BILLING_CYCLE1` | `{catalog}.{tgt_schema}.tm_billing_cycle`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_billing_cycle (
    BILLING_CYCLE_ID DOUBLE NOT NULL,
    CYCLE_CODE VARCHAR(20) NOT NULL,
    CYCLE_DAY DECIMAL(2,0) NOT NULL,
    DESCRIPTION VARCHAR(200)
)""")

# COMMAND ----------

# Source TM_BILLING_CYCLE
df_tm_billing_cycle = spark.read.table(f"{catalog}.{src_schema}.tm_billing_cycle")

# Source Qualifier SQ_TM_BILLING_CYCLE
df_sq_tm_billing_cycle = df_tm_billing_cycle.select(
    F.col("BILLING_CYCLE_ID"),
    F.col("CYCLE_CODE"),
    F.col("CYCLE_DAY"),
    F.col("DESCRIPTION"),
)

# Target TM_BILLING_CYCLE1: Truncate target = YES -> overwrite
df_tm_billing_cycle1 = df_sq_tm_billing_cycle.select(
    F.col("BILLING_CYCLE_ID"),
    F.col("CYCLE_CODE"),
    F.col("CYCLE_DAY"),
    F.col("DESCRIPTION"),
)
df_tm_billing_cycle1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_billing_cycle")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_billing_cycle")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_billing_cycle")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["BILLING_CYCLE_ID", "CYCLE_CODE", "CYCLE_DAY", "DESCRIPTION"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
