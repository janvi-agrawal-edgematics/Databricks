# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMBILL_PROF
# MAGIC Generated from Informatica mapping `PG_MAP_TMBILL_PROF` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_BILLING_PROFILE` | `{catalog}.{src_schema}.tm_billing_profile` |
# MAGIC | Target `TM_BILLING_PROFILE1` | `{catalog}.{tgt_schema}.tm_billing_profile`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_billing_profile (
    BILLING_PROFILE_ID DOUBLE NOT NULL,
    ACCOUNT_ID DOUBLE NOT NULL,
    BILLING_CYCLE_ID DOUBLE NOT NULL,
    BILL_DELIVERY VARCHAR(10) NOT NULL,
    BILL_CURRENCY VARCHAR(3) NOT NULL,
    PAYMENT_TERMS_DAYS DECIMAL(3,0) NOT NULL
)""")

# COMMAND ----------

# Source TM_BILLING_PROFILE
df_tm_billing_profile = spark.read.table(f"{catalog}.{src_schema}.tm_billing_profile")

# Source Qualifier SQ_TM_BILLING_PROFILE
df_sq_tm_billing_profile = df_tm_billing_profile.select(
    F.col("BILLING_PROFILE_ID"),
    F.col("ACCOUNT_ID"),
    F.col("BILLING_CYCLE_ID"),
    F.col("BILL_DELIVERY"),
    F.col("BILL_CURRENCY"),
    F.col("PAYMENT_TERMS_DAYS"),
)

# Target TM_BILLING_PROFILE1: Truncate target = YES -> overwrite
df_tm_billing_profile1 = df_sq_tm_billing_profile.select(
    F.col("BILLING_PROFILE_ID"),
    F.col("ACCOUNT_ID"),
    F.col("BILLING_CYCLE_ID"),
    F.col("BILL_DELIVERY"),
    F.col("BILL_CURRENCY"),
    F.col("PAYMENT_TERMS_DAYS"),
)
df_tm_billing_profile1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_billing_profile")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_billing_profile")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_billing_profile")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["BILLING_PROFILE_ID", "ACCOUNT_ID", "BILLING_CYCLE_ID", "BILL_DELIVERY", "BILL_CURRENCY", "PAYMENT_TERMS_DAYS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
