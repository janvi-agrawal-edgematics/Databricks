# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_SUB
# MAGIC Generated from Informatica mapping `PG_MAP_TM_SUB` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_SUBSCRIPTION` | `{catalog}.{src_schema}.tm_subscription` |
# MAGIC | Target `TM_SUBSCRIPTION1` | `{catalog}.{tgt_schema}.tm_subscription`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_subscription (
    SUBSCRIPTION_ID DOUBLE NOT NULL,
    ACCOUNT_ID DOUBLE NOT NULL,
    PLAN_ID DOUBLE NOT NULL,
    START_DT TIMESTAMP NOT NULL,
    END_DT TIMESTAMP,
    STATUS VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_SUBSCRIPTION
df_tm_subscription = spark.read.table(f"{catalog}.{src_schema}.tm_subscription")

# Source Qualifier SQ_TM_SUBSCRIPTION
df_sq_tm_subscription = df_tm_subscription.select(
    F.col("SUBSCRIPTION_ID"),
    F.col("ACCOUNT_ID"),
    F.col("PLAN_ID"),
    F.col("START_DT"),
    F.col("END_DT"),
    F.col("STATUS"),
)

# Target TM_SUBSCRIPTION1: Truncate target = YES -> overwrite
df_tm_subscription1 = df_sq_tm_subscription.select(
    F.col("SUBSCRIPTION_ID"),
    F.col("ACCOUNT_ID"),
    F.col("PLAN_ID"),
    F.col("START_DT"),
    F.col("END_DT"),
    F.col("STATUS"),
)
df_tm_subscription1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_subscription")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_subscription")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_subscription")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["SUBSCRIPTION_ID", "ACCOUNT_ID", "PLAN_ID", "START_DT", "END_DT", "STATUS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
