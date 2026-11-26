# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMACCOUNT
# MAGIC Generated from Informatica mapping `PG_MAP_TMACCOUNT` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_ACCOUNT` | `{catalog}.{src_schema}.tm_account` |
# MAGIC | Target `TM_ACCOUNT1` | `{catalog}.{tgt_schema}.tm_account`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_account (
    ACCOUNT_ID DOUBLE NOT NULL,
    ACCOUNT_NO VARCHAR(30) NOT NULL,
    CUSTOMER_ID DOUBLE NOT NULL,
    ACCOUNT_TYPE VARCHAR(20) NOT NULL,
    STATUS VARCHAR(10) NOT NULL,
    OPENED_DT TIMESTAMP
)""")

# COMMAND ----------

# Source TM_ACCOUNT
df_tm_account = spark.read.table(f"{catalog}.{src_schema}.tm_account")

# Source Qualifier SQ_TM_ACCOUNT
df_sq_tm_account = df_tm_account.select(
    F.col("ACCOUNT_ID"),
    F.col("ACCOUNT_NO"),
    F.col("CUSTOMER_ID"),
    F.col("ACCOUNT_TYPE"),
    F.col("STATUS"),
    F.col("OPENED_DT"),
)

# Target TM_ACCOUNT1: Truncate target = YES -> overwrite
df_tm_account1 = df_sq_tm_account.select(
    F.col("ACCOUNT_ID"),
    F.col("ACCOUNT_NO"),
    F.col("CUSTOMER_ID"),
    F.col("ACCOUNT_TYPE"),
    F.col("STATUS"),
    F.col("OPENED_DT"),
)
df_tm_account1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_account")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_account")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_account")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["ACCOUNT_ID", "ACCOUNT_NO", "CUSTOMER_ID", "ACCOUNT_TYPE", "STATUS", "OPENED_DT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
