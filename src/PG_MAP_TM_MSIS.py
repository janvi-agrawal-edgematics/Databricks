# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_MSIS
# MAGIC Generated from Informatica mapping `PG_MAP_TM_MSIS` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_MSISDN1` | `{catalog}.{src_schema}.tm_msisdn` |
# MAGIC | Target `TM_MSISDN` | `{catalog}.{tgt_schema}.tm_msisdn`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_msisdn (
    MSISDN_ID DOUBLE NOT NULL,
    MSISDN VARCHAR(20) NOT NULL,
    SUBSCRIPTION_ID DOUBLE NOT NULL,
    SIM_ID DOUBLE NOT NULL,
    ACTIVATED_DT TIMESTAMP NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_MSISDN1
df_tm_msisdn1 = spark.read.table(f"{catalog}.{src_schema}.tm_msisdn")

# Source Qualifier SQ_TM_MSISDN
df_sq_tm_msisdn = df_tm_msisdn1.select(
    F.col("MSISDN_ID"),
    F.col("MSISDN"),
    F.col("SUBSCRIPTION_ID"),
    F.col("SIM_ID"),
    F.col("ACTIVATED_DT"),
    F.col("STATUS"),
)

# Target TM_MSISDN: Truncate target = YES -> overwrite
df_tm_msisdn = df_sq_tm_msisdn.select(
    F.col("MSISDN_ID"),
    F.col("MSISDN"),
    F.col("SUBSCRIPTION_ID"),
    F.col("SIM_ID"),
    F.col("ACTIVATED_DT"),
    F.col("STATUS"),
)
df_tm_msisdn.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_msisdn")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_msisdn")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_msisdn")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["MSISDN_ID", "MSISDN", "SUBSCRIPTION_ID", "SIM_ID", "ACTIVATED_DT", "STATUS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
