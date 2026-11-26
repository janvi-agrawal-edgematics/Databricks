# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMCDR_DATA
# MAGIC Generated from Informatica mapping `PG_MAP_TMCDR_DATA` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_CDR_DATA` | `{catalog}.{src_schema}.tm_cdr_data` |
# MAGIC | Target `TM_CDR_DATA1` | `{catalog}.{tgt_schema}.tm_cdr_data`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_cdr_data (
    CDR_DATA_ID DOUBLE NOT NULL,
    MSISDN_ID DOUBLE NOT NULL,
    SESSION_START_TS TIMESTAMP NOT NULL,
    SESSION_END_TS TIMESTAMP NOT NULL,
    DATA_MB DECIMAL(12,3) NOT NULL,
    APN VARCHAR(50),
    CELL_ID DOUBLE,
    CHARGE_AMT DECIMAL(10,4) NOT NULL
)""")

# COMMAND ----------

# Source TM_CDR_DATA
df_tm_cdr_data = spark.read.table(f"{catalog}.{src_schema}.tm_cdr_data")

# Source Qualifier SQ_TM_CDR_DATA
df_sq_tm_cdr_data = df_tm_cdr_data.select(
    F.col("CDR_DATA_ID"),
    F.col("MSISDN_ID"),
    F.col("SESSION_START_TS"),
    F.col("SESSION_END_TS"),
    F.col("DATA_MB"),
    F.col("APN"),
    F.col("CELL_ID"),
    F.col("CHARGE_AMT"),
)

# Target TM_CDR_DATA1: Truncate target = YES -> overwrite
df_tm_cdr_data1 = df_sq_tm_cdr_data.select(
    F.col("CDR_DATA_ID"),
    F.col("MSISDN_ID"),
    F.col("SESSION_START_TS"),
    F.col("SESSION_END_TS"),
    F.col("DATA_MB"),
    F.col("APN"),
    F.col("CELL_ID"),
    F.col("CHARGE_AMT"),
)
df_tm_cdr_data1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_cdr_data")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_cdr_data")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_cdr_data")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["CDR_DATA_ID", "MSISDN_ID", "SESSION_START_TS", "SESSION_END_TS", "DATA_MB", "APN", "CELL_ID", "CHARGE_AMT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
