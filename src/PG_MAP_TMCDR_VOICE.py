# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMCDR_VOICE
# MAGIC Generated from Informatica mapping `PG_MAP_TMCDR_VOICE` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_CDR_VOICE` | `{catalog}.{src_schema}.tm_cdr_voice` |
# MAGIC | Target `TM_CDR_VOICE1` | `{catalog}.{tgt_schema}.tm_cdr_voice`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_cdr_voice (
    CDR_VOICE_ID DOUBLE NOT NULL,
    MSISDN_ID DOUBLE NOT NULL,
    EVENT_TS TIMESTAMP NOT NULL,
    CALLED_NUMBER VARCHAR(20) NOT NULL,
    DURATION_SEC DECIMAL(10,0) NOT NULL,
    CELL_ID DOUBLE,
    CHARGE_AMT DECIMAL(10,4) NOT NULL
)""")

# COMMAND ----------

# Source TM_CDR_VOICE
df_tm_cdr_voice = spark.read.table(f"{catalog}.{src_schema}.tm_cdr_voice")

# Source Qualifier SQ_TM_CDR_VOICE
df_sq_tm_cdr_voice = df_tm_cdr_voice.select(
    F.col("CDR_VOICE_ID"),
    F.col("MSISDN_ID"),
    F.col("EVENT_TS"),
    F.col("CALLED_NUMBER"),
    F.col("DURATION_SEC"),
    F.col("CELL_ID"),
    F.col("CHARGE_AMT"),
)

# Target TM_CDR_VOICE1: Truncate target = YES -> overwrite
df_tm_cdr_voice1 = df_sq_tm_cdr_voice.select(
    F.col("CDR_VOICE_ID"),
    F.col("MSISDN_ID"),
    F.col("EVENT_TS"),
    F.col("CALLED_NUMBER"),
    F.col("DURATION_SEC"),
    F.col("CELL_ID"),
    F.col("CHARGE_AMT"),
)
df_tm_cdr_voice1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_cdr_voice")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_cdr_voice")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_cdr_voice")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["CDR_VOICE_ID", "MSISDN_ID", "EVENT_TS", "CALLED_NUMBER", "DURATION_SEC", "CELL_ID", "CHARGE_AMT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
