# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMCDR_SMS
# MAGIC Generated from Informatica mapping `PG_MAP_TMCDR_SMS` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_CDR_SMS1` | `{catalog}.{src_schema}.tm_cdr_sms` |
# MAGIC | Target `TM_CDR_SMS` | `{catalog}.{tgt_schema}.tm_cdr_sms`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_cdr_sms (
    CDR_SMS_ID DOUBLE NOT NULL,
    MSISDN_ID DOUBLE NOT NULL,
    EVENT_TS TIMESTAMP NOT NULL,
    DEST_NUMBER VARCHAR(20) NOT NULL,
    SMS_TYPE VARCHAR(10) NOT NULL,
    CHARGE_AMT DECIMAL(10,4) NOT NULL
)""")

# COMMAND ----------

# Source TM_CDR_SMS1
df_tm_cdr_sms1 = spark.read.table(f"{catalog}.{src_schema}.tm_cdr_sms")

# Source Qualifier SQ_TM_CDR_SMS
df_sq_tm_cdr_sms = df_tm_cdr_sms1.select(
    F.col("CDR_SMS_ID"),
    F.col("MSISDN_ID"),
    F.col("EVENT_TS"),
    F.col("DEST_NUMBER"),
    F.col("SMS_TYPE"),
    F.col("CHARGE_AMT"),
)

# Target TM_CDR_SMS: Truncate target = YES -> overwrite
df_tm_cdr_sms = df_sq_tm_cdr_sms.select(
    F.col("CDR_SMS_ID"),
    F.col("MSISDN_ID"),
    F.col("EVENT_TS"),
    F.col("DEST_NUMBER"),
    F.col("SMS_TYPE"),
    F.col("CHARGE_AMT"),
)
df_tm_cdr_sms.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_cdr_sms")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_cdr_sms")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_cdr_sms")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["CDR_SMS_ID", "MSISDN_ID", "EVENT_TS", "DEST_NUMBER", "SMS_TYPE", "CHARGE_AMT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
