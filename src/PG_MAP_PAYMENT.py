# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_PAYMENT
# MAGIC Generated from Informatica mapping `PG_MAP_PAYMENT` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_PAYMENT1` | `{catalog}.{src_schema}.tm_payment` |
# MAGIC | Target `TM_PAYMENT` | `{catalog}.{tgt_schema}.tm_payment`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_payment (
    PAYMENT_ID DOUBLE NOT NULL,
    INVOICE_ID DOUBLE NOT NULL,
    PAID_DT TIMESTAMP NOT NULL,
    PAID_AMT DECIMAL(12,2) NOT NULL,
    METHOD VARCHAR(10) NOT NULL,
    REFERENCE_NO VARCHAR(40)
)""")

# COMMAND ----------

# Source TM_PAYMENT1
df_tm_payment1 = spark.read.table(f"{catalog}.{src_schema}.tm_payment")

# Source Qualifier SQ_TM_PAYMENT
df_sq_tm_payment = df_tm_payment1.select(
    F.col("PAYMENT_ID"),
    F.col("INVOICE_ID"),
    F.col("PAID_DT"),
    F.col("PAID_AMT"),
    F.col("METHOD"),
    F.col("REFERENCE_NO"),
)

# Target TM_PAYMENT: Truncate target = YES -> overwrite
df_tm_payment = df_sq_tm_payment.select(
    F.col("PAYMENT_ID"),
    F.col("INVOICE_ID"),
    F.col("PAID_DT"),
    F.col("PAID_AMT"),
    F.col("METHOD"),
    F.col("REFERENCE_NO"),
)
df_tm_payment.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_payment")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_payment")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_payment")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["PAYMENT_ID", "INVOICE_ID", "PAID_DT", "PAID_AMT", "METHOD", "REFERENCE_NO"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
