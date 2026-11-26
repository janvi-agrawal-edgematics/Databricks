# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_INVOICE_LINE
# MAGIC Generated from Informatica mapping `PG_MAP_INVOICE_LINE` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_INVOICE_LINE1` | `{catalog}.{src_schema}.tm_invoice_line` |
# MAGIC | Target `TM_INVOICE_LINE` | `{catalog}.{tgt_schema}.tm_invoice_line`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_invoice_line (
    INVOICE_LINE_ID DOUBLE NOT NULL,
    INVOICE_ID DOUBLE NOT NULL,
    LINE_TYPE VARCHAR(20) NOT NULL,
    DESCRIPTION VARCHAR(200),
    QTY DECIMAL(12,3),
    UNIT_PRICE DECIMAL(12,4) NOT NULL,
    LINE_AMT DECIMAL(12,2) NOT NULL
)""")

# COMMAND ----------

# Source TM_INVOICE_LINE1
df_tm_invoice_line1 = spark.read.table(f"{catalog}.{src_schema}.tm_invoice_line")

# Source Qualifier SQ_TM_INVOICE_LINE
df_sq_tm_invoice_line = df_tm_invoice_line1.select(
    F.col("INVOICE_LINE_ID"),
    F.col("INVOICE_ID"),
    F.col("LINE_TYPE"),
    F.col("DESCRIPTION"),
    F.col("QTY"),
    F.col("UNIT_PRICE"),
    F.col("LINE_AMT"),
)

# Target TM_INVOICE_LINE: Truncate target = YES -> overwrite
df_tm_invoice_line = df_sq_tm_invoice_line.select(
    F.col("INVOICE_LINE_ID"),
    F.col("INVOICE_ID"),
    F.col("LINE_TYPE"),
    F.col("DESCRIPTION"),
    F.col("QTY"),
    F.col("UNIT_PRICE"),
    F.col("LINE_AMT"),
)
df_tm_invoice_line.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_invoice_line")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_invoice_line")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_invoice_line")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["INVOICE_LINE_ID", "INVOICE_ID", "LINE_TYPE", "DESCRIPTION", "QTY", "UNIT_PRICE", "LINE_AMT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
