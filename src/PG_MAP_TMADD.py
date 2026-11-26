# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMADD
# MAGIC Generated from Informatica mapping `PG_MAP_TMADD` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_ADDRESS1` | `{catalog}.{src_schema}.tm_address` |
# MAGIC | Target `TM_ADDRESS` | `{catalog}.{tgt_schema}.tm_address`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_address (
    ADDRESS_ID DOUBLE NOT NULL,
    CUSTOMER_ID DOUBLE NOT NULL,
    ADDR_LINE1 VARCHAR(200) NOT NULL,
    CITY VARCHAR(50) NOT NULL,
    STATE VARCHAR(50),
    POSTAL_CODE VARCHAR(12),
    COUNTRY VARCHAR(2),
    ADDR_TYPE VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_ADDRESS1
df_tm_address1 = spark.read.table(f"{catalog}.{src_schema}.tm_address")

# Source Qualifier SQ_TM_ADDRESS
df_sq_tm_address = df_tm_address1.select(
    F.col("ADDRESS_ID"),
    F.col("CUSTOMER_ID"),
    F.col("ADDR_LINE1"),
    F.col("CITY"),
    F.col("STATE"),
    F.col("POSTAL_CODE"),
    F.col("COUNTRY"),
    F.col("ADDR_TYPE"),
)

# Target TM_ADDRESS: Truncate target = YES -> overwrite
df_tm_address = df_sq_tm_address.select(
    F.col("ADDRESS_ID"),
    F.col("CUSTOMER_ID"),
    F.col("ADDR_LINE1"),
    F.col("CITY"),
    F.col("STATE"),
    F.col("POSTAL_CODE"),
    F.col("COUNTRY"),
    F.col("ADDR_TYPE"),
)
df_tm_address.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_address")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_address")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_address")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["ADDRESS_ID", "CUSTOMER_ID", "ADDR_LINE1", "CITY", "STATE", "POSTAL_CODE", "COUNTRY", "ADDR_TYPE"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
