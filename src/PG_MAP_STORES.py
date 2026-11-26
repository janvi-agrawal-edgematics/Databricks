# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_STORES
# MAGIC Generated from Informatica mapping `PG_MAP_STORES` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `STORES1` | `{catalog}.{src_schema}.stores` |
# MAGIC | Target `STORES` | `{catalog}.{tgt_schema}.stores`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.stores (
    STORE_ID DECIMAL(38,0) NOT NULL,
    STORE_DESC VARCHAR(72) NOT NULL,
    ADDRESS1 VARCHAR(72) NOT NULL,
    ADDRESS2 VARCHAR(72),
    CITY VARCHAR(32) NOT NULL,
    STATE VARCHAR(2) NOT NULL,
    POSTAL_CODE VARCHAR(10) NOT NULL,
    MANAGER_ID DECIMAL(38,0) NOT NULL,
    PHONE VARCHAR(32) NOT NULL
)""")

# COMMAND ----------

# Source STORES1
df_stores1 = spark.read.table(f"{catalog}.{src_schema}.stores")

# Source Qualifier SQ_STORES
df_sq_stores = df_stores1.select(
    F.col("STORE_ID"),
    F.col("STORE_DESC"),
    F.col("ADDRESS1"),
    F.col("ADDRESS2"),
    F.col("CITY"),
    F.col("STATE"),
    F.col("POSTAL_CODE"),
    F.col("MANAGER_ID"),
    F.col("PHONE"),
)

# Target STORES: Truncate target = YES -> overwrite
df_stores = df_sq_stores.select(
    F.col("STORE_ID"),
    F.col("STORE_DESC"),
    F.col("ADDRESS1"),
    F.col("ADDRESS2"),
    F.col("CITY"),
    F.col("STATE"),
    F.col("POSTAL_CODE"),
    F.col("MANAGER_ID"),
    F.col("PHONE"),
)
df_stores.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.stores")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.stores")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.stores")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["STORE_ID", "STORE_DESC", "ADDRESS1", "ADDRESS2", "CITY", "STATE", "POSTAL_CODE", "MANAGER_ID", "PHONE"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
