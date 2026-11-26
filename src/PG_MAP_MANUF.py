# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_MANUF
# MAGIC Generated from Informatica mapping `PG_MAP_MANUF` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `MANUFACTURERS1` | `{catalog}.{src_schema}.manufacturers` |
# MAGIC | Target `MANUFACTURERS` | `{catalog}.{tgt_schema}.manufacturers`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.manufacturers (
    MANUFACTURER_ID DECIMAL(38,0) NOT NULL,
    MANUFACTURER_NAME VARCHAR(48) NOT NULL
)""")

# COMMAND ----------

# Source MANUFACTURERS1
df_manufacturers1 = spark.read.table(f"{catalog}.{src_schema}.manufacturers")

# Source Qualifier SQ_MANUFACTURERS
df_sq_manufacturers = df_manufacturers1.select(
    F.col("MANUFACTURER_ID"),
    F.col("MANUFACTURER_NAME"),
)

# Target MANUFACTURERS: Truncate target = YES -> overwrite
df_manufacturers = df_sq_manufacturers.select(
    F.col("MANUFACTURER_ID"),
    F.col("MANUFACTURER_NAME"),
)
df_manufacturers.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.manufacturers")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.manufacturers")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.manufacturers")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["MANUFACTURER_ID", "MANUFACTURER_NAME"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
