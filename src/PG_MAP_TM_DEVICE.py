# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_DEVICE
# MAGIC Generated from Informatica mapping `PG_MAP_TM_DEVICE` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_DEVICE1` | `{catalog}.{src_schema}.tm_device` |
# MAGIC | Target `TM_DEVICE` | `{catalog}.{tgt_schema}.tm_device`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_device (
    DEVICE_ID DOUBLE NOT NULL,
    IMEI VARCHAR(20) NOT NULL,
    DEVICE_MODEL VARCHAR(50) NOT NULL,
    OS_NAME VARCHAR(20) NOT NULL,
    PURCHASED_DT TIMESTAMP,
    STATUS VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_DEVICE1
df_tm_device1 = spark.read.table(f"{catalog}.{src_schema}.tm_device")

# Source Qualifier SQ_TM_DEVICE
df_sq_tm_device = df_tm_device1.select(
    F.col("DEVICE_ID"),
    F.col("IMEI"),
    F.col("DEVICE_MODEL"),
    F.col("OS_NAME"),
    F.col("PURCHASED_DT"),
    F.col("STATUS"),
)

# Target TM_DEVICE: Truncate target = YES -> overwrite
df_tm_device = df_sq_tm_device.select(
    F.col("DEVICE_ID"),
    F.col("IMEI"),
    F.col("DEVICE_MODEL"),
    F.col("OS_NAME"),
    F.col("PURCHASED_DT"),
    F.col("STATUS"),
)
df_tm_device.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_device")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_device")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_device")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["DEVICE_ID", "IMEI", "DEVICE_MODEL", "OS_NAME", "PURCHASED_DT", "STATUS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
