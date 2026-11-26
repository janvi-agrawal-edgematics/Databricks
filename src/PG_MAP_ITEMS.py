# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_ITEMS
# MAGIC Generated from Informatica mapping `PG_MAP_ITEMS` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `ITEMS` | `{catalog}.{src_schema}.items` |
# MAGIC | Target `ITEMS1` | `{catalog}.{tgt_schema}.items`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.items (
    ITEM_ID DECIMAL(38,0) NOT NULL,
    ITEM_NAME VARCHAR(72) NOT NULL,
    ITEM_DESC VARCHAR(72),
    PRICE DECIMAL(10,2) NOT NULL,
    WHOLESALE_COST DECIMAL(10,2) NOT NULL,
    DISCONTINUED_FLAG DECIMAL(38,0),
    MANUFACTURER_ID DECIMAL(38,0),
    DISTRIBUTOR_ID DECIMAL(38,0)
)""")

# COMMAND ----------

# Source ITEMS
df_items = spark.read.table(f"{catalog}.{src_schema}.items")

# Source Qualifier SQ_ITEMS
df_sq_items = df_items.select(
    F.col("ITEM_ID"),
    F.col("ITEM_NAME"),
    F.col("ITEM_DESC"),
    F.col("PRICE"),
    F.col("WHOLESALE_COST"),
    F.col("DISCONTINUED_FLAG"),
    F.col("MANUFACTURER_ID"),
    F.col("DISTRIBUTOR_ID"),
)

# Target ITEMS1: Truncate target = YES -> overwrite
df_items1 = df_sq_items.select(
    F.col("ITEM_ID"),
    F.col("ITEM_NAME"),
    F.col("ITEM_DESC"),
    F.col("PRICE"),
    F.col("WHOLESALE_COST"),
    F.col("DISCONTINUED_FLAG"),
    F.col("MANUFACTURER_ID"),
    F.col("DISTRIBUTOR_ID"),
)
df_items1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.items")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.items")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.items")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["ITEM_ID", "ITEM_NAME", "ITEM_DESC", "PRICE", "WHOLESALE_COST", "DISCONTINUED_FLAG", "MANUFACTURER_ID", "DISTRIBUTOR_ID"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
