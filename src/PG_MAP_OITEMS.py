# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_OITEMS
# MAGIC Generated from Informatica mapping `PG_MAP_OITEMS` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `ORDER_ITEMS1` | `{catalog}.{src_schema}.order_items` |
# MAGIC | Target `ORDER_ITEMS` | `{catalog}.{tgt_schema}.order_items`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.order_items (
    ORDER_ID DECIMAL(38,0) NOT NULL,
    ITEM_ID DECIMAL(38,0) NOT NULL,
    QUANTITY DECIMAL(38,0) NOT NULL,
    DISCOUNT DECIMAL(10,2) NOT NULL
)""")

# COMMAND ----------

# Source ORDER_ITEMS1
df_order_items1 = spark.read.table(f"{catalog}.{src_schema}.order_items")

# Source Qualifier SQ_ORDER_ITEMS
df_sq_order_items = df_order_items1.select(
    F.col("ORDER_ID"),
    F.col("ITEM_ID"),
    F.col("QUANTITY"),
    F.col("DISCOUNT"),
)

# Target ORDER_ITEMS: Truncate target = YES -> overwrite
df_order_items = df_sq_order_items.select(
    F.col("ORDER_ID"),
    F.col("ITEM_ID"),
    F.col("QUANTITY"),
    F.col("DISCOUNT"),
)
df_order_items.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.order_items")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.order_items")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.order_items")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["ORDER_ID", "ITEM_ID", "QUANTITY", "DISCOUNT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
