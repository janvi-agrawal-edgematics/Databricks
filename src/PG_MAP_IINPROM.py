# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_IINPROM
# MAGIC Generated from Informatica mapping `PG_MAP_IINPROM` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `ITEMS_IN_PROMOTIONS1` | `{catalog}.{src_schema}.items_in_promotions` |
# MAGIC | Target `ITEMS_IN_PROMOTIONS` | `{catalog}.{tgt_schema}.items_in_promotions`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.items_in_promotions (
    PROMOTION_ID DECIMAL(38,0) NOT NULL,
    ITEM_ID DECIMAL(38,0) NOT NULL,
    COMMENTS VARCHAR(72),
    DISCOUNT DECIMAL(10,2)
)""")

# COMMAND ----------

# Source ITEMS_IN_PROMOTIONS1
df_items_in_promotions1 = spark.read.table(f"{catalog}.{src_schema}.items_in_promotions")

# Source Qualifier SQ_ITEMS_IN_PROMOTIONS
df_sq_items_in_promotions = df_items_in_promotions1.select(
    F.col("PROMOTION_ID"),
    F.col("ITEM_ID"),
    F.col("COMMENTS"),
    F.col("DISCOUNT"),
)

# Target ITEMS_IN_PROMOTIONS: Truncate target = YES -> overwrite
df_items_in_promotions = df_sq_items_in_promotions.select(
    F.col("PROMOTION_ID"),
    F.col("ITEM_ID"),
    F.col("COMMENTS"),
    F.col("DISCOUNT"),
)
df_items_in_promotions.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.items_in_promotions")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.items_in_promotions")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.items_in_promotions")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["PROMOTION_ID", "ITEM_ID", "COMMENTS", "DISCOUNT"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
