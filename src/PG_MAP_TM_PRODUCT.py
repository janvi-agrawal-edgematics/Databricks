# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_PRODUCT
# MAGIC Generated from Informatica mapping `PG_MAP_TM_PRODUCT` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_PRODUCT1` | `{catalog}.{src_schema}.tm_product` |
# MAGIC | Target `TM_PRODUCT` | `{catalog}.{tgt_schema}.tm_product`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_product (
    PRODUCT_ID DOUBLE NOT NULL,
    PRODUCT_CODE VARCHAR(30) NOT NULL,
    PRODUCT_NAME VARCHAR(100) NOT NULL,
    PRODUCT_TYPE VARCHAR(20) NOT NULL,
    PRICE DECIMAL(10,2) NOT NULL,
    CURRENCY VARCHAR(3) NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_PRODUCT1
df_tm_product1 = spark.read.table(f"{catalog}.{src_schema}.tm_product")

# Source Qualifier SQ_TM_PRODUCT
df_sq_tm_product = df_tm_product1.select(
    F.col("PRODUCT_ID"),
    F.col("PRODUCT_CODE"),
    F.col("PRODUCT_NAME"),
    F.col("PRODUCT_TYPE"),
    F.col("PRICE"),
    F.col("CURRENCY"),
    F.col("STATUS"),
)

# Target TM_PRODUCT: Truncate target = YES -> overwrite
df_tm_product = df_sq_tm_product.select(
    F.col("PRODUCT_ID"),
    F.col("PRODUCT_CODE"),
    F.col("PRODUCT_NAME"),
    F.col("PRODUCT_TYPE"),
    F.col("PRICE"),
    F.col("CURRENCY"),
    F.col("STATUS"),
)
df_tm_product.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_product")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_product")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_product")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["PRODUCT_ID", "PRODUCT_CODE", "PRODUCT_NAME", "PRODUCT_TYPE", "PRICE", "CURRENCY", "STATUS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
