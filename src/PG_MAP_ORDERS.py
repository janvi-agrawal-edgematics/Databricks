# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_ORDERS
# MAGIC Generated from Informatica mapping `PG_MAP_ORDERS` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `ORDERS` | `{catalog}.{src_schema}.orders` |
# MAGIC | Target `ORDERS1` | `{catalog}.{tgt_schema}.orders`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.orders (
    ORDER_ID DECIMAL(38,0) NOT NULL,
    DATE_ENTERED TIMESTAMP NOT NULL,
    DATE_PROMISED TIMESTAMP NOT NULL,
    DATE_SHIPPED TIMESTAMP,
    EMPLOYEE_ID DECIMAL(38,0),
    CUSTOMER_ID DECIMAL(38,0),
    SALES_TAX_RATE DECIMAL(5,4) NOT NULL,
    STORE_ID DECIMAL(38,0)
)""")

# COMMAND ----------

# Source ORDERS
df_orders = spark.read.table(f"{catalog}.{src_schema}.orders")

# Source Qualifier SQ_ORDERS
df_sq_orders = df_orders.select(
    F.col("ORDER_ID"),
    F.col("DATE_ENTERED"),
    F.col("DATE_PROMISED"),
    F.col("DATE_SHIPPED"),
    F.col("EMPLOYEE_ID"),
    F.col("CUSTOMER_ID"),
    F.col("SALES_TAX_RATE"),
    F.col("STORE_ID"),
)

# Target ORDERS1: Truncate target = YES -> overwrite
df_orders1 = df_sq_orders.select(
    F.col("ORDER_ID"),
    F.col("DATE_ENTERED"),
    F.col("DATE_PROMISED"),
    F.col("DATE_SHIPPED"),
    F.col("EMPLOYEE_ID"),
    F.col("CUSTOMER_ID"),
    F.col("SALES_TAX_RATE"),
    F.col("STORE_ID"),
)
df_orders1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.orders")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.orders")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.orders")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["ORDER_ID", "DATE_ENTERED", "DATE_PROMISED", "DATE_SHIPPED", "EMPLOYEE_ID", "CUSTOMER_ID", "SALES_TAX_RATE", "STORE_ID"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
