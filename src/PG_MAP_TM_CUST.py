# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_CUST
# MAGIC Generated from Informatica mapping `PG_MAP_TM_CUST` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_CUSTOMER` | `{catalog}.{src_schema}.tm_customer` |
# MAGIC | Target `TM_CUSTOMER1` | `{catalog}.{tgt_schema}.tm_customer`, insert (append) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_customer (
    CUSTOMER_ID DOUBLE NOT NULL,
    CUSTOMER_REF VARCHAR(30) NOT NULL,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    DOB TIMESTAMP,
    EMAIL VARCHAR(200),
    KYC_STATUS VARCHAR(10) NOT NULL,
    CREATED_DT TIMESTAMP
)""")

# COMMAND ----------

# Source TM_CUSTOMER
df_tm_customer = spark.read.table(f"{catalog}.{src_schema}.tm_customer")

# Source Qualifier SQ_TM_CUSTOMER
df_sq_tm_customer = df_tm_customer.select(
    F.col("CUSTOMER_ID"),
    F.col("CUSTOMER_REF"),
    F.col("FIRST_NAME"),
    F.col("LAST_NAME"),
    F.col("DOB"),
    F.col("EMAIL"),
    F.col("KYC_STATUS"),
    F.col("CREATED_DT"),
)

# Target TM_CUSTOMER1: Insert only -> append
df_tm_customer1 = df_sq_tm_customer.select(
    F.col("CUSTOMER_ID"),
    F.col("CUSTOMER_REF"),
    F.col("FIRST_NAME"),
    F.col("LAST_NAME"),
    F.col("DOB"),
    F.col("EMAIL"),
    F.col("KYC_STATUS"),
    F.col("CREATED_DT"),
)
df_tm_customer1.write.mode("append").insertInto(f"{catalog}.{tgt_schema}.tm_customer")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_customer")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_customer")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
print(f"source rows={src_cnt} target rows={tgt_cnt} (append load: target accumulates)")
