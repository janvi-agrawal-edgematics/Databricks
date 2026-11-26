# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_DEPT
# MAGIC Generated from Informatica mapping `PG_MAP_DEPT` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `DEPARTMENT` | `{catalog}.{src_schema}.department` |
# MAGIC | Target `DEPARTMENT1` | `{catalog}.{tgt_schema}.department`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.department (
    DEPTID VARCHAR(5) NOT NULL,
    DEPTNAME VARCHAR(30)
)""")

# COMMAND ----------

# Source DEPARTMENT
df_department = spark.read.table(f"{catalog}.{src_schema}.department")

# Source Qualifier SQ_DEPARTMENT
df_sq_department = df_department.select(
    F.col("DEPTID"),
    F.col("DEPTNAME"),
)

# Target DEPARTMENT1: Truncate target = YES -> overwrite
df_department1 = df_sq_department.select(
    F.col("DEPTID"),
    F.col("DEPTNAME"),
)
df_department1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.department")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.department")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.department")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["DEPTID", "DEPTNAME"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
