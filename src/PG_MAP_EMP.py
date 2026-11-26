# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_EMP
# MAGIC Generated from Informatica mapping `PG_MAP_EMP` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `EMPLOYEES` | `{catalog}.{src_schema}.employees` |
# MAGIC | Target `EMPLOYEES1` | `{catalog}.{tgt_schema}.employees`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.employees (
    EMPLOYEE_ID DECIMAL(38,0) NOT NULL,
    JOB_ID DECIMAL(38,0) NOT NULL,
    FIRST_NAME VARCHAR(30) NOT NULL,
    LAST_NAME VARCHAR(30) NOT NULL,
    ADDRESS1 VARCHAR(72) NOT NULL,
    ADDRESS2 VARCHAR(72),
    CITY VARCHAR(32),
    STATE VARCHAR(2),
    POSTAL_CODE VARCHAR(10),
    HOME_PHONE VARCHAR(30) NOT NULL,
    OFFICE_PHONE VARCHAR(30),
    EMAIL VARCHAR(30)
)""")

# COMMAND ----------

# Source EMPLOYEES
df_employees = spark.read.table(f"{catalog}.{src_schema}.employees")

# Source Qualifier SQ_EMPLOYEES
df_sq_employees = df_employees.select(
    F.col("EMPLOYEE_ID"),
    F.col("JOB_ID"),
    F.col("FIRST_NAME"),
    F.col("LAST_NAME"),
    F.col("ADDRESS1"),
    F.col("ADDRESS2"),
    F.col("CITY"),
    F.col("STATE"),
    F.col("POSTAL_CODE"),
    F.col("HOME_PHONE"),
    F.col("OFFICE_PHONE"),
    F.col("EMAIL"),
)

# Target EMPLOYEES1: Truncate target = YES -> overwrite
df_employees1 = df_sq_employees.select(
    F.col("EMPLOYEE_ID"),
    F.col("JOB_ID"),
    F.col("FIRST_NAME"),
    F.col("LAST_NAME"),
    F.col("ADDRESS1"),
    F.col("ADDRESS2"),
    F.col("CITY"),
    F.col("STATE"),
    F.col("POSTAL_CODE"),
    F.col("HOME_PHONE"),
    F.col("OFFICE_PHONE"),
    F.col("EMAIL"),
)
df_employees1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.employees")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.employees")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.employees")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["EMPLOYEE_ID", "JOB_ID", "FIRST_NAME", "LAST_NAME", "ADDRESS1", "ADDRESS2", "CITY", "STATE", "POSTAL_CODE", "HOME_PHONE", "OFFICE_PHONE", "EMAIL"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
