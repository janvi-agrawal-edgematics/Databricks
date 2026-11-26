# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_DIST
# MAGIC Generated from Informatica mapping `PG_MAP_DIST` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `DISTRIBUTORS` | `{catalog}.{src_schema}.distributors` |
# MAGIC | Target `DISTRIBUTORS1` | `{catalog}.{tgt_schema}.distributors`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.distributors (
    DISTRIBUTOR_ID DECIMAL(38,0) NOT NULL,
    DISTRIBUTOR_NAME VARCHAR(48) NOT NULL,
    CONTACT_PERSON VARCHAR(48),
    CONTACT_PHONE VARCHAR(32),
    CONTACT_EMAIL VARCHAR(72),
    FIRST_NAME VARCHAR(48),
    LAST_NAME VARCHAR(48)
)""")

# COMMAND ----------

# Source DISTRIBUTORS
df_distributors = spark.read.table(f"{catalog}.{src_schema}.distributors")

# Source Qualifier SQ_DISTRIBUTORS
df_sq_distributors = df_distributors.select(
    F.col("DISTRIBUTOR_ID"),
    F.col("DISTRIBUTOR_NAME"),
    F.col("CONTACT_PERSON"),
    F.col("CONTACT_PHONE"),
    F.col("CONTACT_EMAIL"),
    F.col("FIRST_NAME"),
    F.col("LAST_NAME"),
)

# Target DISTRIBUTORS1: Truncate target = YES -> overwrite
df_distributors1 = df_sq_distributors.select(
    F.col("DISTRIBUTOR_ID"),
    F.col("DISTRIBUTOR_NAME"),
    F.col("CONTACT_PERSON"),
    F.col("CONTACT_PHONE"),
    F.col("CONTACT_EMAIL"),
    F.col("FIRST_NAME"),
    F.col("LAST_NAME"),
)
df_distributors1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.distributors")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.distributors")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.distributors")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["DISTRIBUTOR_ID", "DISTRIBUTOR_NAME", "CONTACT_PERSON", "CONTACT_PHONE", "CONTACT_EMAIL", "FIRST_NAME", "LAST_NAME"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
