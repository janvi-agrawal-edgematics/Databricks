# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_SIM
# MAGIC Generated from Informatica mapping `PG_MAP_TM_SIM` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_SIM1` | `{catalog}.{src_schema}.tm_sim` |
# MAGIC | Target `TM_SIM` | `{catalog}.{tgt_schema}.tm_sim`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_sim (
    SIM_ID DOUBLE NOT NULL,
    SIM_ICCID VARCHAR(25) NOT NULL,
    SIM_TYPE VARCHAR(10) NOT NULL,
    ISSUED_DT TIMESTAMP NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_SIM1
df_tm_sim1 = spark.read.table(f"{catalog}.{src_schema}.tm_sim")

# Source Qualifier SQ_TM_SIM
df_sq_tm_sim = df_tm_sim1.select(
    F.col("SIM_ID"),
    F.col("SIM_ICCID"),
    F.col("SIM_TYPE"),
    F.col("ISSUED_DT"),
    F.col("STATUS"),
)

# Target TM_SIM: Truncate target = YES -> overwrite
df_tm_sim = df_sq_tm_sim.select(
    F.col("SIM_ID"),
    F.col("SIM_ICCID"),
    F.col("SIM_TYPE"),
    F.col("ISSUED_DT"),
    F.col("STATUS"),
)
df_tm_sim.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_sim")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_sim")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_sim")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["SIM_ID", "SIM_ICCID", "SIM_TYPE", "ISSUED_DT", "STATUS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
