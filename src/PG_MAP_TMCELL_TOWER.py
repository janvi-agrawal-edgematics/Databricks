# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TMCELL_TOWER
# MAGIC Generated from Informatica mapping `PG_MAP_TMCELL_TOWER` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_CELL_TOWER1` | `{catalog}.{src_schema}.tm_cell_tower` |
# MAGIC | Target `TM_CELL_TOWER` | `{catalog}.{tgt_schema}.tm_cell_tower`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_cell_tower (
    CELL_ID DOUBLE NOT NULL,
    CELL_CODE VARCHAR(30) NOT NULL,
    NODE_ID DOUBLE NOT NULL,
    TECH VARCHAR(10) NOT NULL,
    CITY VARCHAR(50) NOT NULL,
    LATITUDE DECIMAL(10,6),
    LONGITUDE DECIMAL(10,6),
    STATUS VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_CELL_TOWER1
df_tm_cell_tower1 = spark.read.table(f"{catalog}.{src_schema}.tm_cell_tower")

# Source Qualifier SQ_TM_CELL_TOWER
df_sq_tm_cell_tower = df_tm_cell_tower1.select(
    F.col("CELL_ID"),
    F.col("CELL_CODE"),
    F.col("NODE_ID"),
    F.col("TECH"),
    F.col("CITY"),
    F.col("LATITUDE"),
    F.col("LONGITUDE"),
    F.col("STATUS"),
)

# Target TM_CELL_TOWER: Truncate target = YES -> overwrite
df_tm_cell_tower = df_sq_tm_cell_tower.select(
    F.col("CELL_ID"),
    F.col("CELL_CODE"),
    F.col("NODE_ID"),
    F.col("TECH"),
    F.col("CITY"),
    F.col("LATITUDE"),
    F.col("LONGITUDE"),
    F.col("STATUS"),
)
df_tm_cell_tower.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_cell_tower")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_cell_tower")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_cell_tower")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["CELL_ID", "CELL_CODE", "NODE_ID", "TECH", "CITY", "LATITUDE", "LONGITUDE", "STATUS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
