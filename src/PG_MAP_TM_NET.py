# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_TM_NET
# MAGIC Generated from Informatica mapping `PG_MAP_TM_NET` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `TM_NETWORK_NODE` | `{catalog}.{src_schema}.tm_network_node` |
# MAGIC | Target `TM_NETWORK_NODE1` | `{catalog}.{tgt_schema}.tm_network_node`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.tm_network_node (
    NODE_ID DOUBLE NOT NULL,
    NODE_CODE VARCHAR(30) NOT NULL,
    NODE_TYPE VARCHAR(20) NOT NULL,
    CITY VARCHAR(50) NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")

# COMMAND ----------

# Source TM_NETWORK_NODE
df_tm_network_node = spark.read.table(f"{catalog}.{src_schema}.tm_network_node")

# Source Qualifier SQ_TM_NETWORK_NODE
df_sq_tm_network_node = df_tm_network_node.select(
    F.col("NODE_ID"),
    F.col("NODE_CODE"),
    F.col("NODE_TYPE"),
    F.col("CITY"),
    F.col("STATUS"),
)

# Target TM_NETWORK_NODE1: Truncate target = YES -> overwrite
df_tm_network_node1 = df_sq_tm_network_node.select(
    F.col("NODE_ID"),
    F.col("NODE_CODE"),
    F.col("NODE_TYPE"),
    F.col("CITY"),
    F.col("STATUS"),
)
df_tm_network_node1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.tm_network_node")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.tm_network_node")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.tm_network_node")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["NODE_ID", "NODE_CODE", "NODE_TYPE", "CITY", "STATUS"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
