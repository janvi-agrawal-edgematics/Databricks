# Databricks notebook source
# MAGIC %md
# MAGIC # PG_MAP_PROM
# MAGIC Generated from Informatica mapping `PG_MAP_PROM` (folder `For_Migration`) by the Axoma migration utility.
# MAGIC
# MAGIC | Informatica | Databricks |
# MAGIC |---|---|
# MAGIC | Source `PROMOTIONS` | `{catalog}.{src_schema}.promotions` |
# MAGIC | Target `PROMOTIONS1` | `{catalog}.{tgt_schema}.promotions`, truncate + insert (overwrite) |

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
CREATE TABLE IF NOT EXISTS {catalog}.{tgt_schema}.promotions (
    PROMOTION_ID DECIMAL(38,0) NOT NULL,
    PROMOTION_NAME VARCHAR(72) NOT NULL,
    DESCRIPTION VARCHAR(72),
    START_DATE TIMESTAMP NOT NULL,
    END_DATE TIMESTAMP NOT NULL
)""")

# COMMAND ----------

# Source PROMOTIONS
df_promotions = spark.read.table(f"{catalog}.{src_schema}.promotions")

# Source Qualifier SQ_PROMOTIONS
df_sq_promotions = df_promotions.select(
    F.col("PROMOTION_ID"),
    F.col("PROMOTION_NAME"),
    F.col("DESCRIPTION"),
    F.col("START_DATE"),
    F.col("END_DATE"),
)

# Target PROMOTIONS1: Truncate target = YES -> overwrite
df_promotions1 = df_sq_promotions.select(
    F.col("PROMOTION_ID"),
    F.col("PROMOTION_NAME"),
    F.col("DESCRIPTION"),
    F.col("START_DATE"),
    F.col("END_DATE"),
)
df_promotions1.write.mode("overwrite").insertInto(f"{catalog}.{tgt_schema}.promotions")

# COMMAND ----------

# Reconciliation
src_df = spark.read.table(f"{catalog}.{src_schema}.promotions")
tgt_df = spark.read.table(f"{catalog}.{tgt_schema}.promotions")
src_cnt, tgt_cnt = src_df.count(), tgt_df.count()
cols = ["PROMOTION_ID", "PROMOTION_NAME", "DESCRIPTION", "START_DATE", "END_DATE"]
missing = src_df.select(cols).exceptAll(tgt_df.select(cols)).count()
extra = tgt_df.select(cols).exceptAll(src_df.select(cols)).count()
print(f"source rows={src_cnt} target rows={tgt_cnt} missing={missing} extra={extra}")
assert src_cnt == tgt_cnt and missing == 0 and extra == 0, "Reconciliation FAILED"
print("Reconciliation PASSED")
