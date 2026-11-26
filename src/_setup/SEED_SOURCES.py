# Databricks notebook source
# MAGIC %md
# MAGIC # SEED_SOURCES
# MAGIC POC only: creates every source table with synthetic rows, because Free Edition
# MAGIC cannot reach the original Oracle source. Skip this when sources are already loaded.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("rows", "5")
catalog = dbutils.widgets.get("catalog")
rows = int(dbutils.widgets.get("rows"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.oltp_user")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.department (
    DEPTID VARCHAR(5) NOT NULL,
    DEPTNAME VARCHAR(30)
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.department
SELECT
  lpad(cast(id AS STRING), 5, '0') AS DEPTID,
  lpad(cast(id AS STRING), 10, '0') AS DEPTNAME
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.department")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.distributors (
    DISTRIBUTOR_ID DECIMAL(38,0) NOT NULL,
    DISTRIBUTOR_NAME VARCHAR(48) NOT NULL,
    CONTACT_PERSON VARCHAR(48),
    CONTACT_PHONE VARCHAR(32),
    CONTACT_EMAIL VARCHAR(72),
    FIRST_NAME VARCHAR(48),
    LAST_NAME VARCHAR(48)
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.distributors
SELECT
  cast(id AS DECIMAL(38,0)) AS DISTRIBUTOR_ID,
  lpad(cast(id AS STRING), 10, '0') AS DISTRIBUTOR_NAME,
  lpad(cast(id AS STRING), 10, '0') AS CONTACT_PERSON,
  lpad(cast(id AS STRING), 10, '0') AS CONTACT_PHONE,
  lpad(cast(id AS STRING), 10, '0') AS CONTACT_EMAIL,
  lpad(cast(id AS STRING), 10, '0') AS FIRST_NAME,
  lpad(cast(id AS STRING), 10, '0') AS LAST_NAME
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.distributors")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.employees (
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
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.employees
SELECT
  cast(id AS DECIMAL(38,0)) AS EMPLOYEE_ID,
  cast(id AS DECIMAL(38,0)) AS JOB_ID,
  lpad(cast(id AS STRING), 10, '0') AS FIRST_NAME,
  lpad(cast(id AS STRING), 10, '0') AS LAST_NAME,
  lpad(cast(id AS STRING), 10, '0') AS ADDRESS1,
  lpad(cast(id AS STRING), 10, '0') AS ADDRESS2,
  lpad(cast(id AS STRING), 10, '0') AS CITY,
  lpad(cast(id AS STRING), 2, '0') AS STATE,
  lpad(cast(id AS STRING), 10, '0') AS POSTAL_CODE,
  lpad(cast(id AS STRING), 10, '0') AS HOME_PHONE,
  lpad(cast(id AS STRING), 10, '0') AS OFFICE_PHONE,
  lpad(cast(id AS STRING), 10, '0') AS EMAIL
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.employees")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.items (
    ITEM_ID DECIMAL(38,0) NOT NULL,
    ITEM_NAME VARCHAR(72) NOT NULL,
    ITEM_DESC VARCHAR(72),
    PRICE DECIMAL(10,2) NOT NULL,
    WHOLESALE_COST DECIMAL(10,2) NOT NULL,
    DISCONTINUED_FLAG DECIMAL(38,0),
    MANUFACTURER_ID DECIMAL(38,0),
    DISTRIBUTOR_ID DECIMAL(38,0)
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.items
SELECT
  cast(id AS DECIMAL(38,0)) AS ITEM_ID,
  lpad(cast(id AS STRING), 10, '0') AS ITEM_NAME,
  lpad(cast(id AS STRING), 10, '0') AS ITEM_DESC,
  cast(id AS DECIMAL(10,2)) AS PRICE,
  cast(id AS DECIMAL(10,2)) AS WHOLESALE_COST,
  cast(id AS DECIMAL(38,0)) AS DISCONTINUED_FLAG,
  cast(id AS DECIMAL(38,0)) AS MANUFACTURER_ID,
  cast(id AS DECIMAL(38,0)) AS DISTRIBUTOR_ID
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.items")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.items_in_promotions (
    PROMOTION_ID DECIMAL(38,0) NOT NULL,
    ITEM_ID DECIMAL(38,0) NOT NULL,
    COMMENTS VARCHAR(72),
    DISCOUNT DECIMAL(10,2)
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.items_in_promotions
SELECT
  cast(id AS DECIMAL(38,0)) AS PROMOTION_ID,
  cast(id AS DECIMAL(38,0)) AS ITEM_ID,
  lpad(cast(id AS STRING), 10, '0') AS COMMENTS,
  cast(id AS DECIMAL(10,2)) AS DISCOUNT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.items_in_promotions")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.jobs (
    JOB_ID DECIMAL(38,0) NOT NULL,
    JOB_DESC VARCHAR(72) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.jobs
SELECT
  cast(id AS DECIMAL(38,0)) AS JOB_ID,
  lpad(cast(id AS STRING), 10, '0') AS JOB_DESC
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.jobs")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.manufacturers (
    MANUFACTURER_ID DECIMAL(38,0) NOT NULL,
    MANUFACTURER_NAME VARCHAR(48) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.manufacturers
SELECT
  cast(id AS DECIMAL(38,0)) AS MANUFACTURER_ID,
  lpad(cast(id AS STRING), 10, '0') AS MANUFACTURER_NAME
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.manufacturers")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.order_items (
    ORDER_ID DECIMAL(38,0) NOT NULL,
    ITEM_ID DECIMAL(38,0) NOT NULL,
    QUANTITY DECIMAL(38,0) NOT NULL,
    DISCOUNT DECIMAL(10,2) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.order_items
SELECT
  cast(id AS DECIMAL(38,0)) AS ORDER_ID,
  cast(id AS DECIMAL(38,0)) AS ITEM_ID,
  cast(id AS DECIMAL(38,0)) AS QUANTITY,
  cast(id AS DECIMAL(10,2)) AS DISCOUNT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.order_items")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.orders (
    ORDER_ID DECIMAL(38,0) NOT NULL,
    DATE_ENTERED TIMESTAMP NOT NULL,
    DATE_PROMISED TIMESTAMP NOT NULL,
    DATE_SHIPPED TIMESTAMP,
    EMPLOYEE_ID DECIMAL(38,0),
    CUSTOMER_ID DECIMAL(38,0),
    SALES_TAX_RATE DECIMAL(5,4) NOT NULL,
    STORE_ID DECIMAL(38,0)
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.orders
SELECT
  cast(id AS DECIMAL(38,0)) AS ORDER_ID,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS DATE_ENTERED,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS DATE_PROMISED,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS DATE_SHIPPED,
  cast(id AS DECIMAL(38,0)) AS EMPLOYEE_ID,
  cast(id AS DECIMAL(38,0)) AS CUSTOMER_ID,
  cast(id AS DECIMAL(5,4)) AS SALES_TAX_RATE,
  cast(id AS DECIMAL(38,0)) AS STORE_ID
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.orders")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.promotions (
    PROMOTION_ID DECIMAL(38,0) NOT NULL,
    PROMOTION_NAME VARCHAR(72) NOT NULL,
    DESCRIPTION VARCHAR(72),
    START_DATE TIMESTAMP NOT NULL,
    END_DATE TIMESTAMP NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.promotions
SELECT
  cast(id AS DECIMAL(38,0)) AS PROMOTION_ID,
  lpad(cast(id AS STRING), 10, '0') AS PROMOTION_NAME,
  lpad(cast(id AS STRING), 10, '0') AS DESCRIPTION,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS START_DATE,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS END_DATE
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.promotions")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.stores (
    STORE_ID DECIMAL(38,0) NOT NULL,
    STORE_DESC VARCHAR(72) NOT NULL,
    ADDRESS1 VARCHAR(72) NOT NULL,
    ADDRESS2 VARCHAR(72),
    CITY VARCHAR(32) NOT NULL,
    STATE VARCHAR(2) NOT NULL,
    POSTAL_CODE VARCHAR(10) NOT NULL,
    MANAGER_ID DECIMAL(38,0) NOT NULL,
    PHONE VARCHAR(32) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.stores
SELECT
  cast(id AS DECIMAL(38,0)) AS STORE_ID,
  lpad(cast(id AS STRING), 10, '0') AS STORE_DESC,
  lpad(cast(id AS STRING), 10, '0') AS ADDRESS1,
  lpad(cast(id AS STRING), 10, '0') AS ADDRESS2,
  lpad(cast(id AS STRING), 10, '0') AS CITY,
  lpad(cast(id AS STRING), 2, '0') AS STATE,
  lpad(cast(id AS STRING), 10, '0') AS POSTAL_CODE,
  cast(id AS DECIMAL(38,0)) AS MANAGER_ID,
  lpad(cast(id AS STRING), 10, '0') AS PHONE
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.stores")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_account (
    ACCOUNT_ID DOUBLE NOT NULL,
    ACCOUNT_NO VARCHAR(30) NOT NULL,
    CUSTOMER_ID DOUBLE NOT NULL,
    ACCOUNT_TYPE VARCHAR(20) NOT NULL,
    STATUS VARCHAR(10) NOT NULL,
    OPENED_DT TIMESTAMP
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_account
SELECT
  cast(id AS DOUBLE) AS ACCOUNT_ID,
  lpad(cast(id AS STRING), 10, '0') AS ACCOUNT_NO,
  cast(id AS DOUBLE) AS CUSTOMER_ID,
  lpad(cast(id AS STRING), 10, '0') AS ACCOUNT_TYPE,
  lpad(cast(id AS STRING), 10, '0') AS STATUS,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS OPENED_DT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_account")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_address (
    ADDRESS_ID DOUBLE NOT NULL,
    CUSTOMER_ID DOUBLE NOT NULL,
    ADDR_LINE1 VARCHAR(200) NOT NULL,
    CITY VARCHAR(50) NOT NULL,
    STATE VARCHAR(50),
    POSTAL_CODE VARCHAR(12),
    COUNTRY VARCHAR(2),
    ADDR_TYPE VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_address
SELECT
  cast(id AS DOUBLE) AS ADDRESS_ID,
  cast(id AS DOUBLE) AS CUSTOMER_ID,
  lpad(cast(id AS STRING), 10, '0') AS ADDR_LINE1,
  lpad(cast(id AS STRING), 10, '0') AS CITY,
  lpad(cast(id AS STRING), 10, '0') AS STATE,
  lpad(cast(id AS STRING), 10, '0') AS POSTAL_CODE,
  lpad(cast(id AS STRING), 2, '0') AS COUNTRY,
  lpad(cast(id AS STRING), 10, '0') AS ADDR_TYPE
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_address")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_billing_cycle (
    BILLING_CYCLE_ID DOUBLE NOT NULL,
    CYCLE_CODE VARCHAR(20) NOT NULL,
    CYCLE_DAY DECIMAL(2,0) NOT NULL,
    DESCRIPTION VARCHAR(200)
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_billing_cycle
SELECT
  cast(id AS DOUBLE) AS BILLING_CYCLE_ID,
  lpad(cast(id AS STRING), 10, '0') AS CYCLE_CODE,
  cast(id AS DECIMAL(2,0)) AS CYCLE_DAY,
  lpad(cast(id AS STRING), 10, '0') AS DESCRIPTION
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_billing_cycle")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_billing_profile (
    BILLING_PROFILE_ID DOUBLE NOT NULL,
    ACCOUNT_ID DOUBLE NOT NULL,
    BILLING_CYCLE_ID DOUBLE NOT NULL,
    BILL_DELIVERY VARCHAR(10) NOT NULL,
    BILL_CURRENCY VARCHAR(3) NOT NULL,
    PAYMENT_TERMS_DAYS DECIMAL(3,0) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_billing_profile
SELECT
  cast(id AS DOUBLE) AS BILLING_PROFILE_ID,
  cast(id AS DOUBLE) AS ACCOUNT_ID,
  cast(id AS DOUBLE) AS BILLING_CYCLE_ID,
  lpad(cast(id AS STRING), 10, '0') AS BILL_DELIVERY,
  lpad(cast(id AS STRING), 3, '0') AS BILL_CURRENCY,
  cast(id AS DECIMAL(3,0)) AS PAYMENT_TERMS_DAYS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_billing_profile")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_cdr_data (
    CDR_DATA_ID DOUBLE NOT NULL,
    MSISDN_ID DOUBLE NOT NULL,
    SESSION_START_TS TIMESTAMP NOT NULL,
    SESSION_END_TS TIMESTAMP NOT NULL,
    DATA_MB DECIMAL(12,3) NOT NULL,
    APN VARCHAR(50),
    CELL_ID DOUBLE,
    CHARGE_AMT DECIMAL(10,4) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_cdr_data
SELECT
  cast(id AS DOUBLE) AS CDR_DATA_ID,
  cast(id AS DOUBLE) AS MSISDN_ID,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS SESSION_START_TS,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS SESSION_END_TS,
  cast(id AS DECIMAL(12,3)) AS DATA_MB,
  lpad(cast(id AS STRING), 10, '0') AS APN,
  cast(id AS DOUBLE) AS CELL_ID,
  cast(id AS DECIMAL(10,4)) AS CHARGE_AMT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_cdr_data")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_cdr_sms (
    CDR_SMS_ID DOUBLE NOT NULL,
    MSISDN_ID DOUBLE NOT NULL,
    EVENT_TS TIMESTAMP NOT NULL,
    DEST_NUMBER VARCHAR(20) NOT NULL,
    SMS_TYPE VARCHAR(10) NOT NULL,
    CHARGE_AMT DECIMAL(10,4) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_cdr_sms
SELECT
  cast(id AS DOUBLE) AS CDR_SMS_ID,
  cast(id AS DOUBLE) AS MSISDN_ID,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS EVENT_TS,
  lpad(cast(id AS STRING), 10, '0') AS DEST_NUMBER,
  lpad(cast(id AS STRING), 10, '0') AS SMS_TYPE,
  cast(id AS DECIMAL(10,4)) AS CHARGE_AMT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_cdr_sms")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_cdr_voice (
    CDR_VOICE_ID DOUBLE NOT NULL,
    MSISDN_ID DOUBLE NOT NULL,
    EVENT_TS TIMESTAMP NOT NULL,
    CALLED_NUMBER VARCHAR(20) NOT NULL,
    DURATION_SEC DECIMAL(10,0) NOT NULL,
    CELL_ID DOUBLE,
    CHARGE_AMT DECIMAL(10,4) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_cdr_voice
SELECT
  cast(id AS DOUBLE) AS CDR_VOICE_ID,
  cast(id AS DOUBLE) AS MSISDN_ID,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS EVENT_TS,
  lpad(cast(id AS STRING), 10, '0') AS CALLED_NUMBER,
  cast(id AS DECIMAL(10,0)) AS DURATION_SEC,
  cast(id AS DOUBLE) AS CELL_ID,
  cast(id AS DECIMAL(10,4)) AS CHARGE_AMT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_cdr_voice")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_cell_tower (
    CELL_ID DOUBLE NOT NULL,
    CELL_CODE VARCHAR(30) NOT NULL,
    NODE_ID DOUBLE NOT NULL,
    TECH VARCHAR(10) NOT NULL,
    CITY VARCHAR(50) NOT NULL,
    LATITUDE DECIMAL(10,6),
    LONGITUDE DECIMAL(10,6),
    STATUS VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_cell_tower
SELECT
  cast(id AS DOUBLE) AS CELL_ID,
  lpad(cast(id AS STRING), 10, '0') AS CELL_CODE,
  cast(id AS DOUBLE) AS NODE_ID,
  lpad(cast(id AS STRING), 10, '0') AS TECH,
  lpad(cast(id AS STRING), 10, '0') AS CITY,
  cast(id AS DECIMAL(10,6)) AS LATITUDE,
  cast(id AS DECIMAL(10,6)) AS LONGITUDE,
  lpad(cast(id AS STRING), 10, '0') AS STATUS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_cell_tower")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_customer (
    CUSTOMER_ID DOUBLE NOT NULL,
    CUSTOMER_REF VARCHAR(30) NOT NULL,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    DOB TIMESTAMP,
    EMAIL VARCHAR(200),
    KYC_STATUS VARCHAR(10) NOT NULL,
    CREATED_DT TIMESTAMP
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_customer
SELECT
  cast(id AS DOUBLE) AS CUSTOMER_ID,
  lpad(cast(id AS STRING), 10, '0') AS CUSTOMER_REF,
  lpad(cast(id AS STRING), 10, '0') AS FIRST_NAME,
  lpad(cast(id AS STRING), 10, '0') AS LAST_NAME,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS DOB,
  lpad(cast(id AS STRING), 10, '0') AS EMAIL,
  lpad(cast(id AS STRING), 10, '0') AS KYC_STATUS,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS CREATED_DT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_customer")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_device (
    DEVICE_ID DOUBLE NOT NULL,
    IMEI VARCHAR(20) NOT NULL,
    DEVICE_MODEL VARCHAR(50) NOT NULL,
    OS_NAME VARCHAR(20) NOT NULL,
    PURCHASED_DT TIMESTAMP,
    STATUS VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_device
SELECT
  cast(id AS DOUBLE) AS DEVICE_ID,
  lpad(cast(id AS STRING), 10, '0') AS IMEI,
  lpad(cast(id AS STRING), 10, '0') AS DEVICE_MODEL,
  lpad(cast(id AS STRING), 10, '0') AS OS_NAME,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS PURCHASED_DT,
  lpad(cast(id AS STRING), 10, '0') AS STATUS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_device")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_invoice_line (
    INVOICE_LINE_ID DOUBLE NOT NULL,
    INVOICE_ID DOUBLE NOT NULL,
    LINE_TYPE VARCHAR(20) NOT NULL,
    DESCRIPTION VARCHAR(200),
    QTY DECIMAL(12,3),
    UNIT_PRICE DECIMAL(12,4) NOT NULL,
    LINE_AMT DECIMAL(12,2) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_invoice_line
SELECT
  cast(id AS DOUBLE) AS INVOICE_LINE_ID,
  cast(id AS DOUBLE) AS INVOICE_ID,
  lpad(cast(id AS STRING), 10, '0') AS LINE_TYPE,
  lpad(cast(id AS STRING), 10, '0') AS DESCRIPTION,
  cast(id AS DECIMAL(12,3)) AS QTY,
  cast(id AS DECIMAL(12,4)) AS UNIT_PRICE,
  cast(id AS DECIMAL(12,2)) AS LINE_AMT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_invoice_line")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_msisdn (
    MSISDN_ID DOUBLE NOT NULL,
    MSISDN VARCHAR(20) NOT NULL,
    SUBSCRIPTION_ID DOUBLE NOT NULL,
    SIM_ID DOUBLE NOT NULL,
    ACTIVATED_DT TIMESTAMP NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_msisdn
SELECT
  cast(id AS DOUBLE) AS MSISDN_ID,
  lpad(cast(id AS STRING), 10, '0') AS MSISDN,
  cast(id AS DOUBLE) AS SUBSCRIPTION_ID,
  cast(id AS DOUBLE) AS SIM_ID,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS ACTIVATED_DT,
  lpad(cast(id AS STRING), 10, '0') AS STATUS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_msisdn")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_network_node (
    NODE_ID DOUBLE NOT NULL,
    NODE_CODE VARCHAR(30) NOT NULL,
    NODE_TYPE VARCHAR(20) NOT NULL,
    CITY VARCHAR(50) NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_network_node
SELECT
  cast(id AS DOUBLE) AS NODE_ID,
  lpad(cast(id AS STRING), 10, '0') AS NODE_CODE,
  lpad(cast(id AS STRING), 10, '0') AS NODE_TYPE,
  lpad(cast(id AS STRING), 10, '0') AS CITY,
  lpad(cast(id AS STRING), 10, '0') AS STATUS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_network_node")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_payment (
    PAYMENT_ID DOUBLE NOT NULL,
    INVOICE_ID DOUBLE NOT NULL,
    PAID_DT TIMESTAMP NOT NULL,
    PAID_AMT DECIMAL(12,2) NOT NULL,
    METHOD VARCHAR(10) NOT NULL,
    REFERENCE_NO VARCHAR(40)
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_payment
SELECT
  cast(id AS DOUBLE) AS PAYMENT_ID,
  cast(id AS DOUBLE) AS INVOICE_ID,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS PAID_DT,
  cast(id AS DECIMAL(12,2)) AS PAID_AMT,
  lpad(cast(id AS STRING), 10, '0') AS METHOD,
  lpad(cast(id AS STRING), 10, '0') AS REFERENCE_NO
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_payment")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_plan (
    PLAN_ID DOUBLE NOT NULL,
    PLAN_CODE VARCHAR(30) NOT NULL,
    PLAN_NAME VARCHAR(100) NOT NULL,
    PLAN_TYPE VARCHAR(20) NOT NULL,
    MONTHLY_FEE DECIMAL(10,2) NOT NULL,
    CURRENCY VARCHAR(3) NOT NULL,
    STATUS VARCHAR(10) NOT NULL,
    CREATED_DT TIMESTAMP
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_plan
SELECT
  cast(id AS DOUBLE) AS PLAN_ID,
  lpad(cast(id AS STRING), 10, '0') AS PLAN_CODE,
  lpad(cast(id AS STRING), 10, '0') AS PLAN_NAME,
  lpad(cast(id AS STRING), 10, '0') AS PLAN_TYPE,
  cast(id AS DECIMAL(10,2)) AS MONTHLY_FEE,
  lpad(cast(id AS STRING), 3, '0') AS CURRENCY,
  lpad(cast(id AS STRING), 10, '0') AS STATUS,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS CREATED_DT
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_plan")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_plan_rate (
    PLAN_RATE_ID DOUBLE NOT NULL,
    PLAN_ID DOUBLE NOT NULL,
    RATE_TYPE VARCHAR(20) NOT NULL,
    INCLUDED_QTY DECIMAL(12,2) NOT NULL,
    OVERAGE_RATE DECIMAL(10,4) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_plan_rate
SELECT
  cast(id AS DOUBLE) AS PLAN_RATE_ID,
  cast(id AS DOUBLE) AS PLAN_ID,
  lpad(cast(id AS STRING), 10, '0') AS RATE_TYPE,
  cast(id AS DECIMAL(12,2)) AS INCLUDED_QTY,
  cast(id AS DECIMAL(10,4)) AS OVERAGE_RATE
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_plan_rate")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_product (
    PRODUCT_ID DOUBLE NOT NULL,
    PRODUCT_CODE VARCHAR(30) NOT NULL,
    PRODUCT_NAME VARCHAR(100) NOT NULL,
    PRODUCT_TYPE VARCHAR(20) NOT NULL,
    PRICE DECIMAL(10,2) NOT NULL,
    CURRENCY VARCHAR(3) NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_product
SELECT
  cast(id AS DOUBLE) AS PRODUCT_ID,
  lpad(cast(id AS STRING), 10, '0') AS PRODUCT_CODE,
  lpad(cast(id AS STRING), 10, '0') AS PRODUCT_NAME,
  lpad(cast(id AS STRING), 10, '0') AS PRODUCT_TYPE,
  cast(id AS DECIMAL(10,2)) AS PRICE,
  lpad(cast(id AS STRING), 3, '0') AS CURRENCY,
  lpad(cast(id AS STRING), 10, '0') AS STATUS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_product")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_sim (
    SIM_ID DOUBLE NOT NULL,
    SIM_ICCID VARCHAR(25) NOT NULL,
    SIM_TYPE VARCHAR(10) NOT NULL,
    ISSUED_DT TIMESTAMP NOT NULL,
    STATUS VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_sim
SELECT
  cast(id AS DOUBLE) AS SIM_ID,
  lpad(cast(id AS STRING), 10, '0') AS SIM_ICCID,
  lpad(cast(id AS STRING), 10, '0') AS SIM_TYPE,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS ISSUED_DT,
  lpad(cast(id AS STRING), 10, '0') AS STATUS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_sim")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.oltp_user.tm_subscription (
    SUBSCRIPTION_ID DOUBLE NOT NULL,
    ACCOUNT_ID DOUBLE NOT NULL,
    PLAN_ID DOUBLE NOT NULL,
    START_DT TIMESTAMP NOT NULL,
    END_DT TIMESTAMP,
    STATUS VARCHAR(10) NOT NULL
)""")
spark.sql(f"""
INSERT INTO {catalog}.oltp_user.tm_subscription
SELECT
  cast(id AS DOUBLE) AS SUBSCRIPTION_ID,
  cast(id AS DOUBLE) AS ACCOUNT_ID,
  cast(id AS DOUBLE) AS PLAN_ID,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS START_DT,
  cast(date_add(DATE'2026-01-01', cast(id AS INT)) AS TIMESTAMP) AS END_DT,
  lpad(cast(id AS STRING), 10, '0') AS STATUS
FROM range(1, {rows} + 1)
""")
print("seeded oltp_user.tm_subscription")
