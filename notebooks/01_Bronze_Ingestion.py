# Databricks notebook source
# DBTITLE 1,Config
# MAGIC %run "/Workspace/Users/mail.abhiprofessional@gmail.com/JPMorganChase_Technical Evaluation/Config"

# COMMAND ----------

# DBTITLE 1,Create Catalog
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}
""")

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}
""")

# COMMAND ----------

# DBTITLE 1,Display Catalog
display(spark.sql(f"""
SHOW SCHEMAS IN {CATALOG_NAME}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Dimension Tables

# COMMAND ----------

# DBTITLE 1,CUSTOMER_DIM
# Customer Dimension Table
# Implements Slowly Changing Dimension (SCD) Type 2

CUSTOMER_DIM = f"{CATALOG_NAME}.gold.dim_customer"

# Create the customer dimension table if it does not already exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CUSTOMER_DIM} (
    customer_key BIGINT,          -- Surrogate key for the customer dimension
    customer_name STRING,        -- Customer name
    region STRING,               -- Customer region
    segment STRING,               -- Customer segment
    effective_date DATE,         -- Date from which this dimension version is valid
    expiry_date DATE,            -- Date until which this dimension version is valid
    is_current BOOLEAN,          -- Indicates whether this is the current dimension version
    created_timestamp TIMESTAMP, -- Timestamp when the record was created
    updated_timestamp TIMESTAMP  -- Timestamp when the record was last updated
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,PRODUCT_DIM
# Product Dimension Table
# Implements Slowly Changing Dimension (SCD) Type 1

PRODUCT_DIM = f"{CATALOG_NAME}.gold.dim_product"

# Create the product dimension table if it does not already exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PRODUCT_DIM} (
    product_key BIGINT,           -- Surrogate key for the product dimension
    product_name STRING,          -- Name of the product
    created_timestamp TIMESTAMP,  -- Timestamp when the record was created
    updated_timestamp TIMESTAMP   -- Timestamp when the record was last updated
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,DATE_DIM
# Date Dimension Table
# Provides calendar attributes for date-based analysis and reporting

DATE_DIM = f"{CATALOG_NAME}.gold.dim_date"

# Create the date dimension table if it does not exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DATE_DIM} (
date_key INT,           -- Warehouse date key based on YYYYMMDD
full_date DATE,         -- Full calendar date
day INT,                -- Day of the month
month INT,              -- Month number
month_name STRING,      -- Month name
quarter INT,             -- Quarter number
year INT,                -- Calendar year
day_of_week INT,         -- Day of week number (1=Sunday, 7=Saturday)
day_name STRING          -- Day name (e.g., Monday)
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,FACT_TABLE
# Fact Table
# Stores transaction-level sales measures and references the dimension tables
# using surrogate keys.

FACT_TABLE = f"{CATALOG_NAME}.gold.fact_sales"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
sale_id BIGINT,                  -- Unique identifier for the sales transaction
date_key INT,                    -- Foreign key referencing the date dimension
customer_key BIGINT,             -- Foreign key referencing the customer dimension
product_key BIGINT,              -- Foreign key referencing the product dimension
quantity INT,                    -- Quantity of products sold
amount DECIMAL(18,2),             -- Transaction sales amount
created_timestamp TIMESTAMP,     -- Timestamp when the fact record was created
updated_timestamp TIMESTAMP,     -- Timestamp when the fact record was last updated
batch_id STRING                  -- Identifier of the source/incremental batch
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,DQ_TABLE
# Quarantine table for records that fail data quality checks

DQ_TABLE = f"{CATALOG_NAME}.gold.dq_quarantine"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_TABLE} (
sale_id BIGINT,                     -- Unique identifier for the sales transaction
sale_date STRING,                   -- Original sale date value from the source
customer_name STRING,               -- Customer name from the source
region STRING,                      -- Customer region from the source
segment STRING,                     -- Customer segment from the source
product_name STRING,                -- Product name from the source
amount STRING,                      -- Original amount value from the source
quantity STRING,                    -- Original quantity value from the source
error_reason STRING,                -- Reason why the record failed validation
batch_id STRING,                    -- Identifier of the source/incremental batch
quarantined_timestamp TIMESTAMP     -- Timestamp when the record was quarantined
)
USING DELTA
""")

# COMMAND ----------

display(spark.sql(f"""
SHOW TABLES IN {CATALOG_NAME}.gold
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Bronze table

# COMMAND ----------

# DBTITLE 1,BRONZE INGESTION
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType
)

# COMMAND ----------

# DBTITLE 1,Schema
raw_schema = StructType([
    StructField("sale_id", LongType(), True),
    StructField("sale_date", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("quantity", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the Source CSV File

# COMMAND ----------

# DBTITLE 1,Source File path
BASE_PATH = "/Volumes/jpmc_assessment/bronze/source_data/SampleData_Base.csv"
UPDATE_PATH = "/Volumes/jpmc_assessment/bronze/source_data/SampleData_Updates.csv"

# COMMAND ----------

# DBTITLE 1,Read base, update file
# Read the base sales data
base_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(raw_schema)
    .load(BASE_PATH)
)

# Read the incremental sales data
updates_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(raw_schema)
    .load(UPDATE_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Metadata

# COMMAND ----------

# DBTITLE 1,metadata base
# Add metadata for the initial batch
base_bronze_df = (
    base_df
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("batch_id", F.lit("initial_load"))
)

# COMMAND ----------

# DBTITLE 1,metadata updates
# Add metadata for the incremental batch
updates_bronze_df = (
    updates_df
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("batch_id", F.lit("incremental_load_001"))
)

# COMMAND ----------

# DBTITLE 1,Combine base and update data
# Combine the initial and incremental data
bronze_df = base_bronze_df.unionByName(
    updates_bronze_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ### - Create the Bronze table if it does not already exist.
# MAGIC ### - Bronze stores the raw data as received from the source, along with
# MAGIC ### - basic ingestion metadata. Data types such as amount and quantity
# MAGIC ### - are kept as strings so that invalid source values can be preserved
# MAGIC ### - and handled later during the Silver transformation.

# COMMAND ----------

# DBTITLE 1,BRONZE_TABLE
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    sale_id BIGINT,                       -- Sale transaction ID
    sale_date STRING,                    -- Sale date from the source
    customer_name STRING,                -- Customer name
    region STRING,                       -- Customer region
    segment STRING,                      -- Customer segment
    product_name STRING,                 -- Product name
    amount STRING,                       -- Amount as received from the source
    quantity STRING,                     -- Quantity as received from the source
    source_file STRING,                  -- File from which the record was read
    ingestion_timestamp TIMESTAMP,       -- Time when the record was loaded
    batch_id STRING                      -- Identifies the load batch
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,Write BRONZE_TABLE
# Write the data
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate

# COMMAND ----------

# DBTITLE 1,Validate
# Validate the Bronze load by checking the total records
# and the number of records received in each batch.
bronze = spark.table(BRONZE_TABLE)

print("Total records:", bronze.count())

print(
    "Base records:",
    bronze.filter(
        F.col("batch_id") == "initial_load"
    ).count()
)

print(
    "Update records:",
    bronze.filter(
        F.col("batch_id") == "incremental_load_001"
    ).count()
)

# COMMAND ----------

# Check the raw data
display(bronze.orderBy("sale_id"))

# COMMAND ----------

# And specifically verify the bad record:
# Check a known bad record
display(bronze.filter(
    F.col("sale_id") == 1016
))

# COMMAND ----------

# DBTITLE 1,bronze schema
# Check the schema
bronze.printSchema()