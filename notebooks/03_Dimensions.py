# Databricks notebook source
# DBTITLE 1,Config
# MAGIC %run "/Workspace/Users/mail.abhiprofessional@gmail.com/JPMorganChase_Technical Evaluation/Config"

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC # Build dim_date

# COMMAND ----------

# Get the date range from the Silver data for the date dimension.
date_range = silver_df.select(
    F.min("sale_date").alias("min_date"),
    F.max("sale_date").alias("max_date")
).collect()[0]

min_date = date_range["min_date"]
max_date = date_range["max_date"]

print("Minimum date:", min_date)
print("Maximum date:", max_date)

# COMMAND ----------

# DBTITLE 1,date_df
# Create a row for each date between the minimum and maximum sale date.
date_df = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
            )
        ) AS full_date
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add calendar attributes

# COMMAND ----------

# MAGIC %md
# MAGIC ### - The main purpose of this code is to build the dim_date dimension table from the dates present in sales data column
# MAGIC ### - Taking every date and creating useful calendar information around it for analysis purpose for example if we want to show total sales by month and quarter etc.

# COMMAND ----------

# DBTITLE 1,date_dim_df
# Adding useful information for each date.
# First, create a date_key in YYYYMMDD format
# Then extract the day, month, month name, quarter, year, and day of the week.
# These columns will be used later for filtering and grouping sales data.
date_dim_df = (
    date_df
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn(
        "month_name",
        F.date_format("full_date", "MMMM")
    )
    .withColumn(
        "quarter",
        F.quarter("full_date")
    )
    .withColumn(
        "year",
        F.year("full_date")
    )
    .withColumn(
        "day_of_week",
        F.dayofweek("full_date")
    )
    .withColumn(
        "day_name",
        F.date_format("full_date", "EEEE")
    )
    .select(
        "date_key",
        "full_date",
        "day",
        "month",
        "month_name",
        "quarter",
        "year",
        "day_of_week",
        "day_name"
    )
)

# COMMAND ----------

# DBTITLE 1,DATE_DIM write
# Write the date dimension to the Gold layer only when valid dates are available.
# If the Silver table has no dates, skip the write and show a warning.

if min_date is None or max_date is None:
    print("Warning: No dates found in silver table. Skipping date dimension creation.")
    print("Please ensure the silver table has data before creating dimensions.")
else:
    (
        date_dim_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(DATE_DIM)
    )

# COMMAND ----------

# DBTITLE 1,DATE_DIM display
# Verify
display(spark.table(DATE_DIM).orderBy("full_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Build dim_product

# COMMAND ----------

# Get the unique products from the Silver data.
# We use these values to build the product dimension.
product_source_df = (
    silver_df
    .select("product_name")
    .dropDuplicates()
)

# COMMAND ----------

# DBTITLE 1,product_window
# Assign a unique surrogate key to each product.
# Also add timestamps to track when the product record was created and updated.
product_window = Window.orderBy("product_name")

product_dim_df = (
    product_source_df
    .withColumn(
        "product_key",
        F.row_number().over(product_window).cast("long")
    )
    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
    .select(
        "product_key",
        "product_name",
        "created_timestamp",
        "updated_timestamp"
    )
)

# COMMAND ----------

# DBTITLE 1,PRODUCT_DIM write
(
    product_dim_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(PRODUCT_DIM)
)

# COMMAND ----------

# DBTITLE 1,PRODUCT_DIM display
display(spark.table(PRODUCT_DIM).orderBy(
    "product_key"
))

# COMMAND ----------

# Check how many records came from each batch.
# This helps separate the initial load from the incremental load.
silver_df.groupBy(
    "batch_id"
).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Initial customer dimension

# COMMAND ----------

# DBTITLE 1,initial_customers
# Get the unique customers from the initial load.
# These records are used to create the first customer dimension records.
initial_customers = (
    silver_df
    .filter(
        F.col("batch_id") == "initial_load"   # Take only the records that came from the initial/base file.
    )
    # keeping only the col we need to create the customer dim.
    .select(
        "customer_name",
        "region",
        "segment",
        "sale_date"
    )
    .dropDuplicates(["customer_name"])  # If the same customer appears multiple times in the initial sales data, keep only one customer record.
)

# COMMAND ----------

# DBTITLE 1,initial_customer_dim
# Create the first version of each customer in the dimension.
# Assign a surrogate key and set the record as current.
# The expiry date is set to 9999 until customer's details change.
customer_window = Window.orderBy("customer_name")

initial_customer_dim = (
    initial_customers
    .withColumn(
        "customer_key",
        F.row_number()
        .over(customer_window)
        .cast("long")
    )
    .withColumn(
        "effective_date",
        F.col("sale_date")
    )
    .withColumn(
        "expiry_date",
        F.to_date(F.lit("9999-12-31"))
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
    .select(
        "customer_key",
        "customer_name",
        "region",
        "segment",
        "effective_date",
        "expiry_date",
        "is_current",
        "created_timestamp",
        "updated_timestamp"
    )
)

# COMMAND ----------

# DBTITLE 1,CUSTOMER_DIM write
(
    initial_customer_dim.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(CUSTOMER_DIM)
)

# COMMAND ----------

# DBTITLE 1,CUSTOMER_DIM display
display(spark.table(CUSTOMER_DIM)
    .orderBy("customer_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Process the incremental customer records

# COMMAND ----------

# DBTITLE 1,incremental_customers
# Get the customer records from the incremental load.
# These records will be compared with the current customer records
# to find new or changed customers.
incremental_customers = (
    silver_df
    .filter(
        F.col("batch_id") == "incremental_load_001"
    )
    .select(
        "customer_name",
        "region",
        "segment",
        "sale_date"
    )
    .dropDuplicates()
)

# COMMAND ----------

display(incremental_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC # Find current customer versions

# COMMAND ----------

# DBTITLE 1,current_customers
# Get the current version of each customer.
# These records are used to compare against the incoming customer data.
current_customers = (
    spark.table(CUSTOMER_DIM)
    .filter(
        F.col("is_current") == True
    )
)

# COMMAND ----------

# DBTITLE 1,comparison_df
# Compare the incoming customers with the current customer records.
# The left join keeps all incoming customers, including new customers
# that do not yet exist in the dimension.
comparison_df = (
    incremental_customers.alias("src")
    .join(
        current_customers.alias("tgt"),
        F.col("src.customer_name") ==
        F.col("tgt.customer_name"),
        "left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Identify new customers

# COMMAND ----------

# DBTITLE 1,new_customers
# Find customers from the incremental load who are not already in the dimension.
# These customers are new, so they will get a new surrogate key and a current record.
new_customers = (
    comparison_df
    .filter(
        F.col("tgt.customer_key").isNull()
    )
    .select(
        F.col("src.customer_name").alias("customer_name"),
        F.col("src.region").alias("region"),
        F.col("src.segment").alias("segment"),
        F.col("src.sale_date").alias("effective_date")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Identify changed customers

# COMMAND ----------

# DBTITLE 1,changed_customers
# Find existing customers whose region or segment has changed.
# These changes will create a new version of the customer record.
changed_customers = (
    comparison_df
    .filter(
        F.col("tgt.customer_key").isNotNull()
    )
    .filter(
        (F.coalesce(F.col("src.region"), F.lit("")) !=
         F.coalesce(F.col("tgt.region"), F.lit("")))
        |
        (F.coalesce(F.col("src.segment"), F.lit("")) !=
         F.coalesce(F.col("tgt.segment"), F.lit("")))
    )
    .select(
        F.col("src.customer_name").alias("customer_name"),
        F.col("src.region").alias("region"),
        F.col("src.segment").alias("segment"),
        F.col("src.sale_date").alias("effective_date")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Identify unchanged customers

# COMMAND ----------

# DBTITLE 1,unchanged_customers
# Find existing customers whose details have not changed.
# These customers already have the correct current record, so no update is needed.
unchanged_customers = (
    comparison_df
    .filter(
        F.col("tgt.customer_key").isNotNull()
    )
    .filter(
        (F.coalesce(F.col("src.region"), F.lit("")) ==
         F.coalesce(F.col("tgt.region"), F.lit("")))
        &
        (F.coalesce(F.col("src.segment"), F.lit("")) ==
         F.coalesce(F.col("tgt.segment"), F.lit("")))
    )
)

# COMMAND ----------

# DBTITLE 1,count
print("New:", new_customers.count())
print("Changed:", changed_customers.count())
print("Unchanged:", unchanged_customers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Expire changed records

# COMMAND ----------

# DBTITLE 1,customer_delta
# Get the customer Delta table so we can update the existing records
customer_delta = DeltaTable.forName(
    spark,
    CUSTOMER_DIM
)

# COMMAND ----------

# DBTITLE 1,set expiry date
# Mark the old customer record as inactive when their details change.
# Set the expiry date to the date when the new version becomes effective.
(
    customer_delta.alias("tgt")
    .merge(
        changed_customers.alias("src"),
        """
        tgt.customer_name = src.customer_name
        AND tgt.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "is_current": "false",
            "expiry_date": "src.effective_date",
            "updated_timestamp": "current_timestamp()"
        }
    )
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create new versions

# COMMAND ----------

# DBTITLE 1,changed + new customers
# combine changed_customers + new_customers
customers_to_insert = (
    changed_customers
    .unionByName(new_customers)
)

# COMMAND ----------

# DBTITLE 1,max_customer_key
# Get the highest customer key already in the dimension.
# New customer records will use the next available keys.
max_customer_key = (
    spark.table(CUSTOMER_DIM)
    .agg(
        F.max("customer_key").alias("max_key")
    )
    .collect()[0]["max_key"]
)

# COMMAND ----------

# DBTITLE 1,new_customer_window
# Assign new keys to the customer records that need to be inserted.
# Changed customers get a new key for their new version, while new customers
# also get their first key. The new records are marked as current.
new_customer_window = Window.orderBy(
    "customer_name",
    "effective_date"
)

customers_to_insert = (
    customers_to_insert
    .withColumn(
        "customer_key",
        (
            F.row_number()
            .over(new_customer_window)
            + F.lit(max_customer_key)
        ).cast("long")
    )
    .withColumn(
        "expiry_date",
        F.to_date(F.lit("9999-12-31"))
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
    .withColumn(
        "created_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "updated_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

# DBTITLE 1,customers_to_insert
# Keep only the columns needed to add in customer dim table.
customers_to_insert = customers_to_insert.select(
    "customer_key",
    "customer_name",
    "region",
    "segment",
    "effective_date",
    "expiry_date",
    "is_current",
    "created_timestamp",
    "updated_timestamp"
)

# COMMAND ----------

# DBTITLE 1,CUSTOMER_DIM write
(
    customers_to_insert.write
    .format("delta")
    .mode("append")
    .saveAsTable(CUSTOMER_DIM)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Final customer validation

# COMMAND ----------

# DBTITLE 1,CUSTOMER_DIM display
# Check the customer dimension and view each customer's versions in order.
# This makes it easy to verify the SCD Type 2 history.
display(spark.table(CUSTOMER_DIM)
    .orderBy(
        "customer_name",
        "effective_date"
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC Critical SCD2 test

# COMMAND ----------

# DBTITLE 1,no. of versions for each customer
# Check the number of versions for each customer.
# Each customer should have only one current version in the SCD Type 2 table.
display(spark.table(CUSTOMER_DIM)
    .groupBy("customer_name")
    .agg(
        F.count("*").alias("versions"),
        F.sum(
            F.col("is_current").cast("int")
        ).alias("current_versions")
    )
    .orderBy("customer_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify Product dimension

# COMMAND ----------

print(
    "Products:",
    spark.table(PRODUCT_DIM).count()
)

# COMMAND ----------

spark.table(PRODUCT_DIM) \
    .select("product_key", "product_name") \
    .orderBy("product_key") \
    .show()