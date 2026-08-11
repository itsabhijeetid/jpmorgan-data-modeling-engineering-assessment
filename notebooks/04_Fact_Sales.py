# Databricks notebook source
# DBTITLE 1,Config
# MAGIC %run "/Workspace/Users/mail.abhiprofessional@gmail.com/JPMorganChase_Technical Evaluation/Config"

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)
customer_dim_df = spark.table(CUSTOMER_DIM)
product_dim_df = spark.table(PRODUCT_DIM)
date_dim_df = spark.table(DATE_DIM)

# COMMAND ----------

# MAGIC %md
# MAGIC # Date Key

# COMMAND ----------

# DBTITLE 1,date_key
# Match each sale with the date dimension to get its date_key.
# The left join keeps all sales even if a matching date is not found.
fact_df = (
    silver_df.alias("s")
    .join(
        date_dim_df.alias("d"),
        F.col("s.sale_date") == F.col("d.full_date"),
        "left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Product Key

# COMMAND ----------

# DBTITLE 1,Product Key
# Match each sale with the product dimension to get its product_key.
# The left join keeps all sales even if a matching product is not found.
fact_df = (
    fact_df
    .join(
        product_dim_df.alias("p"),
        F.col("s.product_name") == F.col("p.product_name"),
        "left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Customer SCD2 Key

# COMMAND ----------

# DBTITLE 1,Customer SCD2 Key
# Match each sale with the correct customer version from the SCD Type 2 dimension.
# The customer name must match, and the sale date must fall within the
# customer's effective and expiry dates.
# This gives the fact table the correct customer_key for that sale.
fact_df = (
    fact_df
    .join(
        customer_dim_df.alias("c"),
        (
            (F.col("s.customer_name") == F.col("c.customer_name"))
            &
            (F.col("s.sale_date") >= F.col("c.effective_date"))
            &
            (F.col("s.sale_date") < F.col("c.expiry_date"))
        ),
        "left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Select the final fact columns

# COMMAND ----------

# DBTITLE 1,Select the final fact columns
# Select the final columns needed for the fact table.
# Use the surrogate keys from the dimensions and keep the sales measures
# and batch information from the Silver data.
fact_df = (
    fact_df
    .select(
        F.col("s.sale_id"),
        F.col("d.date_key"),
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("s.quantity"),
        F.col("s.amount"),
        F.current_timestamp().alias("created_timestamp"),
        F.current_timestamp().alias("updated_timestamp"),
        F.col("s.batch_id")
    )
)

# COMMAND ----------

# DBTITLE 1,fact_df display
display(fact_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate dimension keys

# COMMAND ----------

# DBTITLE 1,dimension key is missing
# Show sales where a dimension key is missing.
display(fact_df.filter(
    F.col("date_key").isNull()
    | F.col("customer_key").isNull()
    | F.col("product_key").isNull()
))

# COMMAND ----------

# DBTITLE 1,dimension key is missing count
# Count sales where a dimension key is missing.
print(
    "Unresolved dimension keys:",
    fact_df.filter(
        F.col("date_key").isNull()
        | F.col("customer_key").isNull()
        | F.col("product_key").isNull()
    ).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate fact

# COMMAND ----------

# DBTITLE 1,Validate fact
# Check for duplicate sale IDs in the fact data.
duplicate_sales = (
    fact_df
    .groupBy("sale_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write the fact table using MERGE

# COMMAND ----------

# Get the fact Delta table so we can merge the sales records into it.
fact_delta = DeltaTable.forName(
    spark,
    FACT_TABLE
)

# COMMAND ----------

# DBTITLE 1,Write the fact table using MERGE
# Merge the sales data into the fact table.
# Update existing sales and insert new sales based on sale_id.
(
    fact_delta.alias("target")
    .merge(
        fact_df.alias("source"),
        "target.sale_id = source.sale_id"
    )
    .whenMatchedUpdate(
        set={
            "date_key": "source.date_key",
            "customer_key": "source.customer_key",
            "product_key": "source.product_key",
            "quantity": "source.quantity",
            "amount": "source.amount",
            "updated_timestamp": "source.updated_timestamp",
            "batch_id": "source.batch_id"
        }
    )
    .whenNotMatchedInsert(
        values={
            "sale_id": "source.sale_id",
            "date_key": "source.date_key",
            "customer_key": "source.customer_key",
            "product_key": "source.product_key",
            "quantity": "source.quantity",
            "amount": "source.amount",
            "created_timestamp": "source.created_timestamp",
            "updated_timestamp": "source.updated_timestamp",
            "batch_id": "source.batch_id"
        }
    )
    .execute()
)

# COMMAND ----------

# DBTITLE 1,sale_id is unique
# Check the total fact records and make sure sale_id is unique.
fact_table_df = spark.table(FACT_TABLE)

print("Fact records:", fact_table_df.count())
print(
    "Distinct sale IDs:",
    fact_table_df.select("sale_id").distinct().count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test SCD2

# COMMAND ----------

# DBTITLE 1,Verify that the fact table
# Verify that the fact table is using the correct customer_key.
# Join the fact table with the customer dimension using customer_key
# so we can see which customer version is linked to each sale.
# We check Bob and Carol because they have customer changes in the
# incremental data, so they are useful for testing SCD Type 2.
# The result shows the sale, customer details, and customer_key used
# by each sale.
fact_table_df.alias("f") \
    .join(
        customer_dim_df.alias("c"),
        F.col("f.customer_key") == F.col("c.customer_key"),
        "inner"
    ) \
    .filter(
        F.col("c.customer_name").isin("Bob", "Carol")
    ) \
    .select(
        "f.sale_id",
        "c.customer_name",
        "c.region",
        "c.segment",
        "f.customer_key"
    ) \
    .orderBy("f.sale_id") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test idempotency

# COMMAND ----------

# Run the fact table merge again to test idempotency.
# Existing sale_ids are updated instead of creating duplicate records,
# while any new sale_ids would be inserted into the fact table.
(
    fact_delta.alias("target")
    .merge(
        fact_df.alias("source"),
        "target.sale_id = source.sale_id"
    )
    .whenMatchedUpdate(
        set={
            "date_key": "source.date_key",
            "customer_key": "source.customer_key",
            "product_key": "source.product_key",
            "quantity": "source.quantity",
            "amount": "source.amount",
            "updated_timestamp": "source.updated_timestamp",
            "batch_id": "source.batch_id"
        }
    )
    .whenNotMatchedInsert(
        values={
            "sale_id": "source.sale_id",
            "date_key": "source.date_key",
            "customer_key": "source.customer_key",
            "product_key": "source.product_key",
            "quantity": "source.quantity",
            "amount": "source.amount",
            "created_timestamp": "source.created_timestamp",
            "updated_timestamp": "source.updated_timestamp",
            "batch_id": "source.batch_id"
        }
    )
    .execute()
)

# COMMAND ----------

# Check the fact table count after running the same load again.
# The record count should remain the same if the merge is idempotent.
print(
    "Fact records after rerun:",
    spark.table(FACT_TABLE).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Final validation

# COMMAND ----------

# Final validation of the fact table.
# Check record counts, duplicate sale IDs, missing dimension keys,
# and invalid quantity or amount values.
final_fact = spark.table(FACT_TABLE)

print("FACT VALIDATION")
print("Total records:", final_fact.count())
print("Distinct sale IDs:", final_fact.select("sale_id").distinct().count())
print("Null date keys:", final_fact.filter(F.col("date_key").isNull()).count())
print("Null customer keys:", final_fact.filter(F.col("customer_key").isNull()).count())
print("Null product keys:", final_fact.filter(F.col("product_key").isNull()).count())
print("Invalid quantity:", final_fact.filter(F.col("quantity") <= 0).count())
print("Invalid amount:", final_fact.filter(F.col("amount") < 0).count())