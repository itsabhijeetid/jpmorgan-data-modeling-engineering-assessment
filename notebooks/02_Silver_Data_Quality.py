# Databricks notebook source
# DBTITLE 1,Config
# MAGIC %run "/Workspace/Users/mail.abhiprofessional@gmail.com/JPMorganChase_Technical Evaluation/Config"

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

# COMMAND ----------

# DBTITLE 1,typed_df
# Convert the raw string values into the required data types.
# Invalid amount and quantity values become NULL so they can be
# identified by the data quality checks instead of breaking the pipeline.
typed_df = (
    bronze_df
    .withColumn(
        "sale_date_typed",
        F.to_date(
            F.trim(F.col("sale_date")),
            "M/d/yyyy"
        )
    )
    .withColumn(
        "amount_typed",
        F.expr("try_cast(amount AS DECIMAL(18,2))")
    )
    .withColumn(
        "quantity_typed",
        F.expr("try_cast(quantity AS INT)")
    )
)

# COMMAND ----------

display(typed_df.select(
    "sale_id",
    "sale_date",
    "sale_date_typed",
    "amount",
    "amount_typed",
    "quantity",
    "quantity_typed"
))

# COMMAND ----------

# MAGIC %md
# MAGIC # Create validation rules

# COMMAND ----------

# DBTITLE 1,validation rules
# Apply the data quality rules and store the reason for each failed check.
# Multiple issues for the same record are combined into a single error message.
validated_df = (
    typed_df
    .withColumn(
        "dq_error",
        F.concat_ws(
            "; ",
            
            F.when(
                F.col("sale_id").isNull(),
                F.lit("Missing sale_id")
            ),

            F.when(
                F.col("sale_date_typed").isNull(),
                F.lit("Invalid sale_date")
            ),

            F.when(
                F.trim(F.col("customer_name")) == "",
                F.lit("Missing customer_name")
            ),

            F.when(
                F.col("customer_name").isNull(),
                F.lit("Missing customer_name")
            ),

            F.when(
                F.trim(F.col("product_name")) == "",
                F.lit("Missing product_name")
            ),

            F.when(
                F.col("product_name").isNull(),
                F.lit("Missing product_name")
            ),

            F.when(
                F.col("amount_typed").isNull(),
                F.lit("Invalid amount")
            ),

            F.when(
                F.col("amount_typed") < 0,
                F.lit("Negative amount")
            ),

            F.when(
                F.col("quantity_typed").isNull(),
                F.lit("Invalid quantity")
            ),

            F.when(
                F.col("quantity_typed") <= 0,
                F.lit("Quantity must be greater than zero")
            )
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Separate valid and invalid records

# COMMAND ----------

# DBTITLE 1,valid invalid records
# Separate valid records from records that failed the data quality checks.
valid_df = (
    validated_df
    .filter(F.col("dq_error") == "")
)

invalid_df = (
    validated_df
    .filter(F.col("dq_error") != "")
)

# COMMAND ----------

# DBTITLE 1,Valid, invalid count
print("Valid records:", valid_df.count())
print("Invalid records:", invalid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Create the Silver table

# COMMAND ----------

# DBTITLE 1,silver_df
# Select the validated records and prepare them for the Silver layer.
# Trim text fields and use the converted date, amount and quantity values.
silver_df = (
    valid_df
    .select(
        F.col("sale_id"),
        F.col("sale_date_typed").alias("sale_date"),
        F.trim(F.col("customer_name")).alias("customer_name"),
        F.trim(F.col("region")).alias("region"),
        F.trim(F.col("segment")).alias("segment"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.col("amount_typed").alias("amount"),
        F.col("quantity_typed").alias("quantity"),
        F.col("batch_id"),
        F.col("source_file"),
        F.col("ingestion_timestamp")
    )
)

# COMMAND ----------

# DBTITLE 1,SILVER_TABLE create
# Create the Silver table if it doesn't already exist.
# This table will store the cleaned and validated sales data.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    sale_id BIGINT,
    sale_date DATE,
    customer_name STRING,
    region STRING,
    segment STRING,
    product_name STRING,
    amount DECIMAL(18,2),
    quantity INT,
    batch_id STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,SILVER_TABLE write
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write invalid records to quarantine

# COMMAND ----------

# DBTITLE 1,quarantine_df
# Prepare the invalid records for the quarantine table.
# Keeping the original values and add the reason for the validation failure.
quarantine_df = (
    invalid_df
    .select(
        "sale_id",
        "sale_date",
        "customer_name",
        "region",
        "segment",
        "product_name",
        "amount",
        "quantity",
        F.col("dq_error").alias("error_reason"),
        "batch_id"
    )
    .withColumn(
        "quarantined_timestamp",
        F.current_timestamp()
    )
)

# COMMAND ----------

# DBTITLE 1,DQ_TABLE write
(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(DQ_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Inspecting the rejected records

# COMMAND ----------

# DBTITLE 1,DQ_TABLE display
display(spark.table(DQ_TABLE)
    .select(
        "sale_id",
        "error_reason",
        "batch_id"
    )
    .orderBy("sale_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Important validation

# COMMAND ----------

# DBTITLE 1,record counts
print(
    "Bronze:",
    spark.table(BRONZE_TABLE).count()
)

print(
    "Silver:",
    spark.table(SILVER_TABLE).count()
)

print(
    "Quarantine:",
    spark.table(DQ_TABLE).count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check Silver schema

# COMMAND ----------

# DBTITLE 1,SILVER_TABLE schema
spark.table(SILVER_TABLE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate Silver Table

# COMMAND ----------

# DBTITLE 1,Validate Silver Table
# Check that no invalid values added into the Silver table.
silver = spark.table(SILVER_TABLE)

print(
    "Invalid amounts:",
    silver.filter(F.col("amount") < 0).count()
)

print(
    "Invalid quantities:",
    silver.filter(F.col("quantity") <= 0).count()
)

print(
    "Null customer names:",
    silver.filter(F.col("customer_name").isNull()).count()
)

# COMMAND ----------

# DBTITLE 1,DQ_TABLE count
print("Quarantine:", spark.table(DQ_TABLE).count())

# COMMAND ----------

print("Validated:", validated_df.count())
print("Valid:", valid_df.count())
print("Invalid:", invalid_df.count())