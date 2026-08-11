# JPMorganChase Data Modelling & Engineering Assessment

## Overview

This project implements a modular Databricks data pipeline for transaction-level sales data.

The pipeline follows a **Bronze → Silver → Gold** architecture and covers:

- Raw data ingestion
- Data type conversion and data quality validation
- Invalid record quarantine
- Dimensional modeling
- Surrogate key generation
- Customer SCD Type 2 processing
- Fact table creation
- Incremental processing
- Delta Lake MERGE operations
- Data validation and idempotency checks

The project uses the catalog:

`jpmc_assessment`

## Architecture

```text
                 Source CSV Files
                  /            \
                 /              \
        SampleData_Base.csv   SampleData_Updates.csv
                 \              /
                  \            /
                   v          v
              01_Bronze_Ingestion
                       |
                       v
                Bronze Delta Table
                       |
                       v
             02_Silver_Data_Quality
                  /           \
                 /             \
                v               v
        Silver Delta Table   DQ Quarantine
                |
                v
             03_Dimensions
          /       |        \
         v        v         v
    dim_date  dim_product  dim_customer
                           (SCD Type 2)
          \       |       /
           \      |      /
            v     v     v
              04_Fact_Sales
                    |
                    v
              fact_sales
```

## Project Structure

```text
JPMorganChase_Data_Modeling_Assessment/
│
├── README.md
│
├── notebooks/
│   ├── 00_Config
│   ├── 01_Bronze_Ingestion
│   ├── 02_Silver_Data_Quality
│   ├── 03_Dimensions
│   └── 04_Fact_Sales
│
└── data/
    ├── SampleData_Base.csv
    └── SampleData_Updates.csv
```

## Notebook Execution Order

Run the notebooks in this order:

```text
00_Config
    ↓
01_Bronze_Ingestion
    ↓
02_Silver_Data_Quality
    ↓
03_Dimensions
    ↓
04_Fact_Sales
```

## 1. Configuration

### Notebook

`00_Config`

Contains the common catalog, schema, and table names used throughout the pipeline.

### Catalog

```text
jpmc_assessment
```

### Schemas

```text
jpmc_assessment.bronze
jpmc_assessment.silver
jpmc_assessment.gold
```

### Tables

```text
jpmc_assessment.bronze.raw_sales
jpmc_assessment.silver.sales_clean
jpmc_assessment.gold.dq_quarantine
jpmc_assessment.gold.dim_customer
jpmc_assessment.gold.dim_product
jpmc_assessment.gold.dim_date
jpmc_assessment.gold.fact_sales
```

## 2. Bronze Layer – Raw Ingestion

### Notebook

`01_Bronze_Ingestion`

Reads the base and incremental CSV files and loads them into the Bronze Delta table.

The pipeline adds:

- `source_file`
- `ingestion_timestamp`
- `batch_id`

The two batches are identified as:

```text
initial_load
incremental_load_001
```

The Bronze layer preserves source values so invalid values can be handled during Silver validation.

### Output

```text
jpmc_assessment.bronze.raw_sales
```

## 3. Silver Layer – Data Quality & Transformation

### Notebook

`02_Silver_Data_Quality`

Converts raw values into the required data types and applies data quality rules.

### Validation checks

- Missing `sale_id`
- Invalid `sale_date`
- Missing `customer_name`
- Missing `product_name`
- Invalid `amount`
- Negative `amount`
- Invalid `quantity`
- Quantity less than or equal to zero

Multiple validation errors for the same record are combined into `dq_error`.

### Processing

```text
Bronze
   |
   v
Type conversion
   |
   v
Data quality validation
   |
   +----------------------+
   |                      |
   v                      v
Valid records       Invalid records
   |                      |
   v                      v
Silver table         Quarantine table
```

### Outputs

Valid records:

```text
jpmc_assessment.silver.sales_clean
```

Invalid records:

```text
jpmc_assessment.gold.dq_quarantine
```

Invalid records are retained with the reason for rejection.

## 4. Dimensional Modeling

### Notebook

`03_Dimensions`

Creates the dimensions required by the sales fact table.

### Date Dimension

Table:

```text
jpmc_assessment.gold.dim_date
```

Generated from the minimum and maximum valid sale dates.

Columns:

- `date_key`
- `full_date`
- `day`
- `month`
- `month_name`
- `quarter`
- `year`
- `day_of_week`
- `day_name`

The `date_key` uses the `YYYYMMDD` format.

### Product Dimension

Table:

```text
jpmc_assessment.gold.dim_product
```

Unique products are extracted from Silver data and assigned surrogate keys.

Columns:

- `product_key`
- `product_name`
- `created_timestamp`
- `updated_timestamp`

### Customer Dimension

Table:

```text
jpmc_assessment.gold.dim_customer
```

The customer dimension uses **Slowly Changing Dimension Type 2**.

Columns:

- `customer_key`
- `customer_name`
- `region`
- `segment`
- `effective_date`
- `expiry_date`
- `is_current`
- `created_timestamp`
- `updated_timestamp`

### SCD Type 2 processing

Incremental customer records are classified as:

```text
New
Changed
Unchanged
```

**New customer:** receives a new surrogate key and a current record.

**Changed customer:**

1. Expire the existing current record.
2. Set `is_current` to `false`.
3. Set `expiry_date` to the effective date of the new version.
4. Insert a new record with a new surrogate key.
5. Mark the new record as current.

**Unchanged customer:** no new dimension record is created.

This preserves customer history while maintaining one current version per customer.

## 5. Fact Table

### Notebook

`04_Fact_Sales`

Creates the transaction-level sales fact table.

Table:

```text
jpmc_assessment.gold.fact_sales
```

Columns:

- `sale_id`
- `date_key`
- `customer_key`
- `product_key`
- `quantity`
- `amount`
- `created_timestamp`
- `updated_timestamp`
- `batch_id`

The fact table uses surrogate keys from the dimensions.

### Dimension key resolution

```text
sale_date
    ↓
dim_date
    ↓
date_key
```

```text
product_name
    ↓
dim_product
    ↓
product_key
```

For customers, the customer name and sale date are used to identify the correct SCD Type 2 version.

```text
customer_name
      +
sale_date between effective_date and expiry_date
      ↓
customer_key
```

This ensures historical sales use the correct customer version.

## 6. Incremental Processing

Incremental customer records are identified using:

```text
batch_id = incremental_load_001
```

Incoming customer records are compared with current dimension records.

For the fact table, Delta Lake `MERGE` uses `sale_id` as the matching key.

```text
Existing sale_id
      ↓
Update existing fact record

New sale_id
      ↓
Insert new fact record
```

This supports idempotent processing because rerunning the same batch does not create duplicate `sale_id` records.

## 7. Data Validation

Validation is performed at multiple stages.

### Bronze validation

- Total record count
- Initial batch count
- Incremental batch count
- Source records

### Silver validation

- Valid record count
- Invalid record count
- Quarantine count
- Invalid amounts
- Invalid quantities
- Missing customer names

### Dimension validation

- Customer versions
- Current customer versions
- Product records
- Dimension surrogate keys

### Fact validation

- Total fact records
- Distinct sale IDs
- Missing date keys
- Missing customer keys
- Missing product keys
- Invalid quantities
- Invalid amounts
- Duplicate sale IDs

The expected result for unresolved dimension keys and invalid fact values is zero.

## 8. Technology Stack

- Databricks
- Apache Spark / PySpark
- Delta Lake
- Unity Catalog
- SQL
- Python

## 9. Design Decisions

### Bronze Layer

Stores source data with ingestion metadata and preserves source values for downstream validation.

### Silver Layer

Contains cleaned, typed, and validated records.

Invalid records are quarantined rather than silently dropped.

### Gold Layer

Contains business-ready dimensional and fact tables.

### Surrogate Keys

Surrogate keys are generated in the dimensions and referenced by the fact table.

### SCD Type 2

Customer history is preserved by maintaining multiple versions of customer records.

### Delta MERGE

Delta `MERGE` is used for incremental fact processing and updating existing records.

### Modular Notebooks

Each notebook has a clear responsibility, making the pipeline easier to understand, test, maintain, and execute.

## 10. How to Run

Execute the notebooks in this order:

```text
1. 00_Config
2. 01_Bronze_Ingestion
3. 02_Silver_Data_Quality
4. 03_Dimensions
5. 04_Fact_Sales
```

The required source CSV files should be available at the configured source location before running the Bronze ingestion notebook.

## Expected Data Flow

```text
CSV Source Files
       ↓
     Bronze
       ↓
Silver + Data Quality
       ↓
Dimensions
       ↓
Fact Sales
```

Final Gold-layer model:

```text
                    dim_date
                       |
                       |
dim_customer ---- fact_sales ---- dim_product
```

`fact_sales` contains transaction measures and references the dimension tables using surrogate keys.
