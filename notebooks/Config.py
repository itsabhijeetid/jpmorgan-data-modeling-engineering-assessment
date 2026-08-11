# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import DecimalType

CATALOG_NAME = "jpmc_assessment"

BRONZE_SCHEMA = f"{CATALOG_NAME}.bronze"
SILVER_SCHEMA = f"{CATALOG_NAME}.silver"
GOLD_SCHEMA = f"{CATALOG_NAME}.gold"

BRONZE_TABLE = f"{CATALOG_NAME}.bronze.raw_sales"
SILVER_TABLE = f"{CATALOG_NAME}.silver.sales_clean"
DQ_TABLE = f"{CATALOG_NAME}.gold.dq_quarantine"

CUSTOMER_DIM = f"{CATALOG_NAME}.gold.dim_customer"
PRODUCT_DIM = f"{CATALOG_NAME}.gold.dim_product"
DATE_DIM = f"{CATALOG_NAME}.gold.dim_date"
FACT_TABLE = f"{CATALOG_NAME}.gold.fact_sales"