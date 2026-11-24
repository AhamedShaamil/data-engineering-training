# Databricks notebook source
# MAGIC %md
# MAGIC # Day 02 – Delta Lake and Medallion Architecture
# MAGIC
# MAGIC ## Objective
# MAGIC Build a reliable data pipeline using Bronze, Silver and Gold
# MAGIC Delta tables, including data cleaning, validation,
# MAGIC aggregation and MERGE operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select the existing catalog
# MAGIC USE CATALOG workspace;
# MAGIC
# MAGIC -- Create separate schemas for the Medallion Architecture
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC -- Verify that all three schemas exist
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# Set the source dataset path in the Unity Catalog volume
data_path = "/Volumes/workspace/default/day1_data"

# List available files to verify source data availability
display(dbutils.fs.ls(data_path))

# COMMAND ----------

# Read raw orders with column headers and preserve values as strings
orders_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{data_path}/orders.csv")
)

# Preview the first 10 records to inspect the source data
display(orders_raw.limit(10))

# Count source records for later data reconciliation
print("Total raw orders:", orders_raw.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer – Raw Data Ingestion
# MAGIC
# MAGIC Preserve the original orders data in a Delta table
# MAGIC without applying cleaning or transformations.

# COMMAND ----------

# Store raw orders as a permanent Bronze Delta table
# Overwrite the existing table if it already exists
orders_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.orders")

print("Bronze orders table created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview 10 records to verify successful Bronze data ingestion
# MAGIC SELECT * FROM workspace.bronze.orders
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count Bronze records to verify completeness against the source CSV
# MAGIC SELECT COUNT(*) AS bronze_row_count
# MAGIC FROM workspace.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer – Data Cleaning and Validation
# MAGIC
# MAGIC Transform raw Bronze data into clean and reliable data
# MAGIC by correcting data types, handling null values,
# MAGIC validating records and removing duplicate orders.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view for data type conversion and standardization
# MAGIC CREATE OR REPLACE TEMP VIEW orders_typed AS
# MAGIC SELECT
# MAGIC     -- Remove unnecessary spaces and convert empty IDs to NULL
# MAGIC     NULLIF(TRIM(order_id), '') AS order_id,
# MAGIC     TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
# MAGIC
# MAGIC     -- Standardize reference IDs to uppercase and handle empty values
# MAGIC     NULLIF(UPPER(TRIM(customer_id)), '') AS customer_id,
# MAGIC     NULLIF(UPPER(TRIM(product_id)), '') AS product_id,
# MAGIC     NULLIF(UPPER(TRIM(store_id)), '') AS store_id,
# MAGIC
# MAGIC     -- Safely convert data types; invalid values return NULL
# MAGIC     TRY_CAST(quantity AS INT) AS quantity,
# MAGIC     TRY_CAST(unit_price AS DECIMAL(18,2)) AS unit_price,
# MAGIC     TRY_CAST(updated_ts AS TIMESTAMP) AS updated_ts
# MAGIC
# MAGIC FROM workspace.bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view containing only valid order records
# MAGIC CREATE OR REPLACE TEMP VIEW orders_valid AS
# MAGIC SELECT *
# MAGIC FROM orders_typed
# MAGIC
# MAGIC -- Ensure all required IDs and timestamps are not missing
# MAGIC WHERE order_id IS NOT NULL
# MAGIC     AND order_ts IS NOT NULL
# MAGIC     AND updated_ts IS NOT NULL
# MAGIC     AND customer_id IS NOT NULL
# MAGIC     AND product_id IS NOT NULL
# MAGIC     AND store_id IS NOT NULL
# MAGIC
# MAGIC     -- Keep only positive quantities and non-negative prices
# MAGIC     AND quantity > 0
# MAGIC     AND unit_price >= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a permanent Silver Delta table containing clean, unique orders
# MAGIC CREATE OR REPLACE TABLE workspace.silver.orders_clean
# MAGIC USING DELTA AS
# MAGIC
# MAGIC -- Rank records within each order ID, prioritizing the latest update
# MAGIC -- Additional sorting columns provide consistent handling of tied timestamps
# MAGIC WITH ranked_orders AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY order_id
# MAGIC             ORDER BY updated_ts DESC, order_ts DESC,
# MAGIC                      customer_id, product_id, store_id,
# MAGIC                      quantity, unit_price
# MAGIC         ) AS row_num
# MAGIC     FROM orders_valid
# MAGIC )
# MAGIC
# MAGIC -- Select the required columns without the temporary ranking column
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     order_ts,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     store_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     updated_ts
# MAGIC FROM ranked_orders
# MAGIC
# MAGIC -- Retain only the latest valid record for each order ID
# MAGIC WHERE row_num = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview 10 records to verify the cleaned Silver table
# MAGIC SELECT * FROM workspace.silver.orders_clean
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reconcile row counts across Bronze, validation and Silver stages
# MAGIC SELECT
# MAGIC     bronze_count,
# MAGIC     valid_count,
# MAGIC     silver_count,
# MAGIC
# MAGIC     -- Calculate the number of invalid and duplicate records removed
# MAGIC     bronze_count - valid_count AS invalid_records,
# MAGIC     valid_count - silver_count AS duplicates_removed
# MAGIC
# MAGIC -- Count records at each processing stage
# MAGIC -- CROSS JOIN combines the three single-row counts into one result
# MAGIC FROM
# MAGIC     (SELECT COUNT(*) AS bronze_count
# MAGIC      FROM workspace.bronze.orders)
# MAGIC CROSS JOIN
# MAGIC     (SELECT COUNT(*) AS valid_count
# MAGIC      FROM orders_valid)
# MAGIC CROSS JOIN
# MAGIC     (SELECT COUNT(*) AS silver_count
# MAGIC      FROM workspace.silver.orders_clean);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer – Reference Data Enrichment
# MAGIC
# MAGIC Enrich cleaned orders with product and customer
# MAGIC information to support business reporting.

# COMMAND ----------

# Load product reference data while preserving original values as strings
products_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{data_path}/products.csv")
)

# Preview the first 10 product records
display(products_raw.limit(10))

# Verify column data types and total product count before processing
products_raw.printSchema()
print("Total products:", products_raw.count())

# COMMAND ----------

# Save raw product reference data as a permanent Bronze Delta table
# Overwrite the existing table to support repeatable ingestion
products_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.products")

print("Bronze products table created successfully!")

# COMMAND ----------

from pyspark.sql import functions as F

# Check available source files for customer reference data
files = [f.name for f in dbutils.fs.ls(data_path)]

# Load the original customer file if available
if "customers.csv" in files:
    customers = (
        spark.read
        .option("header", True)
        .csv(f"{data_path}/customers.csv")
        .withColumn("reference_source", F.lit("source_file"))
    )

# If unavailable, create a placeholder reference using unique customer IDs
# Keep unknown customer details as NULL rather than inventing values
else:
    customers = (
        spark.table("workspace.silver.orders_clean")
        .select("customer_id")
        .distinct()
        .withColumn("customer_name", F.lit(None).cast("string"))
        .withColumn("segment", F.lit(None).cast("string"))
        .withColumn("city", F.lit(None).cast("string"))
        .withColumn("created_date", F.lit(None).cast("string"))
        .withColumn("reference_source", F.lit("placeholder"))
    )

# Preview customer records and verify the total count
display(customers.limit(10))
print("Total customers:", customers.count())

# COMMAND ----------

# Standardize customer IDs, clean text fields and safely convert dates
# Remove records with missing customer IDs and identical duplicate rows
customers_clean = customers.selectExpr(
    "NULLIF(UPPER(TRIM(customer_id)), '') AS customer_id",
    "TRIM(customer_name) AS customer_name",
    "TRIM(segment) AS segment",
    "TRIM(city) AS city",
    "TRY_CAST(created_date AS DATE) AS created_date",
    "reference_source"
).filter("customer_id IS NOT NULL").distinct()

# Store cleaned customer reference data as a permanent Silver Delta table
customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.customers_reference")

print("Customer reference table created successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a cleaned product reference table in the Silver layer
# MAGIC CREATE OR REPLACE TABLE workspace.silver.products_reference
# MAGIC USING DELTA AS
# MAGIC
# MAGIC -- Standardize product IDs, clean text fields and remove identical duplicates
# MAGIC SELECT DISTINCT
# MAGIC     UPPER(TRIM(product_id)) AS product_id,
# MAGIC     TRIM(product_name) AS product_name,
# MAGIC     TRIM(category) AS category
# MAGIC FROM workspace.bronze.products
# MAGIC
# MAGIC -- Exclude products with NULL or empty product IDs
# MAGIC WHERE product_id IS NOT NULL
# MAGIC     AND TRIM(product_id) <> '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Silver view enriched with product and customer information
# MAGIC CREATE OR REPLACE VIEW workspace.silver.orders_enriched AS
# MAGIC
# MAGIC SELECT
# MAGIC     o.*,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     c.customer_name,
# MAGIC     c.segment,
# MAGIC     c.city,
# MAGIC     c.reference_source,
# MAGIC
# MAGIC     -- Verify product matching and customer reference availability
# MAGIC     p.product_id IS NOT NULL AS product_matched,
# MAGIC     COALESCE(c.reference_source = 'source_file', FALSE)
# MAGIC         AS customer_verified,
# MAGIC
# MAGIC     -- Calculate revenue for each order
# MAGIC     o.quantity * o.unit_price AS revenue
# MAGIC
# MAGIC FROM workspace.silver.orders_clean o
# MAGIC
# MAGIC -- Use LEFT JOIN to preserve all orders even if reference data is missing
# MAGIC LEFT JOIN workspace.silver.products_reference p
# MAGIC     ON o.product_id = p.product_id
# MAGIC
# MAGIC LEFT JOIN workspace.silver.customers_reference c
# MAGIC     ON o.customer_id = c.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview 10 enriched orders to verify joined reference data and calculated revenue
# MAGIC SELECT *
# MAGIC FROM workspace.silver.orders_enriched
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validate enriched Silver data for completeness and order uniqueness
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC
# MAGIC     -- Identify missing product references and unverified customers
# MAGIC     COUNT_IF(NOT product_matched) AS unmatched_products,
# MAGIC     COUNT_IF(NOT customer_verified) AS unverified_customers,
# MAGIC
# MAGIC     -- Calculate total revenue for reconciliation with the Gold layer
# MAGIC     SUM(revenue) AS total_revenue
# MAGIC FROM workspace.silver.orders_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer – Business Aggregations
# MAGIC
# MAGIC Transform cleaned and enriched Silver data into
# MAGIC business-ready Delta tables for daily sales,
# MAGIC product performance and customer analysis.
# MAGIC
# MAGIC Validate the aggregated outputs against the
# MAGIC Silver layer to ensure revenue and order count accuracy.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a permanent Gold Delta table for daily sales
# MAGIC CREATE OR REPLACE TABLE workspace.gold.daily_sales
# MAGIC USING DELTA AS
# MAGIC
# MAGIC SELECT
# MAGIC     -- Extract only the date from the order timestamp
# MAGIC     CAST(order_ts AS DATE) AS sales_date,
# MAGIC
# MAGIC     -- Identify the store
# MAGIC     store_id,
# MAGIC
# MAGIC     -- Count the number of orders
# MAGIC     COUNT(*) AS total_orders,
# MAGIC
# MAGIC     -- Calculate the total quantity sold
# MAGIC     SUM(quantity) AS total_units,
# MAGIC
# MAGIC     -- Calculate the total sales revenue
# MAGIC     SUM(revenue) AS total_revenue,
# MAGIC
# MAGIC     -- Calculate the average revenue per order
# MAGIC     ROUND(AVG(revenue), 2) AS avg_order_value
# MAGIC
# MAGIC -- Read cleaned and enriched Silver data
# MAGIC FROM workspace.silver.orders_enriched
# MAGIC
# MAGIC -- Summarize orders by date and store
# MAGIC GROUP BY CAST(order_ts AS DATE), store_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Display the daily sales summary
# MAGIC SELECT *
# MAGIC FROM workspace.gold.daily_sales
# MAGIC ORDER BY sales_date, store_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a Gold Delta table for product performance
# MAGIC CREATE OR REPLACE TABLE workspace.gold.product_sales
# MAGIC USING DELTA AS
# MAGIC
# MAGIC SELECT
# MAGIC     -- Extract the sales date
# MAGIC     CAST(order_ts AS DATE) AS sales_date,
# MAGIC
# MAGIC     -- Identify each product
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC
# MAGIC     -- Count orders associated with each product
# MAGIC     COUNT(*) AS total_orders,
# MAGIC
# MAGIC     -- Calculate the total units sold
# MAGIC     SUM(quantity) AS total_units,
# MAGIC
# MAGIC     -- Calculate the total product revenue
# MAGIC     SUM(revenue) AS total_revenue
# MAGIC
# MAGIC -- Use enriched Silver data
# MAGIC FROM workspace.silver.orders_enriched
# MAGIC
# MAGIC -- Aggregate by date and product
# MAGIC GROUP BY
# MAGIC     CAST(order_ts AS DATE),
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Identify the top 10 products by total revenue
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC
# MAGIC     -- Combine daily product sales
# MAGIC     SUM(total_units) AS units_sold,
# MAGIC     SUM(total_revenue) AS revenue
# MAGIC
# MAGIC FROM workspace.gold.product_sales
# MAGIC
# MAGIC GROUP BY product_id, product_name
# MAGIC
# MAGIC -- Highest revenue first
# MAGIC ORDER BY revenue DESC
# MAGIC
# MAGIC -- Return only the top 10 products
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a Gold Delta table for customer analysis
# MAGIC CREATE OR REPLACE TABLE workspace.gold.customer_sales
# MAGIC USING DELTA AS
# MAGIC
# MAGIC SELECT
# MAGIC     -- Identify each customer
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     segment,
# MAGIC     city,
# MAGIC
# MAGIC     -- Calculate the number of customer orders
# MAGIC     COUNT(*) AS total_orders,
# MAGIC
# MAGIC     -- Calculate the total units purchased
# MAGIC     SUM(quantity) AS total_units,
# MAGIC
# MAGIC     -- Calculate the customer's total spending
# MAGIC     SUM(revenue) AS total_revenue,
# MAGIC
# MAGIC     -- Calculate average spending per order
# MAGIC     ROUND(AVG(revenue), 2) AS avg_order_value,
# MAGIC
# MAGIC     -- Identify the customer's first order in the dataset
# MAGIC     MIN(order_ts) AS first_order_date,
# MAGIC
# MAGIC     -- Identify the customer's latest order in the dataset
# MAGIC     MAX(order_ts) AS last_order_date
# MAGIC
# MAGIC -- Use enriched Silver data
# MAGIC FROM workspace.silver.orders_enriched
# MAGIC
# MAGIC -- Summarize information for each customer
# MAGIC GROUP BY
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     segment,
# MAGIC     city;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Display the top 10 customers by total spending
# MAGIC SELECT *
# MAGIC FROM workspace.gold.customer_sales
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Analyse sales performance by customer segment
# MAGIC SELECT
# MAGIC     segment,
# MAGIC
# MAGIC     -- Count customers in each segment
# MAGIC     COUNT(*) AS total_customers,
# MAGIC
# MAGIC     -- Calculate total orders and revenue
# MAGIC     SUM(total_orders) AS total_orders,
# MAGIC     SUM(total_revenue) AS total_revenue,
# MAGIC
# MAGIC     -- Calculate the correct weighted average order value
# MAGIC     ROUND(
# MAGIC         SUM(total_revenue) / SUM(total_orders), 2
# MAGIC     ) AS avg_order_value
# MAGIC
# MAGIC FROM workspace.gold.customer_sales
# MAGIC
# MAGIC GROUP BY segment
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify all tables in the Gold schema
# MAGIC SHOW TABLES IN workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Calculate the original Silver totals as our baseline
# MAGIC WITH silver_totals AS (
# MAGIC     SELECT
# MAGIC         COUNT(*) AS total_orders,
# MAGIC         SUM(revenue) AS total_revenue
# MAGIC     FROM workspace.silver.orders_enriched
# MAGIC ),
# MAGIC
# MAGIC -- Calculate totals from each Gold table
# MAGIC gold_totals AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         'Daily Sales' AS table_name,
# MAGIC         COUNT(*) AS table_rows,
# MAGIC         SUM(total_orders) AS total_orders,
# MAGIC         SUM(total_revenue) AS total_revenue
# MAGIC     FROM workspace.gold.daily_sales
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'Product Sales',
# MAGIC         COUNT(*),
# MAGIC         SUM(total_orders),
# MAGIC         SUM(total_revenue)
# MAGIC     FROM workspace.gold.product_sales
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'Customer Sales',
# MAGIC         COUNT(*),
# MAGIC         SUM(total_orders),
# MAGIC         SUM(total_revenue)
# MAGIC     FROM workspace.gold.customer_sales
# MAGIC )
# MAGIC
# MAGIC -- Compare Gold totals against the Silver baseline
# MAGIC SELECT
# MAGIC     g.table_name,
# MAGIC     g.table_rows,
# MAGIC     g.total_orders,
# MAGIC     g.total_revenue,
# MAGIC
# MAGIC     -- Check for missing or duplicated orders
# MAGIC     g.total_orders - s.total_orders AS order_difference,
# MAGIC
# MAGIC     -- Check for revenue discrepancies
# MAGIC     g.total_revenue - s.total_revenue AS revenue_difference,
# MAGIC
# MAGIC     -- Return the final validation status
# MAGIC     CASE
# MAGIC         WHEN g.total_orders = s.total_orders
# MAGIC          AND g.total_revenue = s.total_revenue
# MAGIC         THEN 'PASS'
# MAGIC         ELSE 'FAIL'
# MAGIC     END AS validation_status
# MAGIC
# MAGIC FROM gold_totals g
# MAGIC CROSS JOIN silver_totals s;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake – MERGE Operation
# MAGIC
# MAGIC Demonstrate incremental updates using Delta MERGE
# MAGIC to update an existing order and insert a new order
# MAGIC without rebuilding the entire Silver table.
# MAGIC
# MAGIC Validate the results and refresh the Gold tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Inspect the existing order and confirm the test ID
# MAGIC -- does not already exist before performing MERGE
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     quantity * unit_price AS revenue,
# MAGIC     updated_ts
# MAGIC FROM workspace.silver.orders_clean
# MAGIC WHERE order_id IN ('O0000001', 'O_DAY2_TEST_001');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create temporary incoming data for the MERGE test
# MAGIC CREATE OR REPLACE TEMP VIEW incoming_orders AS
# MAGIC
# MAGIC SELECT * FROM VALUES
# MAGIC
# MAGIC     -- Existing order: change quantity from 2 to 3
# MAGIC     (
# MAGIC         'O0000001',
# MAGIC         TIMESTAMP '2026-09-19 23:50:49',
# MAGIC         'C0253',
# MAGIC         'P025',
# MAGIC         'S03',
# MAGIC         3,
# MAGIC         CAST(12999 AS DECIMAL(18,2)),
# MAGIC         TIMESTAMP '2026-10-01 10:00:00'
# MAGIC     ),
# MAGIC
# MAGIC     -- New test order: quantity is 1
# MAGIC     (
# MAGIC         'O_DAY2_TEST_001',
# MAGIC         TIMESTAMP '2026-10-01 10:00:00',
# MAGIC         'C0253',
# MAGIC         'P025',
# MAGIC         'S03',
# MAGIC         1,
# MAGIC         CAST(12999 AS DECIMAL(18,2)),
# MAGIC         TIMESTAMP '2026-10-01 10:00:00'
# MAGIC     )
# MAGIC
# MAGIC AS incoming (
# MAGIC     order_id,
# MAGIC     order_ts,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     store_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     updated_ts
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify the two incoming records before merging
# MAGIC SELECT * FROM incoming_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Target: existing Silver Delta table
# MAGIC -- Source: newly received sample records
# MAGIC
# MAGIC MERGE INTO workspace.silver.orders_clean AS target
# MAGIC USING incoming_orders AS source
# MAGIC
# MAGIC -- Match records using their unique order ID
# MAGIC ON target.order_id = source.order_id
# MAGIC
# MAGIC -- Update only when the incoming record is newer
# MAGIC WHEN MATCHED
# MAGIC     AND source.updated_ts > target.updated_ts
# MAGIC THEN UPDATE SET
# MAGIC     target.quantity = source.quantity,
# MAGIC     target.updated_ts = source.updated_ts
# MAGIC
# MAGIC -- Insert a new record if the order ID does not exist
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     order_id,
# MAGIC     order_ts,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     store_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     updated_ts
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.order_id,
# MAGIC     source.order_ts,
# MAGIC     source.customer_id,
# MAGIC     source.product_id,
# MAGIC     source.store_id,
# MAGIC     source.quantity,
# MAGIC     source.unit_price,
# MAGIC     source.updated_ts
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify both the updated and newly inserted orders
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     quantity * unit_price AS revenue,
# MAGIC     updated_ts
# MAGIC FROM workspace.silver.orders_clean
# MAGIC WHERE order_id IN ('O0000001', 'O_DAY2_TEST_001');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check row count, uniqueness and revenue after MERGE
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC     SUM(revenue) AS total_revenue
# MAGIC FROM workspace.silver.orders_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Inspect Delta Lake transaction history
# MAGIC -- Confirm the MERGE operation and its metrics
# MAGIC DESCRIBE HISTORY workspace.silver.orders_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify order count, uniqueness and revenue after MERGE
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC     COUNT_IF(NOT product_matched) AS unmatched_products,
# MAGIC     COUNT_IF(NOT customer_verified) AS unverified_customers,
# MAGIC     SUM(revenue) AS total_revenue
# MAGIC FROM workspace.silver.orders_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-MERGE Gold Refresh and Final Validation
# MAGIC
# MAGIC Refresh all Gold business tables after the MERGE
# MAGIC operation and reconcile their totals against Silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Refresh daily sales using the updated Silver data
# MAGIC CREATE OR REPLACE TABLE workspace.gold.daily_sales
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CAST(order_ts AS DATE) AS sales_date,
# MAGIC     store_id,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(quantity) AS total_units,
# MAGIC     SUM(revenue) AS total_revenue,
# MAGIC     ROUND(AVG(revenue), 2) AS avg_order_value
# MAGIC FROM workspace.silver.orders_enriched
# MAGIC GROUP BY CAST(order_ts AS DATE), store_id;
# MAGIC
# MAGIC
# MAGIC -- Refresh product sales
# MAGIC CREATE OR REPLACE TABLE workspace.gold.product_sales
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     CAST(order_ts AS DATE) AS sales_date,
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(quantity) AS total_units,
# MAGIC     SUM(revenue) AS total_revenue
# MAGIC FROM workspace.silver.orders_enriched
# MAGIC GROUP BY CAST(order_ts AS DATE),
# MAGIC          product_id, product_name, category;
# MAGIC
# MAGIC
# MAGIC -- Refresh customer sales
# MAGIC CREATE OR REPLACE TABLE workspace.gold.customer_sales
# MAGIC USING DELTA AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     segment,
# MAGIC     city,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(quantity) AS total_units,
# MAGIC     SUM(revenue) AS total_revenue,
# MAGIC     ROUND(AVG(revenue), 2) AS avg_order_value,
# MAGIC     MIN(order_ts) AS first_order_date,
# MAGIC     MAX(order_ts) AS last_order_date
# MAGIC FROM workspace.silver.orders_enriched
# MAGIC GROUP BY customer_id, customer_name, segment, city;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Collect the final totals from Silver and Gold
# MAGIC WITH final_totals AS (
# MAGIC
# MAGIC     SELECT 'Silver Enriched' AS layer,
# MAGIC            COUNT(*) AS total_orders,
# MAGIC            SUM(revenue) AS total_revenue
# MAGIC     FROM workspace.silver.orders_enriched
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'Gold Daily Sales',
# MAGIC            SUM(total_orders),
# MAGIC            SUM(total_revenue)
# MAGIC     FROM workspace.gold.daily_sales
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'Gold Product Sales',
# MAGIC            SUM(total_orders),
# MAGIC            SUM(total_revenue)
# MAGIC     FROM workspace.gold.product_sales
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT 'Gold Customer Sales',
# MAGIC            SUM(total_orders),
# MAGIC            SUM(total_revenue)
# MAGIC     FROM workspace.gold.customer_sales
# MAGIC )
# MAGIC
# MAGIC -- Compare every output against the expected final totals
# MAGIC SELECT *,
# MAGIC     CASE
# MAGIC         WHEN total_orders = 100001
# MAGIC          AND total_revenue = 7528531000.00
# MAGIC         THEN 'PASS'
# MAGIC         ELSE 'FAIL'
# MAGIC     END AS validation_status
# MAGIC FROM final_totals;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2 – Conclusion
# MAGIC
# MAGIC ### Completed Tasks
# MAGIC - Created Bronze, Silver and Gold Delta tables.
# MAGIC - Ingested 100,000 raw order records.
# MAGIC - Implemented type casting, data validation and deduplication.
# MAGIC - Joined customer and product reference data.
# MAGIC - Created daily, product and customer sales tables.
# MAGIC - Performed Delta MERGE to update one existing order
# MAGIC   and insert one new training order.
# MAGIC - Refreshed Gold tables after MERGE.
# MAGIC - Successfully reconciled Silver and Gold outputs.
# MAGIC
# MAGIC ### Final Results
# MAGIC - Original Bronze records: 100,000
# MAGIC - Final Silver records: 100,001
# MAGIC - Unique orders: 100,001
# MAGIC - Duplicate order IDs: 0
# MAGIC - Unmatched products: 0
# MAGIC - Unverified customers: 0
# MAGIC - Final revenue: 7,528,531,000.00
# MAGIC - Final validation: PASS
# MAGIC
# MAGIC ### Limitations
# MAGIC - MERGE used simulated training records.
# MAGIC - Gold tables require an explicit refresh.
# MAGIC - Automated incremental ingestion will be implemented
# MAGIC   in Day 3.