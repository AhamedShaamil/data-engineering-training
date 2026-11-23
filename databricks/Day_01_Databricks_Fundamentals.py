# Databricks notebook source

# Day 1: Databricks Fundamentals
# Task 1: Load and inspect 100,000 retail orders

# Step 1: Specify the uploaded folder location
folder_path = "/Volumes/workspace/default/day1_data"

# Step 2: Build the orders file path
orders_path = folder_path + "/orders.csv"

# Step 3: Read the CSV into a PySpark DataFrame
orders_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(orders_path)
)

# Step 4: Count the total number of records
total_orders = orders_raw.count()
print("Total Orders:", total_orders)

# Step 5: Inspect the inferred schema
print("\nInferred Schema:")
orders_raw.printSchema()

# Step 6: Display the first five records
print("\nFirst Five Order Records:")
display(orders_raw.limit(5))


# COMMAND ----------


# Day 1 - Task 2: Correct data types

from pyspark.sql import functions as F

# Step 1: Convert unit_price to Decimal
orders_typed = orders_raw.withColumn(
    "unit_price",
    F.col("unit_price").cast("decimal(12,2)")
)

# Step 2: Display the corrected schema
print("Corrected Schema:")
orders_typed.printSchema()

# Step 3: Verify the number of records
print("Total Orders:", orders_typed.count())

# Step 4: Display sample records
display(orders_typed.limit(5))


# COMMAND ----------


# Day 1 - Task 3: Prepare orders for SQL

# Create a temporary SQL view from the DataFrame
orders_typed.createOrReplaceTempView("orders")

print("Temporary SQL view 'orders' created successfully!")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC -- Day 1 - Task 3: Calculate revenue by store
# MAGIC
# MAGIC SELECT
# MAGIC     store_id,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(quantity * unit_price) AS total_revenue
# MAGIC FROM orders
# MAGIC GROUP BY store_id
# MAGIC ORDER BY total_revenue DESC;
# MAGIC

# COMMAND ----------


# Day 1 - Task 3: Revenue calculation using PySpark

from pyspark.sql import functions as F

# Step 1: Group orders by store and calculate revenue
store_revenue_pyspark = (
    orders_typed
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum(
            F.col("quantity") * F.col("unit_price")
        ).alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
)

# Step 2: Display the results
print("Store Revenue Using PySpark:")
display(store_revenue_pyspark)


# COMMAND ----------


# Day 1 - Task 4: Load and inspect products data

# Step 1: Specify products file location
products_path = "/Volumes/workspace/default/day1_data/products.csv"

# Step 2: Read products CSV
products_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(products_path)
)

# Step 3: Count products
print("Total Products:", products_raw.count())

# Step 4: Inspect schema
print("\nProducts Schema:")
products_raw.printSchema()

# Step 5: Display first five products
print("\nFirst Five Products:")
display(products_raw.limit(5))


# COMMAND ----------


# Day 1 - Task 4: Join orders and products

from pyspark.sql import functions as F

# Step 1: Join orders with product information
joined_orders = orders_typed.join(
    products_raw.select(
        "product_id",
        "product_name",
        "category"
    ),
    on="product_id",
    how="inner"
)

# Step 2: Verify the joined record count
print("Original Orders:", orders_typed.count())
print("Joined Records:", joined_orders.count())

# Step 3: Calculate revenue for each product
top_products = (
    joined_orders
    .groupBy("product_id", "product_name")
    .agg(
        F.sum(
            F.col("quantity") * F.col("unit_price")
        ).alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

# Step 4: Display the top 10 products
print("Top 10 Products by Revenue:")
display(top_products)


# COMMAND ----------


# Day 1 - Task 4: Prepare products for SQL JOIN

# Register products as a temporary SQL view
products_raw.createOrReplaceTempView("products")

print("Products SQL view created successfully!")


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC -- Day 1 - Task 4: Top 10 products by revenue using SQL
# MAGIC
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     SUM(o.quantity * o.unit_price) AS total_revenue
# MAGIC FROM orders o
# MAGIC INNER JOIN products p
# MAGIC     ON o.product_id = p.product_id
# MAGIC GROUP BY
# MAGIC     p.product_id,
# MAGIC     p.product_name
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Day 1 — Databricks Fundamentals
# MAGIC
# MAGIC ## Objective
# MAGIC Learn Databricks fundamentals and perform basic data transformations using SQL and PySpark.
# MAGIC
# MAGIC ## Dataset
# MAGIC - Source: Synthetic retail dataset
# MAGIC - Orders: 100,000 records
# MAGIC - Files: orders.csv and products.csv
# MAGIC - Storage: Unity Catalog managed volume
# MAGIC
# MAGIC ## Tasks Completed
# MAGIC
# MAGIC 1. Loaded CSV files using PySpark.
# MAGIC 2. Inspected the inferred data schema.
# MAGIC 3. Converted unit_price to Decimal(12,2).
# MAGIC 4. Calculated store revenue using SQL.
# MAGIC 5. Repeated the calculation using PySpark.
# MAGIC 6. Joined orders with products using product_id.
# MAGIC 7. Identified the top 10 products by revenue.
# MAGIC
# MAGIC ## Key Findings
# MAGIC
# MAGIC - All 100,000 orders were successfully loaded.
# MAGIC - Store S07 generated the highest revenue.
# MAGIC - The orders-products JOIN returned 100,000 records.
# MAGIC - Product P009 generated the highest revenue.
# MAGIC - SQL and PySpark produced matching results.
# MAGIC
# MAGIC ## SQL vs PySpark
# MAGIC
# MAGIC SQL was simpler for writing straightforward aggregation and JOIN queries.
# MAGIC
# MAGIC PySpark was useful for programmatically loading CSV files, inspecting schemas, converting data types and building transformations using Python.
# MAGIC
# MAGIC ## Challenges and Solutions
# MAGIC
# MAGIC 1. PATH_NOT_FOUND:
# MAGIC    Corrected the CSV file path.
# MAGIC
# MAGIC 2. NameError:
# MAGIC    Reran the ingestion cell to recreate the missing DataFrame variable.
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC I successfully used Databricks, Spark SQL and PySpark to load, transform and analyze retail sales data.
# MAGIC