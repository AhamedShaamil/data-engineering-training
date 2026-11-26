# Databricks notebook source

from pyspark.sql import functions as F

# Read the Gold daily sales output.
daily_sales = spark.table(
    "workspace.default.day4_gold_daily_sales"
)

# Create a consolidated business sales summary.
sales_summary = daily_sales.agg(
    F.sum("total_revenue").alias("total_revenue"),
    F.sum("order_count").alias("total_orders"),
    F.sum("total_units").alias("total_units")
)

# Save the summary as a persistent Delta table.
sales_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.day4_sales_summary")

display(sales_summary)

print("Aggregation completed successfully.")
