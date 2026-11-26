# Databricks notebook source

from pyspark.sql import functions as F

# Read the final cleaned orders.
silver = spark.table(
    "workspace.default.day4_silver_orders"
)

# Read the consolidated Gold summary.
summary = spark.table(
    "workspace.default.day4_sales_summary"
).first()

# Check for invalid records.
invalid_count = silver.filter(
    F.col("order_id").isNull() |
    (F.trim(F.col("order_id")) == "") |
    F.col("quantity").isNull() |
    (F.col("quantity") <= 0) |
    F.col("unit_price").isNull() |
    (F.col("unit_price") < 0)
).count()

# Check whether duplicate order IDs exist.
duplicate_count = (
    silver.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check whether all customers are known.
customers = (
    spark.table("workspace.silver.customers_reference")
    .select(
        F.trim(F.col("customer_id").cast("string"))
        .alias("customer_id")
    )
    .distinct()
)

unknown_customers = silver.join(
    customers, "customer_id", "left_anti"
).count()

# Calculate Silver totals for reconciliation.
totals = silver.agg(
    F.count("*").alias("total_orders"),
    F.sum("revenue").alias("total_revenue"),
    F.sum("quantity").alias("total_units")
).first()

# Display validation results.
print("Invalid records:", invalid_count)
print("Duplicate order IDs:", duplicate_count)
print("Unknown customers:", unknown_customers)
print("Silver orders:", totals.total_orders)
print("Gold orders:", summary.total_orders)
print("Silver revenue:", totals.total_revenue)
print("Gold revenue:", summary.total_revenue)

# Fail the task if any validation check fails.
assert totals.total_orders > 0, "Silver is empty"
assert invalid_count == 0, "Invalid records detected"
assert duplicate_count == 0, "Duplicate orders detected"
assert unknown_customers == 0, "Unknown customers detected"

assert totals.total_orders == summary.total_orders, \
    "Order count mismatch"

assert totals.total_revenue == summary.total_revenue, \
    "Revenue mismatch"

assert totals.total_units == summary.total_units, \
    "Unit count mismatch"

print("All data quality checks passed successfully.")

# Parameter for controlled failure testing.
dbutils.widgets.text("simulate_failure", "false")

simulate_failure = dbutils.widgets.get(
    "simulate_failure"
).strip().lower()

# Deliberately fail only when testing is enabled.
if simulate_failure == "true":
    raise RuntimeError(
        "Day 4 intentional failure for recovery testing"
    )

print("Data quality task completed successfully.")