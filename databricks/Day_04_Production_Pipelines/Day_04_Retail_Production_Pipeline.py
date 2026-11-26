
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==================================================
# 1. BRONZE LAYER - Combine existing source data
# ==================================================

@dp.materialized_view(name="day4_bronze_orders")
def bronze_orders():

    columns = [
        "order_id", "order_ts", "customer_id",
        "product_id", "store_id", "quantity",
        "unit_price", "updated_ts"
    ]

    # Read the original orders from Day 2.
    original = spark.read.table("workspace.bronze.orders")

    # Read the incremental orders from Day 3.
    incremental = spark.read.table(
        "workspace.bronze.orders_incremental"
    )

    # Standardize source types before combining.
    original = original.select(
        *[F.col(c).cast("string").alias(c) for c in columns]
    ).withColumn("source_priority", F.lit(0))

    incremental = incremental.select(
        *[F.col(c).cast("string").alias(c) for c in columns]
    ).withColumn("source_priority", F.lit(1))

    return original.unionByName(incremental)


# ==================================================
# 2. DATA QUALITY EXPECTATIONS
# ==================================================

quality_rules = {
    "valid_order_id": "order_id IS NOT NULL AND order_id <> ''",
    "positive_quantity": "quantity > 0",
    "non_negative_price": "unit_price >= 0",
    "known_customer": "known_customer = true",
    "valid_order_date": "order_ts IS NOT NULL",
    "valid_updated_date": "updated_ts IS NOT NULL",
    "valid_product": "product_id IS NOT NULL AND product_id <> ''",
    "valid_store": "store_id IS NOT NULL AND store_id <> ''"
}


@dp.materialized_view(name="day4_valid_orders")
@dp.expect_all_or_drop(quality_rules)
def valid_orders():

    # Convert raw data into appropriate data types.
    orders = spark.read.table(
        "workspace.default.day4_bronze_orders"
    ).selectExpr(
        "trim(order_id) AS order_id",
        "try_cast(order_ts AS TIMESTAMP) AS order_ts",
        "trim(customer_id) AS customer_id",
        "trim(product_id) AS product_id",
        "trim(store_id) AS store_id",
        "try_cast(quantity AS INT) AS quantity",
        "try_cast(unit_price AS DECIMAL(18,2)) AS unit_price",
        "try_cast(updated_ts AS TIMESTAMP) AS updated_ts",
        "source_priority"
    )

    # Identify customers that exist in reference data.
    customers = (
        spark.read.table("workspace.silver.customers_reference")
        .select(
            F.trim(F.col("customer_id").cast("string"))
            .alias("customer_id")
        )
        .distinct()
        .withColumn("known_customer", F.lit(True))
    )

    # Unknown customers receive a null quality flag.
    return orders.join(customers, "customer_id", "left")


# ==================================================
# 3. SILVER LAYER - Remove duplicate orders
# ==================================================

@dp.materialized_view(name="day4_silver_orders")
def silver_orders():

    orders = spark.read.table(
        "workspace.default.day4_valid_orders"
    )

    # Keep the latest valid version of each order.
    window = Window.partitionBy("order_id").orderBy(
        F.col("updated_ts").desc(),
        F.col("source_priority").desc(),
        F.col("order_ts").desc(),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("store_id"),
        F.col("quantity"),
        F.col("unit_price")
    )

    return (
        orders
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number", "source_priority", "known_customer")
        .withColumn(
            "revenue",
            F.col("quantity") * F.col("unit_price")
        )
        .withColumn("order_date", F.to_date("order_ts"))
    )


# ==================================================
# 4. GOLD LAYER - Daily sales
# ==================================================

@dp.materialized_view(name="day4_gold_daily_sales")
def daily_sales():

    return (
        spark.read.table("workspace.default.day4_silver_orders")
        .groupBy("order_date", "store_id")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.sum("quantity").alias("total_units")
        )
    )


# ==================================================
# 5. GOLD LAYER - Product sales
# ==================================================

@dp.materialized_view(name="day4_gold_product_sales")
def product_sales():

    return (
        spark.read.table("workspace.default.day4_silver_orders")
        .groupBy("order_date", "product_id")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_units")
        )
    )


# ==================================================
# 6. GOLD LAYER - Customer sales
# ==================================================

@dp.materialized_view(name="day4_gold_customer_sales")
def customer_sales():

    return (
        spark.read.table("workspace.default.day4_silver_orders")
        .groupBy("customer_id")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.avg("revenue").alias("average_order_value")
        )
    )
