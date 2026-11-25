# Databricks notebook source
# MAGIC %md
# MAGIC # Day 03 – Incremental Ingestion and Data Quality
# MAGIC
# MAGIC ## Objective
# MAGIC Implement incremental ingestion using Databricks Auto Loader, handle schema evolution, validate incoming records, and verify idempotency.
# MAGIC
# MAGIC ## 1. Environment Setup and Data Preparation
# MAGIC Configure source, schema and checkpoint locations, and prepare the first daily order file.

# COMMAND ----------

from pyspark.sql import functions as F

# Reuse our existing Day 1 and Day 2 data location
data_path = "/Volumes/workspace/default/day1_data"

# Separate locations for Day 3 incremental ingestion
day3_path = f"{data_path}/day3"
source_path = f"{day3_path}/incoming_orders"
schema_path = f"{day3_path}/schema"
checkpoint_path = f"{day3_path}/checkpoint"

# Create a folder for incoming daily CSV files
dbutils.fs.mkdirs(source_path)

# Check available source files
print("Available source files:")
display(dbutils.fs.ls(data_path))

# Check the existing Silver table structure
print("Existing Silver table schema:")
spark.table("workspace.silver.orders_clean").printSchema()

print("Day 3 setup completed.")

# COMMAND ----------

import csv
import io

# Required order columns from our original dataset
columns = [
    "order_id", "order_ts", "customer_id",
    "product_id", "store_id", "quantity",
    "unit_price", "updated_ts"
]

silver = spark.table("workspace.silver.orders_clean")

# Load existing customer and product IDs
customers = (
    spark.read.option("header", True)
    .csv(f"{data_path}/customers.csv")
    .select("customer_id").distinct()
)

products = (
    spark.read.option("header", True)
    .csv(f"{data_path}/products.csv")
    .select("product_id").distinct()
)

# Select an existing valid order with known references
sample = (
    silver.select(*columns)
    .join(customers, "customer_id", "inner")
    .join(products, "product_id", "inner")
    .first()
)

assert sample is not None, "No matching valid order found."

# Generate new order IDs based on the existing data type
if dict(silver.dtypes)["order_id"] == "string":
    new_ids = [f"DAY3_{i:03d}" for i in range(1, 11)]
else:
    max_id = silver.agg(F.max("order_id")).first()[0]
    new_ids = [max_id + i for i in range(1, 11)]

# Prepare three valid new orders
rows = []

for i in range(3):
    row = sample.asDict()
    row["order_id"] = new_ids[i]
    row["order_ts"] = "2026-09-01 10:00:00"
    row["updated_ts"] = "2026-09-01 10:00:00"
    row["quantity"] = i + 1
    rows.append(row)

# Helper function to create a CSV file
def write_daily_csv(filename, records, fields):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(records)

    dbutils.fs.put(
        f"{source_path}/{filename}",
        buffer.getvalue(),
        False
    )

# Create the first incoming daily file
write_daily_csv("orders_20260901.csv", rows, columns)

display(dbutils.fs.ls(source_path))

print("First daily file created with 3 valid orders.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Incremental Ingestion and Schema Evolution
# MAGIC
# MAGIC - Ingest daily CSV files using Auto Loader.
# MAGIC - Track processed files using checkpoints.
# MAGIC - Introduce the optional payment_method column.
# MAGIC - Handle schema changes and late-arriving data.
# MAGIC
# MAGIC **Result:** Successfully ingested 10 records from two daily files.

# COMMAND ----------

# Separate incremental Bronze table for Day 3
bronze_target = "workspace.bronze.orders_incremental"

# Reusable Auto Loader ingestion function
def run_auto_loader():

    incoming = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("source_file"),
            F.current_timestamp().alias("ingested_at")
        )
    )

    # Process only unprocessed files and save as Delta
    query = (
        incoming.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_target)
    )

    query.awaitTermination()

# Execute the first incremental ingestion
run_auto_loader()

# Validate the ingestion result
display(spark.table(bronze_target))

print("Bronze row count:", spark.table(bronze_target).count())

# COMMAND ----------

# Add a new optional column for schema evolution
new_columns = columns + ["payment_method"]

# Helper function to create new test orders
def make_order(order_id, **changes):
    row = rows[0].copy()

    row.update({
        "order_id": order_id,
        "order_ts": "2026-09-02 10:00:00",
        "updated_ts": "2026-09-02 10:00:00",
        "quantity": 2,
        "payment_method": "CARD"
    })

    row.update(changes)
    return row

# Confirm our deliberately unknown customer does not exist
assert customers.filter(
    F.col("customer_id") == "UNKNOWN_CUSTOMER"
).count() == 0

second_rows = [
    # Valid new order
    make_order(new_ids[3]),

    # Valid late-arriving order
    make_order(
        new_ids[4],
        order_ts="2026-09-01 08:00:00",
        payment_method="CASH"
    ),

    # Invalid: missing product ID
    make_order(new_ids[5], product_id=None),

    # Invalid: duplicate order from the first file
    {**rows[0], "payment_method": None},

    # Invalid: incorrect date
    make_order(new_ids[6], order_ts="invalid_date"),

    # Invalid: negative quantity
    make_order(new_ids[7], quantity=-5),

    # Invalid: unknown customer
    make_order(new_ids[8], customer_id="UNKNOWN_CUSTOMER")
]

# Create the second daily file
write_daily_csv(
    "orders_20260902.csv",
    second_rows,
    new_columns
)

# Verify both files are available
display(dbutils.fs.ls(source_path))

print("Second daily file created.")
print("Total records:", len(second_rows))

# COMMAND ----------

from pyspark.sql import functions as F

# Restore the existing Day 3 paths
data_path = "/Volumes/workspace/default/day1_data"
day3_path = f"{data_path}/day3"

source_path = f"{day3_path}/incoming_orders"
schema_path = f"{day3_path}/schema"
checkpoint_path = f"{day3_path}/checkpoint"

bronze_target = "workspace.bronze.orders_incremental"

# Restore the Auto Loader function
def run_auto_loader():

    incoming = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("source_file"),
            F.current_timestamp().alias("ingested_at")
        )
    )

    # Reuse the existing checkpoint to prevent reprocessing
    query = (
        incoming.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_target)
    )

    query.awaitTermination()

# Process newly arrived files
run_auto_loader()

# Verify records from each source file
display(
    spark.table(bronze_target)
    .groupBy("source_file")
    .count()
)

# Verify schema evolution
display(
    spark.table(bronze_target)
    .select("order_id", "source_file", "payment_method")
    .orderBy("source_file", "order_id")
)

# Verify total records
print("Total Bronze records:",
      spark.table(bronze_target).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Validation and Quarantine
# MAGIC
# MAGIC Validate incoming records for missing product IDs, duplicate orders, invalid dates, negative quantities and unknown customers.
# MAGIC
# MAGIC Preserve rejected records in a separate Delta quarantine table with rejection reasons.
# MAGIC
# MAGIC **Result:** 5 valid records and 5 quarantined records.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

data_path = "/Volumes/workspace/default/day1_data"

# 1. Read incoming Bronze records
raw = spark.table("workspace.bronze.orders_incremental")

# Preserve the original record before modifying it
orders = raw.withColumn(
    "raw_record", F.to_json(F.struct(*raw.columns))
)

# 2. Safely convert data types
orders = (
    orders
    .withColumn("order_ts", F.expr("try_cast(order_ts AS TIMESTAMP)"))
    .withColumn("updated_ts", F.expr("try_cast(updated_ts AS TIMESTAMP)"))
    .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
    .withColumn("unit_price", F.expr("try_cast(unit_price AS DECIMAL(18,2))"))
)

# 3. Load known customer IDs
known_customers = (
    spark.read.option("header", True)
    .csv(f"{data_path}/customers.csv")
    .select(F.col("customer_id").alias("known_customer"))
    .distinct()
)

# Check whether customers exist
orders = orders.join(
    known_customers,
    orders.customer_id == known_customers.known_customer,
    "left"
)

# 4. Assign rejection reasons
orders = orders.withColumn(
    "rejection_reason",
    F.concat_ws("; ",
        F.when(
            F.col("order_id").isNull() |
            (F.trim("order_id") == ""),
            "Missing order ID"
        ),
        F.when(
            F.col("product_id").isNull() |
            (F.trim("product_id") == ""),
            "Missing product ID"
        ),
        F.when(
            F.col("order_ts").isNull() |
            F.col("updated_ts").isNull(),
            "Invalid date"
        ),
        F.when(
            F.col("quantity").isNull() |
            (F.col("quantity") <= 0),
            "Invalid quantity"
        ),
        F.when(
            F.col("unit_price").isNull() |
            (F.col("unit_price") < 0),
            "Invalid unit price"
        ),
        F.when(
            F.col("known_customer").isNull(),
            "Unknown customer"
        ),
        F.when(
            F.col("store_id").isNull() |
            (F.trim("store_id") == ""),
            "Missing store ID"
        )
    )
)

# 5. Rank records with the same order ID
duplicate_window = Window.partitionBy("order_id").orderBy(
    (F.col("rejection_reason") == "").desc(),
    F.col("updated_ts").desc_nulls_last(),
    F.col("source_file").asc(),
    F.col("raw_record").asc()
)

# Identify repeated order IDs
orders = (
    orders
    .withColumn("row_num", F.row_number().over(duplicate_window))
    .withColumn(
        "rejection_reason",
        F.when(
            (F.col("row_num") > 1) &
            (F.col("rejection_reason") == ""),
            "Duplicate order ID"
        ).otherwise(F.col("rejection_reason"))
    )
)

# 6. Separate valid and invalid records
valid_orders = orders.filter(F.col("rejection_reason") == "")
invalid_orders = orders.filter(F.col("rejection_reason") != "")

# 7. Verify the results
display(
    orders.select(
        "order_id", "source_file",
        "quantity", "rejection_reason"
    ).orderBy("source_file", "order_id")
)

print("Total records:", orders.count())
print("Valid records:", valid_orders.count())
print("Invalid records:", invalid_orders.count())

# COMMAND ----------

from pyspark.sql import functions as F

# Define the quarantine table name
quarantine_target = "workspace.silver.orders_quarantine_day3"

# Reuse the invalid records identified in Cell 6
quarantine = invalid_orders.select(
    "order_id",
    "source_file",
    "raw_record",
    "rejection_reason",
    F.current_timestamp().alias("processing_timestamp")
)

# Permanently save the rejected records as a Delta table
(
    quarantine.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(quarantine_target)
)

# Read the saved quarantine table
saved_quarantine = spark.table(quarantine_target)

# Verify the results
display(saved_quarantine)

print("Total quarantined records:", saved_quarantine.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver MERGE and Idempotency Verification
# MAGIC
# MAGIC Merge valid records into the incremental Silver Delta table.
# MAGIC
# MAGIC Rerun Auto Loader and MERGE to verify that previously processed files and orders do not create duplicates.
# MAGIC
# MAGIC **Result:** Idempotency test passed with zero duplicate order IDs in Silver.

# COMMAND ----------

silver_target = "workspace.silver.orders_incremental_clean"

# Select only the required clean order columns
clean_orders = valid_orders.select(
    "order_id",
    "order_ts",
    "customer_id",
    "product_id",
    "store_id",
    "quantity",
    "unit_price",
    "updated_ts",
    "payment_method"
)

# Create a temporary view for SQL operations
clean_orders.createOrReplaceTempView("day3_valid_orders")

# Create an empty Silver Delta table if it does not exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_target}
    USING DELTA
    AS SELECT * FROM day3_valid_orders
    WHERE 1 = 0
""")

# Reusable MERGE function
def merge_valid_orders():
    spark.sql(f"""
        MERGE INTO {silver_target} AS target
        USING day3_valid_orders AS source
        ON target.order_id = source.order_id

        WHEN MATCHED
            AND source.updated_ts > target.updated_ts
        THEN UPDATE SET *

        WHEN NOT MATCHED
        THEN INSERT *
    """)

# Execute the MERGE
merge_valid_orders()

# Verify the saved records
display(spark.table(silver_target).orderBy("order_id"))

print("Total clean Silver records:",
      spark.table(silver_target).count())

# COMMAND ----------

from pyspark.sql import functions as F

# Restore existing paths and table names
day3_path = "/Volumes/workspace/default/day1_data/day3"

source_path = f"{day3_path}/incoming_orders"
schema_path = f"{day3_path}/schema"
checkpoint_path = f"{day3_path}/checkpoint"

bronze_target = "workspace.bronze.orders_incremental"
silver_target = "workspace.silver.orders_incremental_clean"
quarantine_target = "workspace.silver.orders_quarantine_day3"

# 1. Record counts before rerunning
bronze_before = spark.table(bronze_target).count()
silver_before = spark.table(silver_target).count()

# 2. Rerun Auto Loader using the same checkpoint
incoming = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
    .select(
        "*",
        F.col("_metadata.file_name").alias("source_file"),
        F.current_timestamp().alias("ingested_at")
    )
)

query = (
    incoming.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(bronze_target)
)

query.awaitTermination()

# 3. Repeat the Silver MERGE from Cell 8
merge_valid_orders()

# 4. Check record counts after rerunning
bronze_after = spark.table(bronze_target).count()
silver_after = spark.table(silver_target).count()
quarantine_count = spark.table(quarantine_target).count()

# 5. Check for duplicate order IDs in Silver
duplicate_count = (
    spark.table(silver_target)
    .groupBy("order_id")
    .count()
    .filter("count > 1")
    .count()
)

# 6. Display the evidence
print("Bronze before:", bronze_before)
print("Bronze after:", bronze_after)

print("Silver before:", silver_before)
print("Silver after:", silver_after)

print("Quarantined records:", quarantine_count)
print("Duplicate order IDs in Silver:", duplicate_count)

# 7. Automatically verify the expected results
assert bronze_before == bronze_after == 10
assert silver_before == silver_after == 5
assert quarantine_count == 5
assert duplicate_count == 0

print("Idempotency test PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Final Results
# MAGIC
# MAGIC | Metric | Result |
# MAGIC |---|---:|
# MAGIC | Source files processed | 2 |
# MAGIC | Bronze records | 10 |
# MAGIC | Valid Silver records | 5 |
# MAGIC | Quarantined records | 5 |
# MAGIC | Bronze before and after rerun | 10 → 10 |
# MAGIC | Silver before and after rerun | 5 → 5 |
# MAGIC | Duplicate order IDs in Silver | 0 |
# MAGIC | Idempotency test | PASSED |
# MAGIC
# MAGIC **Day 3 Technical Assignment: Successfully Completed.**