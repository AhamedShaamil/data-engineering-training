# Databricks notebook source

# Reuse our existing Day 3 Auto Loader configuration.
# Do not reset checkpoints or recreate source files.

base_path = "/Volumes/workspace/default/day1_data/day3"

source_path = f"{base_path}/incoming_orders"
schema_path = f"{base_path}/schema"
checkpoint_path = f"{base_path}/checkpoint"

target_table = "workspace.bronze.orders_incremental"

# Read new CSV files using Auto Loader.
orders = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
)

# Process only files not previously ingested.
query = (
    orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

# Wait until ingestion is complete.
query.awaitTermination()

print("Ingestion completed successfully.")
