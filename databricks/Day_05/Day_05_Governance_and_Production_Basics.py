# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- List all catalogs visible to the current user
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- List schemas inside our workspace catalog
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Inspect tables and views in the Day 4 pipeline schema
# MAGIC SHOW TABLES IN workspace.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Identify the user executing the notebook
# MAGIC SELECT CURRENT_USER() AS current_user;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check existing permissions on the workspace catalog
# MAGIC SHOW GRANTS ON CATALOG workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check existing access privileges on the Silver schema
# MAGIC SHOW GRANTS ON SCHEMA workspace.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Inspect existing permissions on the Gold schema
# MAGIC SHOW GRANTS ON SCHEMA workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Identify cleaned data tables
# MAGIC SHOW TABLES IN workspace.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Identify available business reporting tables
# MAGIC SHOW TABLES IN workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Allow the group to use the workspace catalog
# MAGIC GRANT USE CATALOG ON CATALOG workspace
# MAGIC TO `gold_readers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Allow the group to use the Gold schema
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.gold
# MAGIC TO `gold_readers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Allow the group to read Gold tables
# MAGIC GRANT SELECT ON SCHEMA workspace.gold
# MAGIC TO `gold_readers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify permissions configured on the Gold schema
# MAGIC SHOW GRANTS ON SCHEMA workspace.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify catalog-level permissions for gold_readers
# MAGIC SHOW GRANTS `gold_readers` ON CATALOG workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check whether the Gold table inherits SELECT permission
# MAGIC SHOW GRANTS `gold_readers`
# MAGIC ON TABLE workspace.gold.daily_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check permissions on the cleaned orders table
# MAGIC SHOW GRANTS `gold_readers`
# MAGIC ON TABLE workspace.silver.orders_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a separate schema for development and testing
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a separate schema to demonstrate production organization
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.prod;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Verify that development and production schemas exist
# MAGIC SHOW SCHEMAS IN workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Baseline query: retrieve all available columns
# MAGIC SELECT *
# MAGIC FROM workspace.silver.orders_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Retrieve only the columns required for sales analysis
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     store_id,
# MAGIC     quantity,
# MAGIC     unit_price
# MAGIC FROM workspace.silver.orders_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Retrieve only the required columns
# MAGIC -- and orders with quantities greater than 3
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     store_id,
# MAGIC     quantity,
# MAGIC     unit_price
# MAGIC FROM workspace.silver.orders_clean
# MAGIC WHERE quantity > 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Inspect how Databricks plans to execute
# MAGIC -- a filtered sales aggregation query
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC     store_id,
# MAGIC     SUM(quantity * unit_price) AS total_revenue
# MAGIC FROM workspace.silver.orders_clean
# MAGIC WHERE quantity > 3
# MAGIC GROUP BY store_id;