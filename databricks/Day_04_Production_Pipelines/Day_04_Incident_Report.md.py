# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 – Incident Report
# MAGIC
# MAGIC ## Incident Overview
# MAGIC - Date: November 26, 2026
# MAGIC - Platform: Databricks Lakeflow Jobs
# MAGIC - Job: Day_04_Retail_Production_Job
# MAGIC - Failed task: Data_Quality
# MAGIC - Incident type: Controlled failure simulation
# MAGIC
# MAGIC ## Root Cause
# MAGIC The job parameter `simulate_failure` was intentionally set to `true` to simulate a task failure.
# MAGIC
# MAGIC ## Error Message
# MAGIC `RuntimeError: Day 4 intentional failure for recovery testing`
# MAGIC
# MAGIC ## Investigation
# MAGIC The job execution graph and task error logs were inspected. The Ingest, Transform and Aggregate tasks had completed successfully, while the Data_Quality task failed.
# MAGIC
# MAGIC ## Resolution
# MAGIC - Changed the failure simulation parameter to `false`.
# MAGIC - Used the Repair Run functionality.
# MAGIC - Reran only the failed Data_Quality task.
# MAGIC - Preserved the successful upstream task executions.
# MAGIC
# MAGIC ## Final Results
# MAGIC - Job recovery: Successful
# MAGIC - Scheduled execution: Successful
# MAGIC - Scheduled time: 5:42 PM (Asia/Colombo)
# MAGIC - Scheduled run duration: 1 minute 38 seconds
# MAGIC - Invalid records in Silver: 0
# MAGIC - Duplicate order IDs: 0
# MAGIC - Unknown customers: 0
# MAGIC
# MAGIC ## Lessons Learned
# MAGIC Lakeflow Jobs supports selective task recovery, reducing unnecessary processing and compute resource consumption. Job parameters and error logs help diagnose and resolve failures efficiently.