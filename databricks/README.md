# Databricks – Data Engineering Training

## Overview
This repository documents my hands-on data engineering internship training using Databricks, SQL, PySpark, Delta Lake and Lakeflow.

## Day 1 – Databricks Fundamentals

### Completed Tasks
- Explored Databricks workspace and compute.
- Loaded 100,000 order records from CSV.
- Inspected schemas and corrected data types.
- Performed filtering, joining and aggregation using SQL and PySpark.
- Calculated revenue by store.
- Identified the top 10 products by revenue.

### Status
Completed ✅

## Day 2 – Delta Lake and Medallion Architecture

### Completed Tasks
- Created Bronze, Silver and Gold schemas.
- Ingested raw data into Delta tables.
- Implemented data cleaning, type casting and null handling.
- Implemented deduplication using ROW_NUMBER.
- Joined customer and product reference data.
- Created daily, product and customer sales tables.
- Performed MERGE to update an existing order and insert a new training order.
- Refreshed Gold tables and performed final reconciliation.

### Data Pipeline Architecture

```text
Source CSV Files
       |
       v
Bronze Layer (Raw Data)
       |
       v
Silver Layer (Cleaned and Enriched Data)
       |
       v
Gold Layer (Business Aggregations)
```

### Final Validation Results

| Metric | Result |
|---|---:|
| Original Bronze records | 100,000 |
| Final Silver records | 100,001 |
| Unique orders | 100,001 |
| Duplicate order IDs | 0 |
| Unmatched products | 0 |
| Unverified customers | 0 |
| Total revenue | 7,528,531,000.00 |
| Final reconciliation | PASS |

### Limitations
- MERGE was demonstrated using simulated training records.
- Gold tables require an explicit refresh.
- Incremental ingestion was implemented separately in Day 3.

### Status
Completed ✅

## Day 3 – Incremental Ingestion and Data Quality

### Completed Tasks
- Implemented incremental file ingestion using Databricks Auto Loader and cloudFiles.
- Configured persistent checkpoints and schema storage.
- Processed two simulated daily CSV files without reprocessing previously ingested files.
- Demonstrated schema evolution by introducing the optional payment_method column.
- Resolved the UnknownFieldException through a successful ingestion retry.
- Handled late-arriving data.
- Implemented data quality validation and deduplication.
- Created a quarantine Delta table containing rejected records and rejection reasons.
- Loaded valid records into Silver using Delta MERGE.
- Verified idempotency by rerunning Auto Loader and MERGE.

### Incremental Pipeline Architecture

```text
Daily CSV Files
       |
       v
Auto Loader
(Checkpoint + Schema Evolution)
       |
       v
Bronze Delta Table
       |
       v
Data Quality Validation
       |
       +-------------------+
       |                   |
       v                   v
Silver Delta Table    Quarantine Table
(Valid Records)       (Rejected Records)
       |
       v
Idempotency Verification
```

### Data Quality Validation

| Validation Scenario | Rejection Reason | Status |
|---|---|---|
| Duplicate order ID | Duplicate order ID | PASS |
| Missing product ID | Missing product ID | PASS |
| Invalid order date | Invalid date | PASS |
| Negative quantity | Invalid quantity | PASS |
| Unknown customer | Unknown customer | PASS |

### Checkpoint and Schema Configuration

- Source: `/Volumes/workspace/default/day1_data/day3/incoming_orders`
- Checkpoint: `/Volumes/workspace/default/day1_data/day3/checkpoint`
- Schema location: `/Volumes/workspace/default/day1_data/day3/schema`
- Schema evolution mode: `addNewColumns`
- Streaming trigger: `AvailableNow`

### Delta Tables

| Layer | Table |
|---|---|
| Bronze | workspace.bronze.orders_incremental |
| Silver | workspace.silver.orders_incremental_clean |
| Quarantine | workspace.silver.orders_quarantine_day3 |

### Final Validation Results

| Metric | Result |
|---|---:|
| Source files processed | 2 |
| First daily file records | 3 |
| Second daily file records | 7 |
| Total Bronze records | 10 |
| Valid Silver records | 5 |
| Quarantined records | 5 |
| Bronze records before rerun | 10 |
| Bronze records after rerun | 10 |
| Silver records before rerun | 5 |
| Silver records after rerun | 5 |
| Duplicate order IDs in Silver | 0 |
| Schema evolution | PASS |
| Idempotency test | PASS |

### Key Learning Outcomes
- Checkpoint-based incremental ingestion.
- Schema inference and schema evolution.
- Late-arriving data handling.
- Safe data type conversion using try_cast.
- Referential validation and deduplication.
- Quarantine and rejection handling.
- Conditional Delta MERGE and idempotency.

### Limitations
- Daily CSV files were simulated for training purposes.
- Separate incremental tables were used to preserve the original Day 2 dataset.
- Auto Loader ingestion is incremental, while data quality validation uses batch processing over the training Bronze table.
- The quarantine table is maintained as a validation snapshot rather than a historical rejection log.
- Managed pipeline orchestration and scheduling were implemented in Day 4.

### Status
Completed ✅

## Day 4 – Production Pipelines and Scheduling

### Objective

Convert the existing medallion architecture into a managed, automated data pipeline with data quality expectations, task dependencies, scheduled execution and failure recovery.

### Completed Tasks
- Created a Lakeflow Spark Declarative Pipeline using the PySpark pipelines API.
- Combined existing Bronze orders and incremental order records.
- Implemented managed Bronze, Silver and Gold datasets using materialized views.
- Configured data quality expectations using expect_all_or_drop.
- Validated customer IDs against existing customer reference data.
- Implemented latest-record deduplication using window functions.
- Created daily sales, product sales and customer sales aggregations.
- Reused the existing Auto Loader configuration and persistent checkpoints.
- Created a consolidated business sales summary.
- Implemented final data quality checks and Silver-to-Gold reconciliation.
- Created a Lakeflow Job with four dependent tasks.
- Configured job parameters, retry settings and scheduled execution.
- Simulated an intentional task failure using a configurable parameter.
- Investigated the failure using task error logs.
- Successfully repaired the failed task without restarting the entire workflow.
- Verified successful automatic execution through the Databricks scheduler.

### Managed Pipeline Architecture

```text
Original Bronze Orders    Incremental Bronze Orders
          |                         |
          +------------+------------+
                       |
                       v
             Managed Bronze Dataset
                       |
                       v
             Data Quality Expectations
                       |
                       v
             Managed Silver Dataset
             (Validated and Deduplicated)
                       |
          +------------+------------+
          |            |            |
          v            v            v
      Daily Sales  Product Sales  Customer Sales
          |            |            |
          +------------+------------+
                       |
                 Gold Outputs
```

### Managed Pipeline Configuration

| Setting | Value |
|---|---|
| Pipeline name | Day_04_Retail_Production_Pipeline |
| Platform | Lakeflow Spark Declarative Pipelines |
| Language | Python / PySpark |
| Catalog | workspace |
| Schema | default |
| Managed dataset type | Materialized Views |
| Compute | Serverless |

### Managed Pipeline Outputs

| Layer | Dataset |
|---|---|
| Bronze | workspace.default.day4_bronze_orders |
| Validation | workspace.default.day4_valid_orders |
| Silver | workspace.default.day4_silver_orders |
| Gold | workspace.default.day4_gold_daily_sales |
| Gold | workspace.default.day4_gold_product_sales |
| Gold | workspace.default.day4_gold_customer_sales |
| Reporting | workspace.default.day4_sales_summary |

### Data Quality Expectations

| Expectation | Validation Rule |
|---|---|
| Valid order ID | Order ID must not be null or empty |
| Positive quantity | Quantity must be greater than zero |
| Non-negative price | Unit price must be greater than or equal to zero |
| Known customer | Customer ID must exist in the reference dataset |
| Valid order date | Order timestamp must not be null |
| Valid updated date | Updated timestamp must not be null |
| Valid product ID | Product ID must not be null or empty |
| Valid store ID | Store ID must not be null or empty |

Invalid records are excluded from the managed Silver output using Lakeflow data quality expectations.

### Lakeflow Job Architecture

```text
Scheduled Trigger
       |
       v
Task 1 - Ingest
(Auto Loader)
       |
       v
Task 2 - Transform
(Managed Lakeflow Pipeline)
       |
       v
Task 3 - Aggregate
(Consolidated Sales Summary)
       |
       v
Task 4 - Data_Quality
(Validation and Reconciliation)
       |
       v
Successful Job Execution
```

### Job Configuration

| Setting | Value |
|---|---|
| Job name | Day_04_Retail_Production_Job |
| Number of tasks | 4 |
| Execution model | Sequential task dependencies |
| Dependency condition | All succeeded |
| Compute | Serverless |
| Trigger type | Scheduled |
| Scheduled test time | 5:42 PM |
| Time zone | Asia/Colombo |
| Job parameter | simulate_failure |
| Default parameter value | false |

### Job Task Configuration

| Task | Type | Depends On |
|---|---|---|
| Ingest | Notebook | None |
| Transform | Lakeflow Pipeline | Ingest |
| Aggregate | Notebook | Transform |
| Data_Quality | Notebook | Aggregate |

### Failure Simulation and Recovery

A controlled failure was introduced in the Data_Quality task using the simulate_failure job parameter.

#### Error Message

```text
RuntimeError: Day 4 intentional failure for recovery testing
```

#### Initial Execution Results

| Task | Status |
|---|---|
| Ingest | Succeeded |
| Transform | Succeeded |
| Aggregate | Succeeded |
| Data_Quality | Failed intentionally |

#### Recovery Procedure
- Inspected the failed job execution graph.
- Reviewed the Data_Quality task error logs.
- Identified the intentional failure as the root cause.
- Changed the failure simulation parameter to false.
- Used the Repair Run functionality.
- Reran only the failed Data_Quality task.
- Preserved the successful upstream task executions.
- Verified that the repaired job completed successfully.

### Scheduled Execution Results

| Metric | Result |
|---|---|
| Execution date | November 26, 2026 |
| Scheduled start time | 5:42 PM |
| Launch method | By scheduler |
| Successful tasks | 4 out of 4 |
| Execution duration | 1 minute 38 seconds |
| Failure simulation | Disabled |
| Final job status | Succeeded |

### Final Data Quality Validation

| Metric | Result |
|---|---:|
| Valid Silver orders | 100,005 |
| Reconciled Gold order count | 100,005 |
| Invalid records in Silver | 0 |
| Duplicate order IDs | 0 |
| Unknown customers | 0 |
| Silver total revenue | 7,528,904,992.00 |
| Gold total revenue | 7,528,904,992.00 |
| Customer sales output records | 500 |
| Daily sales output records | 300 |
| Product sales output records | 900 |
| Order count reconciliation | PASS |
| Revenue reconciliation | PASS |
| Unit count reconciliation | PASS |
| Intentional failure test | PASS |
| Selective task recovery | PASS |
| Automated scheduled execution | PASS |

### Source Files

| File | Purpose |
|---|---|
| Day_04_Retail_Production_Pipeline.py | Managed Bronze, Silver and Gold transformations |
| Day_04_Task_01_Ingest.py | Incremental Auto Loader ingestion |
| Day_04_Task_03_Aggregate.py | Consolidated sales aggregation |
| Day_04_Task_04_Data_Quality.py | Final validation and failure simulation |

### Key Learning Outcomes
- Lakeflow Spark Declarative Pipelines.
- Managed materialized views.
- Declarative data transformations.
- Lakeflow data quality expectations.
- Automatic pipeline dependency management.
- Job orchestration and sequential task execution.
- Parameter-driven job execution.
- Retry policies and error handling.
- Scheduled pipeline automation.
- Failure diagnosis using execution logs.
- Selective task recovery using Repair Run.
- Silver-to-Gold data reconciliation.
- Compute efficiency through avoiding unnecessary task reruns.

### Limitations
- The pipeline reuses existing training datasets rather than live production data.
- Auto Loader ingestion is incremental, but managed materialized views used full recomputation during the observed pipeline update.
- Gold aggregations are grouped primarily by business identifiers.
- Existing Day 3 quarantine data is preserved, while Day 4 expectations exclude invalid records without maintaining a separate historical rejection log.
- Failure recovery was demonstrated using an intentionally simulated exception.
- An automatic retry occurred during the controlled failure test before manual recovery.
- Failure notification configuration was not verified.
- Enterprise governance, access controls and performance optimization are covered in Day 5.

### Status
Core practical assignment completed ✅

## Technologies Used
- Databricks
- Apache Spark
- PySpark
- SQL
- Python
- Delta Lake
- Unity Catalog
- Auto Loader
- Structured Streaming
- Lakeflow Spark Declarative Pipelines
- Lakeflow Jobs
- Materialized Views
- GitHub

## Training Progress

| Day | Topic | Status |
|---|---|---|
| Day 1 | Databricks Fundamentals | Completed ✅ |
| Day 2 | Delta Lake and Medallion Architecture | Completed ✅ |
| Day 3 | Incremental Ingestion and Data Quality | Completed ✅ |
| Day 4 | Production Pipelines and Scheduling | Completed ✅ |
| Day 5 | Governance and Production Basics | Pending |
