# Databricks – Data Engineering Training

## Overview
This repository documents my hands-on data engineering internship training using Databricks, SQL, PySpark, Delta Lake, Unity Catalog and Lakeflow.

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
- Auto Loader ingestion is incremental, but managed materialized views used full recomputation during the initially observed pipeline update. The later scheduled update reviewed in Day 5 showed NO_OP planning.
- Gold aggregations are grouped primarily by business identifiers.
- Existing Day 3 quarantine data is preserved, while Day 4 expectations exclude invalid records without maintaining a separate historical rejection log.
- Failure recovery was demonstrated using an intentionally simulated exception.
- An automatic retry occurred during the controlled failure test before manual recovery.
- Failure notification configuration was not verified.
- Governance, access controls and performance optimization are addressed in Day 5.

### Status
Core practical assignment completed ✅

## Day 5 – Governance and Production Basics

### Objective

Apply Unity Catalog governance, least-privilege access control, data lineage, environment separation, query optimization, pipeline monitoring and cost awareness to the existing Databricks training pipeline.

### Completed Tasks
- Inspected available Unity Catalog catalogs, schemas and tables.
- Identified the current user and reviewed existing access privileges.
- Inspected available Bronze, Silver and Gold data layers.
- Created an account-level group named gold_readers.
- Configured Gold read-only access using Unity Catalog privileges.
- Granted USE CATALOG, USE SCHEMA and SELECT permissions.
- Inspected catalog-level, schema-level and table-level privileges.
- Validated the Gold read-only permission configuration.
- Inspected Silver permissions to check for modification privileges.
- Traced Gold daily sales data back to Bronze using Unity Catalog lineage.
- Identified customer and product reference table dependencies.
- Created separate development and production demonstration schemas.
- Practiced column pruning and early filtering.
- Analyzed physical execution plans using EXPLAIN FORMATTED.
- Corrected the quantity filter from greater than 5 to greater than 3 based on the available dataset.
- Identified scan-level filtering and Photon execution support.
- Explored adaptive query execution and shuffle concepts.
- Reviewed previous manual and scheduled Lakeflow Job executions.
- Analyzed individual task durations and query metrics.
- Identified the Transform task as the longest-running task.
- Reviewed task execution events and detailed pipeline event logs.
- Identified NO_OP refresh planning as a mechanism for avoiding unnecessary recomputation.
- Reviewed production compute efficiency and cost-control practices.

### Unity Catalog Structure

```text
Unity Catalog
       |
       +-- samples
       |
       +-- system
       |
       +-- workspace
              |
              +-- bronze
              +-- silver
              +-- gold
              +-- default
              +-- dev
              +-- prod
              +-- information_schema
```

### Account Group Configuration

| Setting | Value |
|---|---|
| Group name | gold_readers |
| Group source | Account |
| Group members during exercise | 0 |
| Purpose | Read-only access to Gold reporting tables |

### Unity Catalog Privileges

| Principal | Object | Privilege |
|---|---|---|
| gold_readers | workspace | USE CATALOG |
| gold_readers | workspace.gold | USE SCHEMA |
| gold_readers | workspace.gold | SELECT |

### Permission Configuration

```sql
GRANT USE CATALOG ON CATALOG workspace
TO `gold_readers`;

GRANT USE SCHEMA ON SCHEMA workspace.gold
TO `gold_readers`;

GRANT SELECT ON SCHEMA workspace.gold
TO `gold_readers`;
```

The schema-level SELECT privilege provides read access to current and future eligible tables within the Gold schema.

No MODIFY privilege was granted to the group.

### Intended Access Control Model

| Operation | Data Engineer | Gold Readers |
|---|---|---|
| Read Bronze data | Allowed | Not granted |
| Modify Bronze data | Allowed | Not granted |
| Read Silver data | Allowed | Not granted |
| Modify Silver data | Allowed | Not granted |
| Read Gold data | Allowed | Allowed |
| Modify Gold data | Allowed | Not granted |

This configuration demonstrates the principle of least privilege by granting only the permissions necessary for business reporting.

The intended access model assumes that analysts do not receive additional privileges through other groups or ownership.

### Permission Validation Results

| Validation | Result |
|---|---|
| Account-level group creation | PASS |
| Catalog USE CATALOG privilege | PASS |
| Gold USE SCHEMA privilege | PASS |
| Gold SELECT privilege | PASS |
| Gold table privilege inspection | Completed |
| Silver schema privilege inspection | No grants returned |
| Silver orders_clean privilege inspection | No grants returned |
| Separate non-admin access-denial test | Not tested |

The group contained no members during the training exercise. Therefore, the validation confirms the configured privileges rather than actual access enforcement for a separate analyst account.

### Verified Data Lineage

```text
Bronze Orders
      |
      v
Silver Orders Clean
      |
      +-------------------------+
                                |
Products Reference ------------>|
                                v
Customers Reference ----------> Orders Enriched
                                (Silver View)
                                      |
                                      v
                                Daily Sales
                                (Gold Table)
                                      |
                                      v
                              Downstream Consumers
```

### Verified Lineage Objects

| Layer | Object | Responsibility |
|---|---|---|
| Bronze | workspace.bronze.orders | Raw order records |
| Silver | workspace.silver.orders_clean | Validated order records |
| Silver | workspace.silver.products_reference | Product reference information |
| Silver | workspace.silver.customers_reference | Customer reference information |
| Silver | workspace.silver.orders_enriched | Combined order and reference information |
| Gold | workspace.gold.daily_sales | Aggregated business sales metrics |

Unity Catalog lineage confirmed the relationships between the Bronze source table, Silver transformations and Gold business output.

The original CSV source file was not visible in the inspected lineage graph.

### Development and Production Separation

Created separate schemas to demonstrate environment organization.

```sql
CREATE SCHEMA IF NOT EXISTS workspace.dev;

CREATE SCHEMA IF NOT EXISTS workspace.prod;

SHOW SCHEMAS IN workspace;
```

| Schema | Purpose |
|---|---|
| workspace.dev | Development and testing demonstration |
| workspace.prod | Production organization demonstration |

Existing Bronze, Silver and Gold datasets were preserved.

This exercise demonstrates schema-level organization rather than fully isolated enterprise development and production environments.

### Query Optimization

Practiced three fundamental query optimization techniques:

- Selecting only required columns instead of SELECT *.
- Applying filters to reduce unnecessary processing.
- Reviewing physical execution plans to understand query execution.

#### Optimized Query Execution Plan

```sql
EXPLAIN FORMATTED
SELECT
    store_id,
    SUM(quantity * unit_price) AS total_revenue
FROM workspace.silver.orders_clean
WHERE quantity > 3
GROUP BY store_id;
```

### Execution Plan Results

| Observation | Result |
|---|---|
| Available source columns | 8 |
| Required scan columns | 3 |
| Required columns | store_id, quantity, unit_price |
| Scan-level filter | quantity > 3 |
| Scan operation | PhotonScan |
| Aggregation operation | PhotonGroupingAgg |
| Query engine | Photon |
| Photon support | Full |
| Adaptive execution | AdaptiveSparkPlan |
| Optimizer statistics | Full |
| Execution plan state | Initial plan |

The execution plan demonstrated column pruning and scan-level filtering.

Only the three columns required for the aggregation were included in the scan schema.

No explicit shuffle operation was visible in the initial execution plan. Runtime shuffle behavior and measured performance improvements were not established.

### Query Debugging

The initial filtering condition was:

```sql
WHERE quantity > 5
```

The execution plan returned an empty LocalTableScan because the available order quantities did not exceed 5.

The condition was changed to:

```sql
WHERE quantity > 3
```

The corrected execution plan successfully displayed scan, filtering and aggregation operations with full Photon support.

### Lakeflow Job Monitoring

Reviewed the previously executed Day 4 retail production job.

| Setting | Value |
|---|---|
| Job name | Day_04_Retail_Production_Job |
| Execution date | November 26, 2026 |
| Compute for displayed notebook tasks | Serverless |
| Performance optimization | Enabled |
| Successful runs reviewed | 2 |
| Schedule status during review | Paused |

### Job Execution Results

| Execution Type | Duration | Status |
|---|---|---|
| Manual | 2 minutes 39 seconds | Succeeded |
| Scheduled | 1 minute 38 seconds | Succeeded |

The runs used different failure simulation parameter values. Therefore, the duration difference does not establish a performance improvement caused by scheduling.

### Scheduled Job Task Performance

| Task | Duration | Status |
|---|---|---|
| Ingest | 12 seconds | Succeeded |
| Transform | 1 minute 1 second | Succeeded |
| Aggregate | 12 seconds | Succeeded |
| Data_Quality | 8 seconds | Succeeded |

The Transform task was identified as the longest-running task and a candidate for further performance investigation.

### Job Performance Metrics

| Metric | Result |
|---|---|
| Successful tasks | 4 out of 4 |
| Total duration in run details | 1 minute 37 seconds |
| Number of queries | 14 |
| Rows read | 301,116 |
| Rows written in reported query metrics | 1 |
| Performance optimization | Enabled |
| Final job status | Succeeded |

Rows read represent cumulative query activity rather than the number of unique source records.

The reported rows-written metric does not necessarily represent the total output across the entire managed pipeline.

### Pipeline Run Events

Reviewed the following execution lifecycle events:

| Event | Description |
|---|---|
| started | Execution initiated |
| waitingForCompute | Waiting for compute resources |
| computeReady | Compute resources available |
| startedRunning | Processing started |
| succeeded | Execution completed successfully |

### Lakeflow Pipeline Event Log Review

| Pipeline Flow | Refresh Planning | Final Status |
|---|---|---|
| day4_bronze_orders | NO_OP | COMPLETED |
| day4_valid_orders | NO_OP | COMPLETED |
| day4_silver_orders | NO_OP | COMPLETED |
| day4_gold_daily_sales | NO_OP | COMPLETED |
| day4_gold_product_sales | NO_OP | COMPLETED |
| day4_gold_customer_sales | NO_OP | COMPLETED |

All six managed pipeline flows completed successfully.

NO_OP planning indicates that unnecessary recomputation was avoided because the affected flows did not require data updates.

However, NO_OP does not imply that the entire job consumed zero compute resources.

### Performance and Cost Awareness

Reviewed the following production optimization practices:

- Process only new or changed data using incremental ingestion.
- Select only required columns.
- Apply relevant filters early.
- Avoid unnecessary full-table processing.
- Reduce unnecessary data movement and shuffle operations.
- Investigate long-running pipeline tasks.
- Use appropriate compute resources.
- Avoid unnecessary pipeline and job reruns.
- Repair failed tasks instead of restarting successful upstream tasks.
- Pause unused job schedules.
- Monitor Lakeflow refresh planning and NO_OP execution.

### Governance and Production Checklist

- [x] Unity Catalog inspection
- [x] Existing user and privilege inspection
- [x] Account-level group creation
- [x] Gold read-only permission configuration
- [x] Silver privilege inspection
- [x] Bronze-to-Gold data lineage inspection
- [x] Development and production schema creation
- [x] Column pruning demonstration
- [x] Early filtering demonstration
- [x] Physical execution plan analysis
- [x] Photon execution support verification
- [x] Lakeflow Job monitoring
- [x] Individual task performance review
- [x] Pipeline event log inspection
- [x] Cost-awareness review
- [ ] Separate non-admin access-denial test
- [ ] Week 1 mentor presentation

### Source Files

| File | Purpose |
|---|---|
| Day_05_Governance_and_Production_Basics.py | Unity Catalog inspection, permission configuration, schema creation, validation queries and query optimization |

Supporting screenshots are organized in the Day 5 evidence folder.

### Key Learning Outcomes

- Unity Catalog governance and access management.
- Catalog, schema and table hierarchy.
- Account-level groups and data privileges.
- Principle of least privilege.
- Schema-level privilege inheritance.
- Upstream and downstream data lineage.
- Development and production environment separation.
- Column pruning and filter pushdown.
- Physical query execution plans.
- Photon query execution.
- Adaptive query execution.
- Shuffle operations and data movement.
- Lakeflow job history and performance metrics.
- Pipeline execution events and event logs.
- NO_OP refresh planning.
- Compute efficiency and production cost awareness.

### Limitations

- Actual access-denial testing requires a separate non-admin analyst account.
- The permission exercise targets workspace.gold rather than the separate Day 4 managed outputs in workspace.default.
- Development and production separation was demonstrated at schema level only.
- Actual compute consumption and billing charges were not measured in the Databricks Free Edition environment.
- Query optimization was verified through execution plans rather than controlled runtime benchmarks.

### Status

Practical exercises completed ✅

## Week 1 – Databricks Training Summary

The first week of training covered the development of a retail data engineering pipeline from source ingestion to production-style orchestration, governance and operational monitoring.

### End-to-End Architecture

```text
Source CSV Files
       |
       v
Databricks Auto Loader
       |
       v
Bronze Delta Tables
       |
       v
Data Quality Validation
       |
       +-------------------+
       |                   |
       v                   v
Silver Delta Tables    Quarantine Table
       |
       v
Gold Business Aggregations
       |
       v
Business Reporting
```

Cross-cutting platform capabilities:

```text
Lakeflow Spark Declarative Pipelines
    - Managed data transformations
    - Data quality expectations
    - Refresh planning

Lakeflow Jobs
    - Task orchestration
    - Scheduled execution
    - Failure recovery

Unity Catalog
    - Access control
    - Data governance
    - Data lineage

Production Monitoring
    - Job history
    - Performance metrics
    - Pipeline event logs
    - Cost awareness
```

### Week 1 Achievements

| Capability | Status |
|---|---|
| SQL and PySpark transformations | Completed |
| Delta Lake operations | Completed |
| Medallion architecture | Completed |
| Incremental file ingestion | Completed |
| Schema evolution | Completed |
| Data quality validation | Completed |
| Quarantine handling | Completed |
| Idempotency verification | Completed |
| Managed Lakeflow pipelines | Completed |
| Job orchestration | Completed |
| Scheduled execution | Completed |
| Failure simulation and recovery | Completed |
| Unity Catalog governance | Configured |
| Data lineage | Verified |
| Query optimization | Demonstrated |
| Production monitoring | Completed |

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
- Photon
- GitHub

## Training Progress

| Day | Topic | Status |
|---|---|---|
| Day 1 | Databricks Fundamentals | Completed ✅ |
| Day 2 | Delta Lake and Medallion Architecture | Completed ✅ |
| Day 3 | Incremental Ingestion and Data Quality | Completed ✅ |
| Day 4 | Production Pipelines and Scheduling | Completed ✅ |
| Day 5 | Governance and Production Basics | Practical exercises completed ✅ |

**Week 1 – Databricks:** Practical training completed. Mentor presentation and separate analyst access-denial testing remain pending.

**Next Stage:** Week 2 – Snowflake Data Engineering Training.
