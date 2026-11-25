# Databricks – Data Engineering Training

## Overview
This repository documents my hands-on data engineering internship training using Databricks, SQL, PySpark and Delta Lake.

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
- Managed pipeline orchestration and scheduling are planned for Day 4.

### Status
Completed ✅

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
- GitHub

## Training Progress

| Day | Topic | Status |
|---|---|---|
| Day 1 | Databricks Fundamentals | Completed ✅ |
| Day 2 | Delta Lake and Medallion Architecture | Completed ✅ |
| Day 3 | Incremental Ingestion and Data Quality | Completed ✅ |
| Day 4 | Production Pipelines and Scheduling | Pending |
| Day 5 | Governance and Production Basics | Pending |
