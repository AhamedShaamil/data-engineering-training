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
- Automated incremental ingestion is planned for Day 3.

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
- GitHub

## Training Progress

| Day | Topic | Status |
|---|---|---|
| Day 1 | Databricks Fundamentals | Completed ✅ |
| Day 2 | Delta Lake and Medallion Architecture | Completed ✅ |
| Day 3 | Incremental Ingestion and Data Quality | Pending |
| Day 4 | Production Pipelines and Scheduling | Pending |
| Day 5 | Governance and Production Basics | Pending |
