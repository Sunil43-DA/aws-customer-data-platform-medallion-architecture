# ETL Workflow

## Overview

The ETL workflow is the core component of the AWS Customer Data Platform. It extracts raw customer data from Amazon S3, transforms it through multiple processing stages, and loads curated datasets into the Gold layer for analytics.

The project follows the Medallion Architecture, where data flows through three layers:

- Bronze Layer
- Silver Layer
- Gold Layer

Each layer has a specific responsibility that improves data quality while preserving data lineage.

---

# ETL Pipeline Flow

```text
Landing (CSV Files)
        │
        ▼
Bronze ETL
        │
        ▼
Bronze Layer (Raw Parquet)
        │
        ▼
Silver ETL
        │
        ▼
Silver Layer (Cleaned & Enriched)
        │
        ▼
Gold ETL
        │
        ▼
Gold Customer Dimension (SCD Type 2)
```

---

# Bronze ETL

## Purpose

The Bronze ETL job ingests raw customer and country lookup data from the Landing layer and stores it in Parquet format.

Minimal transformations are performed to preserve the original source data.

---

## Source

- Customer CSV files
- Country Lookup CSV

---

## Transformations

The Bronze ETL performs the following tasks:

- Reads CSV files from Amazon S3.
- Infers the schema.
- Converts data into Parquet format.
- Adds a `load_date` timestamp.
- Writes the data to the Bronze layer.

---

## Output

- Bronze Customer Table
- Bronze Country Lookup Table

---

# Silver ETL

## Purpose

The Silver ETL job improves data quality by cleansing, validating, deduplicating, and enriching customer records.

This layer prepares reliable datasets for downstream processing.

---

## Transformations

The Silver ETL performs the following operations:

### Data Cleansing

- Removes unnecessary whitespace.
- Standardizes column values.
- Handles inconsistent formatting.
- Converts timestamps into a consistent format.

### Data Validation

- Removes invalid records.
- Validates mandatory fields.
- Filters incomplete customer records.

### Deduplication

Duplicate customer records are identified using Window Functions.

The latest customer record is retained based on the source update timestamp.

### Data Enrichment

Customer data is joined with the Country Lookup dataset to add descriptive country and region information.

### Metadata

A processing timestamp is added for auditing purposes.

---

## Output

- Customer Enriched Table

---

# Gold ETL

## Purpose

The Gold ETL builds the analytical Customer Dimension using Slowly Changing Dimension (SCD Type 2).

Instead of overwriting customer information, the Gold layer preserves historical versions of customer records.

---

## Processing Logic

Each incoming customer record is compared with the existing Gold dataset.

The pipeline classifies records into:

- New customers
- Changed customers
- Unchanged customers

Only new and changed records are processed.

---

## SCD Type 2 Processing

When changes are detected:

1. The existing customer record is marked as historical.
2. The `valid_to` timestamp is updated.
3. A new record is inserted with:
   - New surrogate key
   - Current flag set to `Y`
   - New `valid_from` timestamp
4. Historical records remain available for reporting.

---

## Hash-Based Change Detection

To efficiently identify changes, a SHA-256 hash is generated using tracked customer attributes.

Compared attributes include:

- First Name
- Last Name
- Email
- Phone
- Address
- City
- State
- Postal Code
- Country
- Region

If the incoming hash differs from the stored hash, the record is treated as an update.

---

## Incremental Processing

The ETL pipeline supports incremental processing by identifying only new and modified records.

This approach:

- Reduces processing time.
- Improves scalability.
- Minimizes unnecessary data movement.

---

# ETL Tracker

After successful processing, the Gold ETL updates the ETL Tracker.

The tracker records:

- Table name
- Number of processed rows
- Latest surrogate key
- Latest source update timestamp
- ETL execution timestamp

This metadata supports auditing and future incremental loads.

---

# Data Quality Checks

Before writing to the Gold layer, several validation checks are performed:

- Customer ID must not be NULL.
- Duplicate surrogate keys are not allowed.
- Only one active (`current_flag = 'Y'`) record is permitted per customer.
- Processed row counts are validated.

If validation fails, the ETL process stops and an error is logged.

---

# Workflow Orchestration

The ETL jobs are executed sequentially using AWS Step Functions:

1. Bronze ETL
2. Silver ETL
3. Gold ETL

The workflow includes retry logic and failure handling to improve reliability.

---

# Summary

The ETL workflow transforms raw customer data into high-quality analytical datasets using a structured Medallion Architecture. By combining AWS Glue, PySpark, and SCD Type 2 processing, the solution ensures data quality, preserves historical records, and supports scalable, production-style data engineering workflows.
