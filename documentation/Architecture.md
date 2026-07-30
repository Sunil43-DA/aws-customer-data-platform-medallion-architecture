# Architecture

## Overview

The AWS Customer Data Platform is built using a modern Medallion Architecture, where data progresses through multiple layers to improve quality, reliability, and usability.

The solution leverages AWS managed services to automate data ingestion, transformation, orchestration, monitoring, and analytics.

---

# Architecture Diagram

> Refer to the architecture diagram included in the repository.

![Architecture](../architecture/architecture.png)

---

# Architecture Flow

```text
Raw Customer Files
        │
        ▼
Amazon S3 Landing Layer
        │
        ▼
AWS Glue Bronze ETL
        │
        ▼
Bronze Layer (Parquet)
        │
        ▼
AWS Glue Silver ETL
        │
        ▼
Silver Layer (Cleaned & Enriched)
        │
        ▼
AWS Glue Gold ETL
        │
        ▼
Gold Layer (SCD Type 2)
        │
        ▼
Amazon Athena
```

The complete workflow is orchestrated using AWS Step Functions, scheduled by Amazon EventBridge, monitored through Amazon CloudWatch, and integrated with Amazon SNS for notifications.

---

# AWS Services Used

## Amazon S3

Amazon S3 serves as the project's data lake.

It stores:

- Raw customer files (Landing layer)
- Bronze data
- Silver data
- Gold data

Using Parquet format improves storage efficiency and query performance.

---

## AWS Glue

AWS Glue performs all ETL operations using PySpark.

Three Glue jobs are implemented:

- Bronze ETL
- Silver ETL
- Gold ETL

Each job performs a specific stage of data transformation within the Medallion Architecture.

---

## AWS Glue Crawlers

Glue Crawlers automatically discover datasets stored in Amazon S3 and update the AWS Glue Data Catalog.

This enables Amazon Athena to query the processed datasets without manually defining schemas.

---

## AWS Glue Data Catalog

The Data Catalog stores metadata for all Bronze, Silver, Gold, and ETL Tracker tables.

It acts as a centralized metadata repository used by AWS Glue and Amazon Athena.

---

## Amazon Athena

Amazon Athena enables serverless SQL queries against the curated datasets stored in Amazon S3.

Athena is used to validate the outputs of:

- Bronze Layer
- Silver Layer
- Gold Layer
- ETL Tracker

---

## AWS Step Functions

AWS Step Functions orchestrates the complete ETL workflow.

The workflow executes the Glue jobs sequentially:

1. Bronze ETL
2. Silver ETL
3. Gold ETL

Retry and error handling mechanisms ensure reliable execution.

---

## Amazon EventBridge

Amazon EventBridge schedules the ETL workflow at predefined intervals.

This removes the need for manual execution and enables automated batch processing.

---

## Amazon SNS

Amazon SNS sends notifications based on the outcome of the ETL workflow.

Notifications include:

- Successful pipeline execution
- Pipeline failure alerts

This provides immediate operational visibility.

---

## Amazon CloudWatch

CloudWatch collects execution logs from:

- AWS Glue Jobs
- AWS Step Functions

These logs support monitoring, troubleshooting, and operational analysis.

---

# Medallion Architecture

The project follows a three-layer Medallion Architecture.

## Bronze Layer

The Bronze layer stores raw customer data with minimal transformation.

Primary objective:

- Preserve source data

---

## Silver Layer

The Silver layer improves data quality through cleansing, validation, standardization, and enrichment.

Primary objective:

- Create reliable datasets for downstream processing

---

## Gold Layer

The Gold layer builds the analytical Customer Dimension using Slowly Changing Dimension (SCD Type 2).

Primary objective:

- Deliver historical customer data for reporting and analytics

---

# Benefits of the Architecture

The implemented architecture provides several advantages:

- Scalable cloud-native solution
- Automated ETL processing
- High-quality curated datasets
- Historical data preservation
- Incremental processing
- Simplified analytics with Amazon Athena
- Centralized monitoring and notifications
- Production-ready workflow orchestration

---

# Summary

This architecture demonstrates a complete, production-style AWS data engineering solution that integrates storage, processing, orchestration, monitoring, and analytics using managed AWS services. The modular Medallion Architecture improves maintainability, scalability, and data quality while preserving historical customer information for analytical workloads.
