# AWS Customer Data Platform using Medallion Architecture

> An end-to-end AWS Data Engineering project that implements a Medallion Architecture (Bronze, Silver, Gold) with AWS Glue, PySpark, Step Functions, EventBridge, Athena, and Slowly Changing Dimension (SCD Type 2) for historical data management.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Business Problem](#business-problem)
- [Solution Overview](#solution-overview)
- [Architecture](#architecture)
- [AWS Services Used](#aws-services-used)
- [Repository Structure](#repository-structure)
- [Data Pipeline Workflow](#data-pipeline-workflow)
- [Bronze Layer](#bronze-layer)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [SCD Type 2 Implementation](#scd-type-2-implementation)
- [Workflow Orchestration](#workflow-orchestration)
- [Monitoring and Notifications](#monitoring-and-notifications)
- [ETL Tracker](#etl-tracker)
- [Data Quality Checks](#data-quality-checks)
- [Project Screenshots](#project-screenshots)
- [Key Skills Demonstrated](#key-skills-demonstrated)
- [Future Enhancements](#future-enhancements)

---

# Project Overview

This project demonstrates the design and implementation of a cloud-native Customer Data Platform (CDP) on AWS using a Medallion Architecture. The solution ingests raw customer data from Amazon S3, transforms and enriches it using AWS Glue and PySpark, and builds a historical customer dimension using Slowly Changing Dimension (SCD Type 2).

The pipeline is fully orchestrated using AWS Step Functions, automatically scheduled with Amazon EventBridge, monitored through CloudWatch Logs, and integrated with Amazon SNS for operational notifications.

The final curated data is stored in Parquet format and made available for analytics through Amazon Athena.

---

# Business Problem

Many organizations receive customer data from multiple operational systems. Raw data often contains:

- Duplicate customer records
- Missing or inconsistent values
- Country codes instead of readable country names
- Customer updates that overwrite historical information

Without a structured data engineering pipeline, reporting becomes unreliable and customer history cannot be accurately tracked.

This project addresses these challenges by implementing a scalable Medallion Architecture with historical data management using SCD Type 2.

---

# Solution Overview

The pipeline follows a three-layer Medallion Architecture:

```text
Landing (CSV Files)
        │
        ▼
Bronze Layer
Raw data converted to Parquet
        │
        ▼
Silver Layer
Cleaned, validated and enriched data
        │
        ▼
Gold Layer
Customer Dimension (SCD Type 2)
        │
        ▼
Amazon Athena
```

The workflow is automatically orchestrated and monitored using AWS managed services.

---

# Architecture

![Architecture](architecture/architecture.png)

---

# AWS Services Used

| Service | Purpose |
|----------|---------|
| Amazon S3 | Stores landing, Bronze, Silver and Gold data |
| AWS Glue ETL | Executes PySpark ETL jobs |
| AWS Glue Crawlers | Creates Data Catalog metadata |
| AWS Glue Data Catalog | Metadata repository for Athena |
| Amazon Athena | SQL analytics on Parquet datasets |
| AWS Step Functions | Orchestrates the ETL workflow |
| Amazon EventBridge | Schedules daily pipeline execution |
| Amazon SNS | Sends success and failure notifications |
| Amazon CloudWatch | Job logging and execution monitoring |

---

# Repository Structure

```text
aws-customer-data-platform-medallion-architecture/
│
├── architecture/
│   └── architecture.png
│
├── documentation/
│
├── glue-jobs/
│   ├── bronze_load_job.py
│   ├── silver_enrichment_job.py
│   └── gold_scd2_job.py
│
├── sample-data/
│   ├── customer_day1.csv
│   ├── customer_day2.csv
│   ├── customer_day3.csv
│   └── country_lookup.csv
│
├── screenshots/
│
└── README.md
```
