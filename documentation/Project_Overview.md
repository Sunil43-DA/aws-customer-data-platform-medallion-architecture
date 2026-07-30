# Project Overview

## Introduction

The AWS Customer Data Platform is a production-style data engineering project designed to demonstrate the implementation of a modern ETL pipeline using AWS cloud services and the Medallion Architecture.

The project ingests raw customer data, transforms and enriches it using AWS Glue and PySpark, and stores curated data for analytics while maintaining historical customer records using Slowly Changing Dimension (SCD Type 2).

---

# Business Problem

Organizations receive customer data from multiple operational systems. Raw data often contains duplicate records, inconsistent formatting, missing values, and outdated customer information.

Without a structured data engineering pipeline, maintaining accurate and historical customer records becomes difficult, leading to unreliable reporting and analytics.

---

# Solution

This project implements an automated cloud-based ETL pipeline that:

- Ingests raw customer data from Amazon S3.
- Stores raw data in the Bronze layer.
- Cleanses and enriches customer data in the Silver layer.
- Builds a historical customer dimension in the Gold layer using SCD Type 2.
- Tracks ETL execution metadata.
- Automates workflow orchestration using AWS Step Functions.
- Schedules execution using Amazon EventBridge.
- Sends execution notifications using Amazon SNS.

---

# Objectives

The primary objectives of this project are:

- Build a scalable ETL pipeline using AWS services.
- Demonstrate Medallion Architecture implementation.
- Preserve customer history using SCD Type 2.
- Enable incremental data processing.
- Improve data quality through validation and cleansing.
- Automate pipeline execution and monitoring.
- Provide curated datasets for analytical reporting.

---

# Key Features

- Medallion Architecture (Bronze, Silver, Gold)
- AWS Glue ETL using PySpark
- Amazon S3 Data Lake
- AWS Glue Crawlers and Data Catalog
- Amazon Athena for querying
- SCD Type 2 historical tracking
- Incremental processing
- SHA-256 hash-based change detection
- AWS Step Functions orchestration
- Amazon EventBridge scheduling
- Amazon SNS notifications
- Amazon CloudWatch logging
- ETL Tracker for execution metadata

---

# Technologies Used

| Category | Technologies |
|----------|--------------|
| Cloud | AWS |
| Storage | Amazon S3 |
| ETL | AWS Glue |
| Processing | PySpark |
| Query Engine | Amazon Athena |
| Workflow | AWS Step Functions |
| Scheduling | Amazon EventBridge |
| Monitoring | Amazon CloudWatch |
| Notifications | Amazon SNS |
| Language | Python |
| Format | Parquet |
| Data Catalog | AWS Glue Data Catalog |

---

# Expected Outcome

The completed solution delivers a reliable, automated, and scalable data engineering pipeline that transforms raw customer data into high-quality analytical datasets while preserving historical changes. The project demonstrates practical implementation of modern data engineering concepts commonly used in enterprise environments.
