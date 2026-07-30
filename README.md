# AWS Customer Data Platform using Medallion Architecture

## Project Overview

This project demonstrates an end-to-end cloud-based Customer Data Platform (CDP) built on AWS using a Medallion Architecture (Bronze, Silver, and Gold layers). The solution automates customer data ingestion, cleansing, enrichment, historical tracking using Slowly Changing Dimension (SCD Type 2), orchestration, scheduling, monitoring, and reporting.

The project was developed to demonstrate modern data engineering practices using AWS services and PySpark.

---

## Architecture

![Architecture](architecture/architecture.png)

---

## AWS Services Used

- Amazon S3
- AWS Glue ETL
- AWS Glue Data Catalog
- AWS Glue Crawlers
- Amazon Athena
- AWS Step Functions
- Amazon EventBridge
- Amazon SNS
- Amazon CloudWatch

---

## Solution Architecture

The pipeline follows the Medallion Architecture:

```
Landing
   │
   ▼
Bronze Layer
   │
   ▼
Silver Layer
   │
   ▼
Gold Layer (SCD Type 2)
   │
   ▼
Athena Reporting
```

---

## Repository Structure

```
aws-customer-data-platform-medallion-architecture/
│
├── architecture/
├── documentation/
├── glue-jobs/
├── sample-data/
├── screenshots/
└── README.md
```

---

## Data Pipeline

### Bronze Layer

- Reads raw CSV files from Amazon S3
- Converts CSV to Parquet
- Adds ingestion timestamp
- Stores raw data in Bronze layer

### Silver Layer

- Cleans customer records
- Standardizes values
- Removes duplicate customers
- Enriches customer records using country lookup
- Stores enriched data in Parquet

### Gold Layer

Implements Slowly Changing Dimension (SCD Type 2):

- Detects new customers
- Detects changed customer attributes
- Generates surrogate keys
- Preserves historical records
- Maintains current and historical versions
- Updates ETL Tracker

---

## Workflow Orchestration

AWS Step Functions orchestrates the pipeline:

1. Bronze ETL
2. Silver ETL
3. Gold ETL
4. SNS Success Notification

The workflow includes Retry and Catch mechanisms for failure handling.

---

## Scheduling

Amazon EventBridge automatically triggers the Step Functions workflow on a predefined schedule.

---

## Monitoring

The project uses:

- CloudWatch Logs
- Step Functions Execution History
- SNS Email Notifications

for operational monitoring.

---

## Data Quality Checks

The Gold layer validates:

- Null Customer IDs
- Duplicate Surrogate Keys
- Multiple Current Records
- Processed Row Counts

The pipeline stops if validation fails.

---

## ETL Tracker

The ETL Tracker maintains:

- Table Name
- Processed Rows
- Last Surrogate Key
- Last Source Update Timestamp
- Load Timestamp

---

## Project Screenshots

### Architecture

![Architecture](screenshots/architecture.png)

### Bronze Layer

![Bronze](screenshots/bronze-data.png)

### Silver Layer

![Silver](screenshots/silver-data.png)

### Gold Layer

![Gold](screenshots/gold-data.png)

### ETL Tracker

![Tracker](screenshots/etl-tracker.png)

### Glue Jobs

![Glue Jobs](screenshots/glue-jobs.png)

### Step Functions

![Step Functions Success](screenshots/step-functions-success.png)

### EventBridge

![EventBridge](screenshots/eventbridge-schedule.png)

---

## Key Features

- Medallion Architecture
- Incremental Data Processing
- SCD Type 2
- Hash-Based Change Detection
- Surrogate Key Generation
- ETL Metadata Tracking
- Step Functions Orchestration
- EventBridge Scheduling
- SNS Notifications
- Data Quality Validation
- Athena Analytics

---

## Future Enhancements

- AWS Lambda Integration
- Infrastructure as Code using Terraform
- CI/CD with GitHub Actions
- Great Expectations Data Validation
- Amazon QuickSight Dashboards
- AWS Lake Formation Integration

---

## Author

Sunil Reddy

Data Engineer | AWS | PySpark | SQL | ETL | Data Warehousing
