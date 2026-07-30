# Testing Results

## Overview

Comprehensive testing was performed to validate the reliability, accuracy, and automation of the AWS Customer Data Platform.

The testing process verified:

- Data ingestion
- Data transformations
- SCD Type 2 implementation
- Incremental processing
- Workflow orchestration
- Pipeline monitoring
- Success notifications
- Failure handling
- Data quality validation

---

# Test Environment

The project was tested using the following AWS services:

- Amazon S3
- AWS Glue
- AWS Glue Crawlers
- AWS Glue Data Catalog
- Amazon Athena
- AWS Step Functions
- Amazon EventBridge
- Amazon SNS
- Amazon CloudWatch

---

# Test Scenarios

## 1. Bronze ETL Validation

### Objective

Verify that raw CSV files are successfully ingested into the Bronze layer.

### Expected Result

- Customer data loaded into Amazon S3 Bronze layer.
- Data converted from CSV to Parquet.
- `load_date` populated.

### Result

✅ Passed

---

## 2. Silver ETL Validation

### Objective

Verify that customer data is cleaned, standardized, deduplicated, and enriched.

### Validation

- Duplicate records removed.
- Customer information standardized.
- Country lookup successfully joined.
- Processing timestamp generated.

### Result

✅ Passed

---

## 3. Gold ETL Validation

### Objective

Verify Slowly Changing Dimension (SCD Type 2) implementation.

### Validation

- New customers inserted.
- Updated customers create new historical versions.
- Historical records retained.
- Current record correctly identified.
- Surrogate keys generated correctly.

### Result

✅ Passed

---

## 4. Incremental Processing

### Objective

Ensure only new and modified customer records are processed.

### Validation

- Existing unchanged customers skipped.
- New customers inserted.
- Modified customers updated using SCD Type 2.

### Result

✅ Passed

---

# Amazon Athena Validation

The processed datasets were validated using Amazon Athena.

The following tables were queried:

- Bronze Customer Table
- Silver Customer Table
- Gold Customer Dimension
- ETL Tracker

### Validation

- Data loaded successfully.
- Record counts verified.
- Customer history validated.
- Current records confirmed.

### Result

✅ Passed

---

# ETL Tracker Validation

The ETL Tracker was validated after each successful pipeline execution.

Verified fields:

- Table Name
- Processed Rows
- Latest Surrogate Key
- Latest Source Update Timestamp
- ETL Execution Timestamp

### Result

✅ Passed

---

# Step Functions Validation

The complete workflow was executed using AWS Step Functions.

Workflow sequence:

1. Bronze ETL
2. Silver ETL
3. Gold ETL

### Validation

- Sequential execution confirmed.
- Retry mechanism available.
- Successful completion verified.

### Result

✅ Passed

---

# Amazon SNS Notifications

## Success Notification

After successful completion of the ETL workflow, an Amazon SNS success notification was received.

### Result

✅ Passed

---

## Failure Notification

Failure handling was tested by intentionally introducing an invalid Amazon S3 input path.

Expected behaviour:

- Glue Job failed.
- Step Functions entered the failure path.
- Amazon SNS failure notification sent.

Observed behaviour matched expectations.

### Result

✅ Passed

---

# CloudWatch Logs

CloudWatch logs were reviewed to verify:

- Glue Job execution
- Error logging
- Pipeline execution history
- Runtime diagnostics

### Result

✅ Passed

---

# Data Quality Validation

The following validation rules were confirmed:

- Customer ID must not be NULL.
- Duplicate surrogate keys are not allowed.
- Only one active customer record exists.
- Processed row counts validated.
- Customer history preserved.

### Result

✅ Passed

---

# Summary of Test Results

| Test | Status |
|------|--------|
| Bronze ETL | ✅ Passed |
| Silver ETL | ✅ Passed |
| Gold ETL | ✅ Passed |
| Incremental Processing | ✅ Passed |
| Athena Validation | ✅ Passed |
| ETL Tracker | ✅ Passed |
| Step Functions | ✅ Passed |
| SNS Success Notification | ✅ Passed |
| SNS Failure Notification | ✅ Passed |
| CloudWatch Logging | ✅ Passed |
| Data Quality Validation | ✅ Passed |

---

# Conclusion

The AWS Customer Data Platform was successfully validated through functional, operational, and data quality testing. The pipeline reliably ingests, transforms, enriches, and maintains customer data while preserving historical changes using SCD Type 2.

Workflow orchestration, automated scheduling, monitoring, notifications, and analytical querying were all verified, demonstrating a production-style implementation of a cloud-based data engineering solution.
