# SCD Type 2 Implementation

## Overview

The Gold layer of the AWS Customer Data Platform implements **Slowly Changing Dimension (SCD Type 2)** to preserve the complete history of customer information.

Unlike a standard update operation that overwrites existing data, SCD Type 2 creates a new version of a record whenever tracked customer attributes change. This approach enables historical reporting and provides a complete audit trail of customer changes over time.

---

# Why SCD Type 2?

Customer information changes over time. Examples include:

- Address changes
- Phone number updates
- Email changes
- Country or region changes

If these updates overwrite existing records, historical information is lost.

SCD Type 2 preserves every version of a customer record while ensuring that only one version is marked as the current record.

---

# Gold Table Structure

The Gold Customer Dimension includes both business attributes and historical tracking fields.

| Column | Description |
|---------|-------------|
| customer_sk | Surrogate key |
| cust_id | Business key |
| first_name | Customer first name |
| last_name | Customer last name |
| email | Email address |
| phone | Contact number |
| address | Customer address |
| city | City |
| state | State |
| postal_code | Postal code |
| country | Country |
| region | Region |
| hash_value | SHA-256 hash of tracked attributes |
| current_flag | Indicates active record (`Y` or `N`) |
| valid_from | Record start timestamp |
| valid_to | Record end timestamp |

---

# Surrogate Key

Each customer version receives a unique surrogate key (`customer_sk`).

Unlike the business key (`cust_id`), the surrogate key changes whenever a new version of a customer record is created.

Example:

| customer_sk | cust_id |
|-------------|---------|
| 1 | C1001 |
| 2 | C1001 |

Although both rows represent the same customer, they correspond to different historical versions.

---

# Business Key

The business key uniquely identifies the customer in the source system.

In this project:

```text
cust_id
```

remains constant across all historical versions of the same customer.

---

# Current Flag

The `current_flag` identifies the active version of each customer.

| Value | Meaning |
|-------|---------|
| Y | Current record |
| N | Historical record |

Only one record per customer should have `current_flag = 'Y'`.

---

# Valid From and Valid To

These timestamps define the period during which a customer record is valid.

Example:

| cust_id | current_flag | valid_from | valid_to |
|---------|--------------|------------|----------|
| C1001 | N | 2026-07-01 | 2026-07-15 |
| C1001 | Y | 2026-07-15 | NULL |

The first row represents the historical version, while the second row is the active version.

---

# Hash-Based Change Detection

To efficiently identify updates, the pipeline generates a SHA-256 hash using selected customer attributes.

Tracked attributes include:

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

Instead of comparing every column individually, the pipeline compares the incoming hash with the existing Gold record.

If the hashes differ, the customer record has changed.

---

# Processing Logic

For each incoming customer record, the pipeline performs the following steps:

1. Search for an existing customer using `cust_id`.
2. If no record exists:
   - Insert a new customer.
3. If a record exists:
   - Compare the hash values.
4. If the hash is unchanged:
   - No action is taken.
5. If the hash has changed:
   - Update the existing record:
     - Set `current_flag = 'N'`
     - Populate `valid_to`
   - Insert a new record:
     - Generate a new surrogate key
     - Set `current_flag = 'Y'`
     - Set `valid_from`
     - Leave `valid_to` as `NULL`

---

# Incremental Processing

The Gold ETL processes only:

- New customers
- Updated customers

Unchanged records are skipped.

This incremental approach:

- Reduces processing time
- Improves scalability
- Minimizes unnecessary writes
- Supports efficient production workloads

---

# ETL Tracker Integration

After each successful load, the ETL Tracker is updated with:

- Table name
- Number of processed rows
- Latest surrogate key
- Latest source update timestamp
- ETL execution timestamp

This information supports incremental processing in future executions.

---

# Benefits of SCD Type 2

Implementing SCD Type 2 provides several advantages:

- Preserves complete customer history
- Enables historical reporting
- Supports auditing and compliance
- Prevents accidental data loss
- Maintains a single active customer record
- Improves analytical accuracy

---

# Summary

The Gold layer uses Slowly Changing Dimension (SCD Type 2) to maintain historical customer information while supporting efficient incremental processing. By combining surrogate keys, hash-based change detection, validity timestamps, and current record indicators, the solution provides a scalable and production-ready approach to managing changing customer data.
