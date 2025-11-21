# ddl_comparator.py

## Overview

### What is DDLComparator?

The **DDLComparator** is a powerful Python class designed to compare database table definitions (DDLs - Data Definition
Language) across different database systems and sources. It automates the complex task of verifying that table schemas
are consistent between multiple database targets.

### Key Features

- **Multi-Source Comparison**: Compare tables from Git (SQL files in ZIP archives) against live database targets
- **Multiple Target Support**: Supports Redshift and Iceberg data warehouses
- **Detailed Reporting**: Generates comprehensive Excel reports with differences at column and constraint level
- **Intelligent Parsing**: Automatically parses CREATE TABLE statements and extracts column/constraint information
- **Data Type Mapping**: Handles data type conversions and normalizations across different database systems
- **Constraint Validation**: Verifies primary keys, unique constraints, and other database constraints
- **Flexible Output**: Can write reports locally or directly to AWS S3
- **Fault Tolerance**: Captures and logs errors per table, allowing comparison to continue for other tables

### Parent Class

Inherits from `GlueELTStep` - an AWS Glue ELT (Extract-Load-Transform) step that integrates with AWS Glue jobs.

---

## Purpose & Use Cases

### Primary Purpose

The `DDLComparator` class solves a critical data engineering problem: **ensuring schema consistency during data
migrations and ETL pipelines**. When migrating data from traditional systems to cloud data warehouses, it's crucial to
verify that table structures match expectations.

### Typical Use Cases

#### **Data Migration Validation**

```
Scenario: You're migrating tables from Teradata to Redshift
Problem: Need to verify all column definitions and constraints match
Solution: DDLComparator compares Git-stored DDLs against Redshift
```

#### **Schema Version Control**

```
Scenario: Track schema changes over time using Git
Problem: Need to verify deployed schemas match version-controlled definitions
Solution: Compare versioned SQL files against live databases
```

#### **Data Warehouse Deployment**

```
Scenario: Deploy new Iceberg tables to AWS Glue
Problem: Ensure table definitions match specifications
Solution: Compare source DDLs against Iceberg catalog
```

#### **Multi-Target Sync Verification**

```
Scenario: Replicate same schema to both Redshift and Iceberg
Problem: Ensure both targets have identical schemas
Solution: Compare both targets independently and report differences
```

#### **Automated Data Quality Checks**

```
Scenario: Include in automated testing pipeline
Problem: Need to validate DDLs as part of CI/CD
Solution: Run DDLComparator as part of ELT job
```

---

## Class Architecture

### Class Hierarchy

```
GlueELTStep (Parent Class)
    │
    └── DDLComparator
            │
            ├── Comparison Methods
            │   ├── compare_redshift_table_ddl()
            │   ├── compare_iceberg_tables()
            │   ├── compare_tables()
            │   ├── compare_tables_iceberg()
            │   └── compare_constraints()
            │
            ├── Extraction Methods
            │   ├── extract_ddl_from_zip()
            │   ├── extract_ddl_for_tables()
            │   ├── get_redshift_table_columns()
            │   ├── get_redshift_table_constraints()
            │   └── get_iceberg_columns_from_glue()
            │
            ├── Parsing Methods
            │   ├── parse_ddl_from_sql()
            │   ├── format_constraint()
            │   └── extract_name_fallback()
            │
            └── Reporting Methods
                ├── executeFlow()
                ├── write_report()
                ├── add_sheet_to_report()
                ├── write_to_excel()
                └── add_failed_table_names_to_report()
```

### Class Variables

```python
log = logging.getLogger(__name__)  # Logger instance for all logging
local_folder = '/tmp/ddl_comparison'  # Local temp directory for files
git_code_dir = 'git_codebase'  # Subdirectory for extracted code
```

---

### Initialization

```python
from com.impetus.idw.wmg.common.ddl_comparator import DDLComparator

# Create an instance
comparator = DDLComparator()

# In AWS Glue context, the instance is created automatically as a step
```

### Required Parameters

When using as an AWS Glue step, provide these parameters:

```python
# For Redshift comparison
{
    'compare_redshift_ddl': True,              # Enable Redshift comparison
    'redshift_zip_s3_path': 's3://bucket/redshift_ddls.zip',
    'redshift_table_names': 'SCHEMA1.TABLE1,SCHEMA1.TABLE2',
    'redshift_secret': 'redshift-secret-name',
    'report_local_path': '/tmp/ddl_report.xlsx',  # Optional: local report
    's3_output_path': 's3://bucket/reports/',    # Optional: S3 upload
}

# For Iceberg comparison
{
    'compare_iceberg_ddl': True,               # Enable Iceberg comparison
    'iceberg_zip_s3_path': 's3://bucket/iceberg_ddls.zip',
    'iceberg_table_names': 'DB1.TABLE1,DB1.TABLE2',
    'report_local_path': '/tmp/ddl_report.xlsx',
    's3_output_path': 's3://bucket/reports/',
}
```

---

## Core Concepts

### DDL (Data Definition Language)

**Definition**: SQL commands that define database structures (CREATE TABLE, ALTER TABLE, etc.)

**Example**:

```sql
CREATE TABLE customers (
    customer_id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_email UNIQUE(email)
);
```

### Schema Comparison Flow

```
┌─────────────────┐
│ Git Source (ZIP)│
└────────┬────────┘
         │
         ├─→ Extract ZIP
         │
         ├─→ Parse SQL Files
         │
         ├─→ Extract DDL Statements
         │
         └─→ Parse Column Definitions
                     │
                     ├──────────────────────┬──────────────────────┐
                     ▼                      ▼                      ▼
            ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐
            │ Redshift Target  │  │ Iceberg Target   │  │ Other Targets│
            └────────┬─────────┘  └────────┬─────────┘  └──────┬───────┘
                     │                     │                   │
                     ├─→Query Live DB      ├─→Query Glue Catalog
                     │                     │
                     ├─→Get Columns        ├─→Get Columns
                     │                     │
                     └─→Get Constraints    └─→Get Constraints
                     │                     │
                     └─────────┬───────────┘
                               │
                               ▼
                      ┌─────────────────┐
                      │  Compare Schemas│
                      └────────┬────────┘
                               │
                               ├─→ Column Differences
                               ├─→ Data Type Mismatches
                               ├─→ Constraint Differences
                               └─→ Default Value Differences
                               │
                               ▼
                      ┌─────────────────────┐
                      │ Generate Excel Report│
                      └─────────────────────┘
```

### Column Definition Structure

Internally, columns are represented as dictionaries:

```python
column_def = {
    'data_type': 'VARCHAR',           # e.g., INT, VARCHAR, DECIMAL
    'column_default': 'NULL',         # Default value if any
    'is_nullable': 'YES',             # YES or NO
    'numeric_precision': None,        # For DECIMAL/NUMERIC types
    'numeric_scale': None,            # For DECIMAL/NUMERIC types
    'character_maximum_length': 100   # For CHAR/VARCHAR types
}
```

### Constraint Representation

Constraints are stored as strings in a specific format:

```python
constraints = [
    'p id',        # 'p' = primary key on column 'id'
    'u email',     # 'u' = unique constraint on column 'email'
]
```

### Difference Report Format

Results are tuples of (table_name, column_name, difference_description):

```python
differences = [
    ('SCHEMA.TABLE1', 'column1', 'Missing in Redshift'),
    ('SCHEMA.TABLE1', 'column2', "Mismatch in data_type: GIT(VARCHAR) != RS(TEXT)"),
    ('SCHEMA.TABLE2', 'id', 'Constraint missing in Redshift'),
]
```

---

## Main Methods

### executeFlow() - Main Entry Point

**Purpose**: Orchestrate the entire DDL comparison workflow

**Parameters via Job Config**:

```python
compare_redshift_ddl = True   # Boolean: run Redshift comparison?
compare_iceberg_ddl = True    # Boolean: run Iceberg comparison?
```

**Example Usage**:

```python
# In AWS Glue job
job = glue.create_job(
    Name='my_ddl_comparison_job',
    Command={
        'Name': 'pythonshell',
        'ScriptLocation': 's3://bucket/ddl_comparator.py'
    }
)

# When job runs, DDLComparator.executeFlow() is called automatically
```

**Output**:

- Excel report at local path (if provided)
- Excel report in S3 (if S3 path provided)
- Console logs with all details

---

### compare_redshift_table_ddl() - Redshift Comparison

**Purpose**: Extract Git DDL and compare against live Redshift database

**Parameters Required**:

```python
redshift_zip_s3_path = 's3://mybucket/redshift_ddl.zip'
redshift_table_names = 'schema1.table1,schema1.table2,schema2.table3'
redshift_secret = 'my-redshift-secret'  # From AWS Secrets Manager
```

**Return Value**:

```python
[
    ('SCHEMA1.TABLE1', 'col1', 'Missing in Redshift'),
    ('SCHEMA1.TABLE1', 'col2', "Mismatch in data_type: GIT(INT) != RS(BIGINT)"),
    ('SCHEMA1.TABLE2', 'id', 'Constraint missing in Redshift'),
]
```

**Example**:

```python
comparator = DDLComparator()
failed = []
matched = set()

# Set required parameters
comparator.set_param_value('redshift_zip_s3_path', 's3://mybucket/ddl.zip')
comparator.set_param_value('redshift_table_names', 'customers.users,customers.orders')
comparator.set_param_value('redshift_secret', 'redshift-prod-secret')

# Run comparison
results = comparator.compare_redshift_table_ddl(failed, matched)

print(f"Matched tables: {matched}")
print(f"Failed tables: {failed}")
print(f"Differences: {results}")
```

---

### compare_iceberg_tables() - Iceberg Comparison

**Purpose**: Extract Git DDL and compare against Iceberg tables in AWS Glue catalog

**Key Differences from Redshift Comparison**:

- Uses Glue catalog queries instead of direct database connection
- Executes `SHOW CREATE TABLE glue_catalog.schema.table`
- Uses Iceberg-specific data type mappings

**Parameters Required**:

```python
iceberg_zip_s3_path = 's3://mybucket/iceberg_ddl.zip'
iceberg_table_names = 'analytics_db.customers,analytics_db.orders'
```

**Example**:

```python
comparator = DDLComparator()
failed = []
matched = set()

comparator.set_param_value('iceberg_zip_s3_path', 's3://mybucket/ddl.zip')
comparator.set_param_value('iceberg_table_names', 'mydb.table1,mydb.table2')

results = comparator.compare_iceberg_tables(failed, matched)

print(f"Tables matching perfectly: {matched}")
print(f"Tables with issues: {failed}")
```

---

### extract_ddl_from_zip() - ZIP Extraction

**Purpose**: Download a ZIP file from S3 and extract locally

**Directory Structure Created**:

```
/tmp/ddl_comparison/
└── git_codebase/
    └── <zip_name_without_extension>/
        ├── schema1/
        │   ├── table1.sql
        │   └── table2.sql
        ├── schema2/
        │   └── table3.sql
        └── ...
```

**Example**:

```python
comparator = DDLComparator()

# Download and extract ZIP
local_path = comparator.extract_ddl_from_zip('s3://mybucket/ddls/redshift_2024.zip')

print(f"Extracted to: {local_path}")
# Output: /tmp/ddl_comparison/git_codebase/redshift_2024

# Now you can access extracted files
import os
files = os.listdir(local_path)
print(files)  # ['schema1', 'schema2', ...]
```

---

### extract_ddl_for_tables() - SQL Parsing

**Purpose**: Parse SQL files and extract DDL statements for specific tables

**Example**:

```python
# Suppose you have this folder structure:
# /tmp/sql_files/
#   SCHEMA1_V1.0.sql
#   SCHEMA1_V1.1.sql
#   SCHEMA2_V1.0.sql

comparator = DDLComparator()

table_list = ['SCHEMA1.CUSTOMERS', 'SCHEMA1.ORDERS', 'SCHEMA2.PRODUCTS']

ddl_map = comparator.extract_ddl_for_tables(table_list, '/tmp/sql_files/')

# Result structure:
# {
#     'SCHEMA1': {
#         'SCHEMA1.CUSTOMERS': 'CREATE TABLE SCHEMA1.CUSTOMERS (...)',
#         'SCHEMA1.ORDERS': 'CREATE TABLE SCHEMA1.ORDERS (...)'
#     },
#     'SCHEMA2': {
#         'SCHEMA2.PRODUCTS': 'CREATE TABLE SCHEMA2.PRODUCTS (...)'
#     }
# }

# Access extracted DDL
customers_ddl = ddl_map['SCHEMA1']['SCHEMA1.CUSTOMERS']
print(customers_ddl)
```

---

### parse_ddl_from_sql() - SQL Parsing

**Purpose**: Parse a CREATE TABLE statement and extract column definitions and constraints

**Column Information Extracted**:

```python
{
    'column_name': {
        'data_type': 'VARCHAR',              # Type: INT, VARCHAR, DECIMAL, etc.
        'column_default': 'CURRENT_DATE',    # Default value if any
        'is_nullable': 'NO',                 # YES or NO
        'numeric_precision': None,           # For DECIMAL: total digits
        'numeric_scale': None,               # For DECIMAL: decimal places
        'character_maximum_length': 100      # For VARCHAR/CHAR: max length
    },
    # ... more columns
}
```

**Example**:

```python
# Input SQL
sql = """
CREATE TABLE customers (
    customer_id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100),
    email VARCHAR(255) NOT NULL UNIQUE,
    age INT,
    birth_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP,
    salary DECIMAL(14, 2),
    CONSTRAINT unique_email UNIQUE(email)
);
"""

columns, constraints = comparator.parse_ddl_from_sql(sql)

```

---

## Helper Methods

### get_redshift_table_columns()

**Purpose**: Query Redshift and retrieve column definitions

**Example**:

```python
secret = 'my-redshift-prod'
cols, consts = comparator.get_redshift_table_columns(
    secret, 'sales_schema', 'customers'
)

for col_name, col_info in cols.items():
    print(f"{col_name}: {col_info['data_type']} ({col_info['is_nullable']})")

# Output:
# customer_id: BIGINT (NO)
# name: VARCHAR(100) (YES)
# email: VARCHAR(255) (NO)
```

---

### get_iceberg_columns_from_glue()

**Purpose**: Query Iceberg table schema via Glue catalog

**Query Executed**:

```sql
SHOW CREATE TABLE glue_catalog.{schema}.{table}
```

**Example**:

```python
cols, consts = comparator.get_iceberg_columns_from_glue('analytics_db', 'events')

for col_name, col_info in cols.items():
    print(f"{col_name}: {col_info['data_type']}")
```

---

### compare_tables()

**Purpose**: Compare two sets of column definitions

**Example**:

```python
git_cols = {
    'id': {'data_type': 'INT', 'is_nullable': 'NO', ...},
    'name': {'data_type': 'VARCHAR', 'character_maximum_length': 100, ...}
}

rs_cols = {
    'id': {'data_type': 'BIGINT', 'is_nullable': 'NO', ...},  # Different type!
    'name': {'data_type': 'VARCHAR', 'character_maximum_length': 100, ...}
}

diffs = comparator.compare_tables('SCHEMA.TABLE', git_cols, rs_cols)

# Result:
# [
#     ('SCHEMA.TABLE', 'id', "Mismatch in data_type: GIT(INT) != RS(BIGINT)")
# ]
```

---

### compare_constraints()

**Purpose**: Compare constraint definitions between sources

**Example**:

```python
git_consts = ['p id', 'u email']
rs_consts = ['p id']

diffs = comparator.compare_constraints('SCHEMA.TABLE', git_consts, rs_consts)

# Result:
# [
#     ('SCHEMA.TABLE', 'u email', 'Constraint missing in Redshift')
# ]
```

---

## Data Structures

### Column Definition Dictionary

```python
# Structure
{
    'column_name': {
        'data_type': str,                      # 'INT', 'VARCHAR', 'DECIMAL', etc.
        'column_default': str or None,         # 'NULL', 'CURRENT_DATE', etc.
        'is_nullable': str,                    # 'YES' or 'NO'
        'numeric_precision': int or None,      # For DECIMAL: total digits
        'numeric_scale': int or None,          # For DECIMAL: decimal places
        'character_maximum_length': int or None# For VARCHAR/CHAR: max chars
    }
}

# Example
columns = {
    'customer_id': {
        'data_type': 'BIGINT',
        'column_default': None,
        'is_nullable': 'NO',
        'numeric_precision': None,
        'numeric_scale': None,
        'character_maximum_length': None
    },
    'email': {
        'data_type': 'VARCHAR',
        'column_default': None,
        'is_nullable': 'NO',
        'numeric_precision': None,
        'numeric_scale': None,
        'character_maximum_length': 255
    },
    'balance': {
        'data_type': 'DECIMAL',
        'column_default': '0.00',
        'is_nullable': 'YES',
        'numeric_precision': 18,
        'numeric_scale': 2,
        'character_maximum_length': None
    }
}
```

### Difference Report Format

```python
# Structure
differences = [
    (table_name, column_or_constraint, description),
    (table_name, column_or_constraint, description),
    ...
]

# Examples
[
    ('CUSTOMERS', 'id', 'Missing in Redshift'),
    ('CUSTOMERS', 'email', "Mismatch in data_type: GIT(VARCHAR) != RS(TEXT)"),
    ('CUSTOMERS', 'phone', "Mismatch in character_maximum_length: GIT(20) != RS(15)"),
    ('CUSTOMERS', 'p customer_id', 'Constraint missing in Redshift'),
    ('ORDERS', 'order_id', 'Missing in GIT'),
]
```

### Extracted DDL Map

```python
# Structure
ddl_map = {
    'SCHEMA_UPPER': {
        'TABLE_UPPER': 'CREATE TABLE ...',
        'TABLE2_UPPER': 'CREATE TABLE ...'
    }
}

# Example
{
    'SALES': {
        'SALES.CUSTOMERS': 'CREATE TABLE SALES.CUSTOMERS (customer_id INT, ...);',
        'SALES.ORDERS': 'CREATE TABLE SALES.ORDERS (order_id INT, ...);'
    },
    'ANALYTICS': {
        'ANALYTICS.METRICS': 'CREATE TABLE ANALYTICS.METRICS (metric_id INT, ...);'
    }
}
```

---

### Parameters Reference

| Parameter              | Type | Required            | Description                                 |
|------------------------|------|---------------------|---------------------------------------------|
| `compare_redshift_ddl` | bool | No                  | Enable Redshift comparison (default: false) |
| `compare_iceberg_ddl`  | bool | No                  | Enable Iceberg comparison (default: false)  |
| `redshift_zip_s3_path` | str  | If compare_redshift | S3 path to Redshift DDL archive             |
| `redshift_table_names` | str  | If compare_redshift | Comma-separated table list                  |
| `redshift_secret`      | str  | If compare_redshift | AWS Secrets Manager secret name             |
| `iceberg_zip_s3_path`  | str  | If compare_iceberg  | S3 path to Iceberg DDL archive              |
| `iceberg_table_names`  | str  | If compare_iceberg  | Comma-separated table list                  |
| `report_local_path`    | str  | No                  | Local path for Excel report                 |
| `s3_output_path`       | str  | No                  | S3 path for report upload                   |

---