# DDLComparator Class

## Overview
The `DDLComparator` class is a Glue ELT step designed to compare database table definitions (DDLs) from version-controlled sources (Git-stored SQL files in ZIP archives) against live database targets. It ensures schema consistency during data migrations and ETL pipelines by validating column definitions, data types, and constraints across different database systems.

## What It Does
- Extracts DDL statements from ZIP files stored in S3 (containing SQL files from Git).
- Parses CREATE TABLE statements to extract column definitions and constraints.
- Compares the extracted schemas against live databases (Redshift or Iceberg).
- Generates detailed Excel reports highlighting differences in columns, data types, constraints, and defaults.
- Supports fault-tolerant execution, logging errors per table while continuing comparisons for others.
- Outputs reports locally or uploads them to S3.

## Key Features
- **Multi-Source Comparison**: Compares Git-based DDLs against Redshift and/or Iceberg targets.
- **Intelligent Parsing**: Automatically parses SQL CREATE TABLE statements and extracts structured column/constraint information.
- **Data Type Mapping**: Handles conversions and normalizations across database systems (e.g., VARCHAR vs. TEXT).
- **Constraint Validation**: Verifies primary keys, unique constraints, and other database constraints.
- **Detailed Reporting**: Produces Excel reports with granular differences at column and constraint levels.
- **Flexible Output**: Supports local file writing and S3 uploads.
- **Fault Tolerance**: Captures and logs errors per table, allowing the process to continue for other tables.

## Arguments
The class uses job parameters (set via Glue job configuration) to control behavior. All parameters are optional unless specified.

### Job Arguments
- `compare_redshift_ddl` (boolean): Enables Redshift DDL comparison. Required if comparing against Redshift.
- `redshift_zip_s3_path` (string): S3 path to ZIP file containing Redshift DDL SQL files. Required if `compare_redshift_ddl` is true.
- `redshift_table_names` (string): Comma-separated list of fully qualified table names (e.g., "schema1.table1,schema1.table2"). Required if `compare_redshift_ddl` is true.
- `redshift_secret` (string): AWS Secrets Manager secret name for Redshift connection. Required if `compare_redshift_ddl` is true.
- `compare_iceberg_ddl` (boolean): Enables Iceberg DDL comparison. Required if comparing against Iceberg.
- `iceberg_zip_s3_path` (string): S3 path to ZIP file containing Iceberg DDL SQL files. Required if `compare_iceberg_ddl` is true.
- `iceberg_table_names` (string): Comma-separated list of fully qualified table names (e.g., "db1.table1,db1.table2"). Required if `compare_iceberg_ddl` is true.
- `report_local_path` (string): Local file path for Excel report output (e.g., "/tmp/ddl_report.xlsx"). Optional; if not provided, no local report is generated.
- `s3_output_path` (string): S3 path for uploading the Excel report (e.g., "s3://bucket/reports/"). Optional; if not provided, no S3 upload occurs.

## Usage
The `DDLComparator` is intended for use in AWS Glue ELT jobs to validate schema consistency. It processes ZIP archives of SQL DDL files and compares them against specified database targets, producing reports for data quality assurance.

### Example Usage Scenarios
- **Data Migration Validation**: Compare Git-stored DDLs against Redshift to ensure migrated tables match source definitions.
- **Schema Version Control**: Verify that deployed Iceberg tables align with versioned SQL files.
- **Multi-Target Sync**: Compare the same DDLs against both Redshift and Iceberg to ensure identical schemas.
- **CI/CD Integration**: Include in automated pipelines to validate DDLs as part of testing.

### Configuration Example
In a Glue job, set parameters as follows:

```python
# For Redshift comparison
job_params = {
    'compare_redshift_ddl': True,
    'redshift_zip_s3_path': 's3://my-bucket/ddls/redshift_ddls.zip',
    'redshift_table_names': 'sales.customers,sales.orders',
    'redshift_secret': 'redshift-prod-secret',
    'report_local_path': '/tmp/ddl_comparison_report.xlsx',
    's3_output_path': 's3://my-bucket/reports/'
}

# For Iceberg comparison
job_params = {
    'compare_iceberg_ddl': True,
    'iceberg_zip_s3_path': 's3://my-bucket/ddls/iceberg_ddls.zip',
    'iceberg_table_names': 'analytics.events,analytics.metrics',
    'report_local_path': '/tmp/ddl_comparison_report.xlsx',
    's3_output_path': 's3://my-bucket/reports/'
}
```

## Exceptions
- **Missing Parameters**: If required parameters (e.g., `redshift_zip_s3_path` when `compare_redshift_ddl` is true) are not provided, the comparison for that target may fail or be skipped.
- **ZIP Extraction Errors**: Exceptions during S3 download or ZIP extraction (e.g., invalid ZIP file, S3 access issues) are logged, and the process may halt for that target.
- **SQL Parsing Errors**: If DDL files contain invalid SQL or unparseable CREATE TABLE statements, errors are logged per table, and comparison continues for others.
- **Database Connection Errors**: Failures in querying Redshift (via secret) or Iceberg (via Glue catalog) result in logged errors, with the table marked as failed.
- **Report Generation Errors**: Issues writing to local files or S3 (e.g., permissions, disk space) are caught and logged, but do not stop other operations.
- **General Exceptions**: Any unexpected errors during comparison are caught, logged with stack traces, and the process continues for remaining tables.

## How to Use It
1. Prepare ZIP archives of SQL DDL files (CREATE TABLE statements) and upload to S3.
2. Configure an AWS Glue job with the `DDLComparator` class as the script.
3. Set the required job parameters based on the targets (Redshift and/or Iceberg).
4. Run the Glue job; the class will automatically execute `executeFlow()` to perform comparisons and generate reports.
5. Review the Excel report for differences, matched tables, and failed tables.
6. For standalone testing, instantiate the class and call methods directly (e.g., `compare_redshift_table_ddl()`), but full integration requires the Glue framework for executor and parameter resolution.

Ensure AWS permissions for S3 access, Secrets Manager (for Redshift), and Glue catalog (for Iceberg) are configured.

                |

---