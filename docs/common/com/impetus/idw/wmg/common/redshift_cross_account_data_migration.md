# RedshiftCrossAccountDataMigration Class

## Overview
The `RedshiftCrossAccountDataMigration` class is a Glue ELT step designed for migrating data to Amazon Redshift databases across different AWS accounts. It supports two migration modes: configuration file-based (using a CSV file for detailed table mappings) and schema-based (automatic discovery of tables from the target schema). The class handles data transfer with support for append and overwrite load types, validates configurations, and logs success/failure metrics. It inherits from `GlueELTStep` and integrates with Glue executors for database operations.

## What It Does
- Reads migration configurations either from a CSV file or generates them dynamically from source and target schema parameters.
- Validates each migration row for required fields and valid load types.
- Executes data migration queries to insert data from source tables to target Redshift tables, handling filters and load types.
- Tracks successful and failed migrations, logging counts and details.
- Optionally writes failed migration configurations to an S3 path for error analysis.
- Supports cross-account data transfers by using Redshift secrets for authentication.

## Arguments
The class expects job arguments retrieved via `self.get_param_value`. It supports two modes, requiring either config file parameters or schema parameters:

### Configuration File Mode
- `config_path` (required for this mode): S3 path to the CSV configuration file.
- `config_file_sep` (optional): Separator for the CSV file. Defaults to '#'.
- `error_output_path` (optional): S3 path to write failed migration rows as JSON.

### Schema Mode
- `source_schema` (required for this mode): Name of the source schema.
- `target_schema` (required for this mode): Name of the target Redshift schema.
- `secret_name` (required for this mode): AWS Secrets Manager secret name for Redshift connection.
- `error_output_path` (optional): S3 path to write failed migration rows as JSON.

The CSV configuration file should have columns: source_table, source_columns (optional), source_filter (optional), target_table, secret_name, load_type (optional, default append).

## Configuration File Format
The CSV file used in configuration file mode must contain the following columns (headers expected):

- `source_table` (required): Fully qualified source table name (e.g., shared_db.schema.table).
- `source_columns` (optional): Comma-separated list of columns to select from the source. If omitted, all columns are selected.
- `source_filter` (optional): WHERE clause condition for filtering source data (without the `WHERE` keyword). Defaults to '1=1' if not provided.
- `target_table` (required): Fully qualified target table name in Redshift (e.g., schema.table).
- `secret_name` (required): AWS Secrets Manager secret name for Redshift connection.
- `load_type` (optional): Data load mode. Valid values: 'append' (default) or 'overwrite'.

Example CSV content (using '#' as separator):

```
source_table#source_columns#source_filter#target_table#secret_name#load_type
shared_db.schema.orders##date >= '2023-01-01'#analytics.orders#redshift-secret#append
shared_db.schema.customers#id,name##analytics.customers#redshift-secret#overwrite
```

## Usage
The `RedshiftCrossAccountDataMigration` class is used in ETL pipelines for secure, cross-account data migration to Redshift. It enables flexible data transfers by supporting both predefined configurations and automatic schema-based migrations. This is ideal for data consolidation, backup, or synchronization tasks where data needs to be moved between Redshift clusters in different accounts.

### Key Features
- **Dual Migration Modes**: Supports CSV-driven configurations for custom mappings or schema-based auto-discovery for bulk migrations.
- **Load Type Support**: Handles 'append' (default) and 'overwrite' operations for data insertion.
- **Validation and Error Handling**: Validates input rows and provides detailed logging of successes and failures.
- **Cross-Account Compatibility**: Uses secrets for secure access to Redshift across AWS accounts.
- **Error Reporting**: Outputs failed configurations to S3 for troubleshooting.

## Exceptions
- **Exception**: Raised if neither `config_path` nor the combination of `source_schema`, `target_schema`, and `secret_name` is provided.
- **Exception**: Raised during row validation if required fields (e.g., source_table, target_table, secret_name) are missing or invalid.
- **Exception**: Propagated from database query executions if connections fail, permissions are insufficient, or SQL errors occur.
- **Exception**: Raised for unsupported load types (only 'append' and 'overwrite' are supported; 'incremental' is not implemented).
- General exceptions from Glue executor methods or S3 operations if configurations are incorrect.

## How to Use It
1. Prepare a CSV configuration file with migration details or specify schema parameters.
2. Configure the Glue job with the appropriate arguments based on the chosen mode.
3. Instantiate `RedshiftCrossAccountDataMigration` within the Glue workflow.
4. Run the migration via the framework's executor, which handles data transfers.
5. For standalone testing (as in the module's `__main__`):
   ```python
   if __name__ == '__main__':
       step = RedshiftCrossAccountDataMigration()
       step.start()
   ```
   Note: Standalone runs require proper AWS credentials, Glue environment, and executor setup.

The class will process each migration task, logging progress and optionally saving error details to S3.