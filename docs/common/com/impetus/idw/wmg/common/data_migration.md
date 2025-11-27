# DataMigration Class

## Overview
The `DataMigration` class is a Glue ELT step that facilitates data migration between databases using a configuration-driven approach. It reads a CSV file containing migration specifications and executes data transfers from source to target databases, supporting different load types such as append, overwrite, and incremental loads. The class inherits from `GlueELTStep` and relies on an executor object for database operations.

## What It Does
- Reads a configuration CSV file that defines migration tasks, including source and target database details, tables, connections, and load options.
- Validates each configuration row for required fields and valid load types.
- For each valid row, constructs and executes SQL queries to read data from the source and write it to the target.
- Supports three load types:
  - **Append** (default): Appends data to the target table.
  - **Overwrite**: Overwrites the target table with new data.
  - **Incremental**: Migrates only new or updated records based on a watermark column.
- Logs progress, success, and failure counts for each migration task.
- Continues processing even if individual rows fail, collecting failed rows for reporting.

## Arguments
The class expects arguments in two forms: job-level arguments and configuration file columns.

### Job Arguments
- `config_path` (required): S3 location or file path to the configuration CSV file.
- `config_file_sep` (optional): Separator used in the CSV file. Defaults to `#` if not provided.

### Configuration File Format
The CSV file must contain the following columns (headers expected):

- `source_name`: Source database type (e.g., postgres, SQL Server, Redshift).
- `source_table`: Fully qualified source table name (e.g., db.table or db.schema.table).
- `source_connection`: Named connection for the source database.
- `source_columns` (optional): Comma-separated list of columns to select. If omitted, selects all columns (`*`).
- `source_filter` (optional): WHERE clause condition for filtering source data (without the `WHERE` keyword). Defaults to `1=1` if not provided.
- `target_name`: Target database type (e.g., postgres, SQL Server, Redshift).
- `target_table`: Fully qualified target table name (e.g., db.table or db.schema.table).
- `target_connection`: Named connection for the target database.
- `load_type` (optional): Load mode. Valid values: `append`, `overwrite`, `incremental`. Defaults to `append`.
- `incremental_load_column`: Required if `load_type` is `incremental`. The column name used as a watermark for incremental loads.

## Usage
The `DataMigration` class is designed to run within a Glue/ELT framework that provides an `executor` object for database interactions. It processes a CSV configuration file to perform batch data migrations across multiple source-target pairs.

### Example Configuration CSV
Assuming separator is `#`:

```
source_name#source_table#source_connection#source_columns#source_filter#target_name#target_table#target_connection#load_type#incremental_load_column
postgres#public.orders#pg_conn##order_date >= '2024-01-01'#redshift#analytics.public.orders#rs_conn#append#
postgres#public.customers#pg_conn## #redshift#analytics.public.customers#rs_conn#overwrite#
postgres#public.products#pg_conn## #redshift#analytics.public.products#rs_conn#incremental#updated_at
```

- First row: Appends orders from Postgres to Redshift, filtered by date.
- Second row: Overwrites customers table.
- Third row: Incremental load for products based on `updated_at` column.

## Exceptions
- **Missing config_path**: Raises `Exception('Need config CSV file path to proceed.')` if the required `config_path` job argument is not provided.
- **Invalid rows**: Rows with missing required fields (e.g., source_name, source_table, etc.) or invalid load_type are skipped. Validation messages are logged, and the row is not processed.
- **Incremental load issues**: If `load_type` is `incremental` but `incremental_load_column` is missing, the row is invalid. Additionally, if querying the max value from the target returns no rows, an exception is raised for that migration.
- **Migration failures**: Exceptions during data reading, querying, or writing (e.g., connection issues, SQL errors) are caught, logged, and the row is added to failed rows. Processing continues for other rows.
- General exceptions may occur from underlying executor methods (e.g., readFile, executeQuery, saveDataframeToTable) if the framework or connections are misconfigured.

## How to Use It
1. Prepare a CSV configuration file with the required columns and data migration specifications.
2. Upload the CSV to an accessible location (e.g., S3) and note the path.
3. In the Glue/ELT framework, configure the job with:
   - `config_path`: Path to the CSV file.
   - `config_file_sep`: CSV separator (optional, defaults to `#`).
4. Instantiate the `DataMigration` class and invoke it via the framework's runner, which provides the `executor` and parameter resolution.
