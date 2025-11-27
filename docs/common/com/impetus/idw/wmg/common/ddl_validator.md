# DDLValidator Class

## Overview
The `DDLValidator` class is a Glue ELT step that validates Data Definition Language (DDL) statements for database objects such as tables and views. It processes zipped SQL files from S3, extracts CREATE TABLE and CREATE OR REPLACE VIEW statements, executes them against specified database layers (Redshift or Iceberg), and generates a validation report in Excel format. The class inherits from `GlueELTStep` and relies on an executor for query execution and file operations.

## What It Does
- Downloads and extracts SQL files from provided zip archives stored in S3.
- Parses the SQL content to identify and extract CREATE TABLE IF NOT EXISTS and CREATE OR REPLACE VIEW statements using regular expressions.
- Executes each extracted DDL statement against the appropriate database (Redshift or Iceberg) to check for validity.
- Captures execution results, including success/failure status, error messages, and categorized error types.
- Generates a timestamped Excel report summarizing the validation results for all DDL statements and uploads it to a specified S3 output location.
- Logs progress and errors throughout the process.

## Arguments
The class expects the following job arguments, retrieved via `self.get_param_value`:

- `redshift_ddl_file_path` (optional): S3 path to a zip file containing Redshift DDL SQL scripts. At least one of `redshift_ddl_file_path` or `iceberg_ddl_file_path` must be provided.
- `iceberg_ddl_file_path` (optional): S3 path to a zip file containing Iceberg DDL SQL scripts. At least one of `redshift_ddl_file_path` or `iceberg_ddl_file_path` must be provided.
- `output_path` (required): S3 path (folder) where the generated Excel validation report will be uploaded.
- `local_folder_path` (optional): Local temporary folder path for extracting zip files and storing the local report. Defaults to `/tmp`.
- `secret_name` (required if `redshift_ddl_file_path` is provided): Name of the secret for Redshift database connection.

## Usage
The `DDLValidator` class is intended for use in ETL workflows to validate DDL scripts before deployment. It processes DDLs from zip files, executes them in a safe manner to detect issues like missing tables, syntax errors, or invalid references, and produces a comprehensive report. This helps ensure DDL compatibility and correctness across environments.

### Key Features
- **Multi-Database Support**: Validates DDLs for both Redshift and Iceberg databases.
- **Error Categorization**: Automatically categorizes errors (e.g., "Table Not Found", "Column Not Found", "Syntax Error") for easier troubleshooting.
- **Batch Processing**: Handles multiple SQL files and DDL statements in a single run.
- **Report Generation**: Produces an Excel report with details on each validated object, including file name, object name, query, execution layer, status, and errors.
- **S3 Integration**: Seamlessly downloads from and uploads to S3 using Glue utilities.

## Exceptions
- **Missing DDL Paths**: Raises `Exception('Need redshift_ddl_file_path or iceberg_ddl_file_path or both for validation')` if neither `redshift_ddl_file_path` nor `iceberg_ddl_file_path` is provided.
- **Missing Output Path**: Raises `Exception('Need output_path to save validation report')` if `output_path` is not specified.
- **Missing Secret for Redshift**: Raises `Exception('Need redshift secret name to execute queries on redshift')` if `redshift_ddl_file_path` is provided but `secret_name` is not.
- **Execution Errors**: Individual DDL execution failures (e.g., SQL syntax errors, missing dependencies) are caught, logged, and recorded in the validation report without stopping the overall process. Errors are categorized and formatted for clarity.
- General exceptions may occur from underlying operations like S3 file copying, zip extraction, or executor query execution if configurations are incorrect.

## How to Use It
1. Prepare zip files containing SQL scripts with DDL statements (CREATE TABLE IF NOT EXISTS or CREATE OR REPLACE VIEW).
2. Upload the zip files to S3 and note their paths.
3. In the Glue/ELT framework, configure the job with the required parameters:
   - `redshift_ddl_file_path` and/or `iceberg_ddl_file_path`: Paths to the DDL zip files.
   - `output_path`: S3 location for the report.
   - `secret_name`: For Redshift validation.
   - Optionally, `local_folder_path`.
4. Instantiate `DDLValidator` and run it via the framework's executor, which handles database connections and file operations.


The validation report will be available at the specified `output_path` in S3, providing insights into DDL validity and any issues encountered.
 ---



