# ddl_validator.py

## Overview

`ddl_validator.py` provides `DDLValidator`, a Glue ELT step that validates DDL statements (CREATE TABLE and CREATE OR
REPLACE VIEW) found in zipped SQL archives. It is designed to:

- extract SQL files from an S3 zip archive,
- identify CREATE TABLE / CREATE OR REPLACE VIEW statements,
- execute those statements against execution layers (Redshift or Iceberg via Glue/executor),
- capture success/failure and categorize errors,
- produce a single Excel validation report and upload it to the configured output location (usually S3).

This guide explains what the class does, how it works, available methods, configuration, example usage, and
troubleshooting tips so beginners can use and extend the step.

 ---

## Quick summary of functionality

- Downloads DDL zip(s) from S3 and extracts .sql files
- Looks for CREATE TABLE IF NOT EXISTS and CREATE OR REPLACE VIEW statements using regex
- Builds `ValidationObject` instances for each discovered statement
- Executes each DDL using an `executor` (must implement executeQuery)
- Records results and formatted errors per statement
- Writes a timestamped Excel report and uploads it to the configured `output_path`

 ---

## Key types

### ValidationObject

Represents one DDL statement to validate. Important attributes:

- `input_file` — source .sql filename
- `db_type` — execution layer (`'redshift'` or `'iceberg'`)
- `query` — the CREATE TABLE / CREATE VIEW SQL
- `object_name` — extracted identifier (lowercased)
- `validation_status` — `'Success'` or `'Failed'` after execution
- `error` — raw error string when present
- `formatted_error` — cleaned/short error for reporting
- `error_type` — inferred category (Table Not Found, Column Not Found, Syntax Error, ...)
- `secret_name` — optional: secret to pass when executing against Redshift

This object is created during extraction and gets updated by the execution step.

### DDLValidator

Subclass of `GlueELTStep`. Orchestrates extract → validate → report flow and expects a runtime `executor`
with `executeQuery` and the glue utils (`gu`) file copy helpers available.

 ---

## Configuration (job parameters)

The step reads parameters via `self.get_param_value`. Important parameters:

- `redshift_ddl_file_path` (str) — S3 path to zip with Redshift SQLs (optional if not validating Redshift)
- `iceberg_ddl_file_path` (str) — S3 path to zip with Iceberg SQLs (optional if not validating Iceberg)
- `output_path` (str) — required. S3 folder where the generated Excel report will be uploaded
- `local_folder_path` (str) — optional. Local temp folder for extraction and local report (default `/tmp`)
- `secret_name` (str) — required if `redshift_ddl_file_path` is present; name of Redshift secret to pass to executor

Validation rules:

- At least one of `redshift_ddl_file_path` or `iceberg_ddl_file_path` must be provided.
- `output_path` is mandatory.
- If validating Redshift, `secret_name` must be provided.

 ---

## Main method

### executeFlow(self, executor, *args, **kwargs)

High-level responsibilities:

- Validate that required parameters are present
- Download and extract each provided zip to a local folder
- Discover CREATE TABLE and CREATE OR REPLACE VIEW statements
- Build `ValidationObject` lists for tables and views
- Execute DDLs and update each object's status
- Generate an Excel report and upload to `output_path`

Notes:

- The method expects an `executor` that implements `executeQuery(sql, dbType=?, secret=?)` and that the `gu` module
  provides `copy_file_from_s3_to_local` and `copy_file_from_local_to_s3` helpers.
- Errors during individual DDL execution are recorded per object and do not stop the overall run.

Example usage (Glue runner handles executor normally):

 ```py
 validator = DDLValidator()
 validator.set_param_value('redshift_ddl_file_path', 's3://.../redshift_ddls.zip')
 validator.set_param_value('output_path', 's3://.../reports/')
 validator.set_param_value('secret_name', 'redshift-secret')
 validator.executeFlow(executor)  # executor supplied by framework
 ```

 ---

## Output format

The Excel report columns (one row per discovered DDL statement):

- File Name — original SQL file name
- Object Name — extracted schema-qualified name (lower case)
- Query — the DDL SQL text
- Execution Layer — `redshift` or `iceberg`
- Validation Status — `Success` or `Failed`
- Formatted Error — cleaned error message
- Error Type — category inferred by `get_err_type`

The file is uploaded to the configured `output_path` and the local temp copy is written to `local_folder_path`.

 ---

## Examples

### Example 1 — Unit-test style with a Mock Executor (recommended when experimenting)

 ```py
 from com.impetus.idw.wmg.common.ddl_validator import DDLValidator

 class MockExecutor:
     def executeQuery(self, sql, dbType=None, secret=None):
         # Raise for a sentinel to simulate an error
         if 'FAIL_ME' in sql:
             raise Exception('Table or view not found: FAIL_ME')
         return None

 step = DDLValidator()
 step.set_param_value('redshift_ddl_file_path', 's3://bucket/redshift.zip')
 step.set_param_value('output_path', 's3://bucket/output/')
 step.set_param_value('secret_name', 'redshift-secret')
 step.set_param_value('local_folder_path', '/tmp')

 # Run with mock executor
 step.executeFlow(MockExecutor())

 # Check S3 for generated report or review logs for successes/failures
 ```

### Example 2 — Glue job parameters (production)

Provide job parameters to the step similar to this JSON:

 ```json
 {
   "redshift_ddl_file_path": "s3://my-bucket/redshift_ddls.zip",
   "iceberg_ddl_file_path": "s3://my-bucket/iceberg_ddls.zip",
   "output_path": "s3://my-bucket/ddl-reports/",
   "secret_name": "my-redshift-secret",
   "local_folder_path": "/tmp"
 }
 ```

Glue will provide the `executor` and call `DDLValidator.executeFlow(executor)` as part of the ETL workflow.
---

