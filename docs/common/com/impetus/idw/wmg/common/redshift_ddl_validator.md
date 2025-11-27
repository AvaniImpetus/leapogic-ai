# RedshiftDDLValidator Class

## Overview
The `RedshiftDDLValidator` class is a Glue ELT step designed for validating and comparing Data Definition Language (DDL) statements between source and target schemas in Amazon Redshift databases. It supports both single schema validation and batch validation using configuration files, generating detailed Excel reports on structural differences such as column mismatches, constraints, and data types. The class inherits from `GlueELTStep` and integrates with Glue executors for database operations and reporting.

## What It Does
- Extracts table and view definitions from source and target Redshift schemas.
- Compares object attributes including column names, data types, nullability, default values, collations, and constraints (primary keys, foreign keys, unique keys).
- Identifies mismatches, missing objects, and structural differences.
- Generates comprehensive Excel reports summarizing validation results.
- Supports email notifications with the report attached.
- Handles both single validation (one source-target pair) and batch validation (multiple pairs from a config file).

## Arguments
The class expects job arguments retrieved via `self.get_param_value`. It operates in two modes:

### Single Schema Validation Mode
- `source_schema` (required): Name of the source Redshift schema.
- `target_schema` (required): Name(s) of the target Redshift schema(s), comma-separated for multiple.
- `table_names` (optional): Comma-separated list of table names to validate. If omitted, all tables are validated.
- `view_names` (optional): Comma-separated list of view names to validate. If omitted, all views are validated.
- `redshift_secret` (required): AWS Secrets Manager secret name for Redshift connection.
- `output_path` (required): S3 or local path for the Excel report output.
- `email_sender` (optional): Email address for sending the report.
- `redshift_ddl_validation_send_report_to` (optional): Recipient email address. Defaults to a predefined address.
- `eng_app_environment` (optional): Environment name for email subject.

### Batch Validation Mode
- `config_file_path` (required): S3 or local path to an Excel configuration file.
- `config_sheet_name` (optional): Sheet name in the config file. Defaults to 'Config'.
- `redshift_secret` (required): AWS Secrets Manager secret name for Redshift connection.
- `output_path` (required): S3 or local path for the Excel report output.
- `email_sender` (optional): Email address for sending the report.
- `redshift_ddl_validation_send_report_to` (optional): Recipient email address.
- `eng_app_environment` (optional): Environment name for email subject.

The config Excel file should have columns:
                Source Schema - Source schema name
                Target Schema - Target schema name(s) (comma-separated for multiple)
                Table Names - Table names to compare (optional, comma-separated)
                View Names - View names to compare (optional, comma-separated)
                Enabled - Flag to enable/disable validation for this row (default: True)


## Usage
The `RedshiftDDLValidator` class is used in ETL workflows for schema validation and migration assurance in Redshift environments. It helps ensure DDL consistency between development, staging, and production schemas by comparing object structures and highlighting discrepancies. This is crucial for data migration projects, schema evolution, and compliance checks.

### Key Features
- **Dual Validation Modes**: Supports single-pair comparisons or batch processing from config files.
- **Comprehensive Comparisons**: Validates columns, data types, constraints, and more for tables and views.
- **Detailed Reporting**: Produces Excel reports with status indicators and comments on differences.
- **Email Integration**: Optional automated email delivery of validation reports.
- **Flexible Object Selection**: Allows specifying subsets of tables/views or validating all objects.
If table/view names not provided:
    Extract all tables/views from source schema
For each table in source or target schema:
    Extract column information from source schema
    Extract column information from target schema(s)
    Compare column attributes:
        1. Column name
        2. Column type
        3. Nullability
        4. Default value
        5. Collation

## Exceptions
- **Exception**: Raised if required parameters like `redshift_secret` or schema names are missing.
- **Exception**: Propagated from Redshift query executions if connections fail, permissions are insufficient, or SQL errors occur.
- **Exception**: Raised during file operations if config files cannot be read or reports cannot be written.
- **Exception**: Occurs if schema or object extractions fail due to invalid schemas or missing objects.
- General exceptions from Glue utilities or email sending if configurations are incorrect.

## What It Returns
 It generates an Excel report at the specified `output_path` and optionally sends it via email. Internally, it returns a list of validation result dictionaries for processing.
Output Report:
    Excel file:
        batch run: ddl_validation_{dd_mm_yyyy}.xlsx
        single run: ddl_validation_{source_schema}_to_{all_target_schemas}.xlsx
    sheet name: "Object Validation Report"
    Columns:
        Source Schema
        Target Schema
        Object Name
        Object Type - Table/View
        Column Name Status - Matched/{no. of columns} column not matched (Tables only)
        Column Datatype Status - Matched/{no. of columns} column not matched (Tables only)
        Column Collation Status - Matched/{no. of columns} column not matched (Tables only)
        Primary Key Status - Matched/{no. of constraints} column not matched (Tables only)
        Foreign Key Status - Matched/{no. of constraints} column not matched (Tables only)
        Unique Key Status - Matched/{no. of constraints} column not matched (Tables only)
        Default Value Status - Matched/{no. of columns} column not matched (Tables only)
        Overall Status - Matched/Not Matched/Not Available in Source Schema/Not Available in Target Schema
    Comments - Detailed differences

## How to Use It
1. Configure job parameters based on the desired mode (single or batch).
2. For batch mode, prepare an Excel config file with validation scenarios.
3. Instantiate `RedshiftDDLValidator` within the Glue workflow.
4. Run validation via the framework's executor, which handles database queries and report generation.
5. For standalone testing (as in the module's `__main__`):
   ```python
   if __name__ == '__main__':
       step = RedshiftDDLValidator()
       step.start()
   ```
   Note: Standalone runs require proper AWS credentials, Redshift access, and Glue environment setup.

The class will compare schemas, generate the report, and notify via email if configured, providing insights into DDL consistency.

