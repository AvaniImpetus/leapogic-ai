# PostgresObjectExtractor Class

## Overview
The `PostgresObjectExtractor` class is designed for extracting Data Definition Language (DDL) statements from PostgreSQL database objects, specifically tables and views. It inherits from `RedshiftObjectDDLExtractor` and adapts the functionality for PostgreSQL databases, generating CREATE statements that can be used for schema replication, migration, or documentation. The class integrates with AWS Glue workflows and uses IAM-based authentication for secure database access.

## What It Does
- Connects to a PostgreSQL database using JDBC and IAM-generated authentication tokens.
- Extracts DDL statements for specified tables and views by querying the information schema.
- Formats the extracted DDL into executable CREATE TABLE or CREATE OR REPLACE VIEW statements.
- Supports extraction of column details, data types, constraints (e.g., NOT NULL), and view definitions.
- Handles PostgreSQL-specific data type mappings (e.g., character varying to varchar, timestamp variants).
- Generates connection configurations dynamically based on job parameters or defaults.

## Arguments
The class retrieves arguments via `self.get_param_value` for database connection and configuration:

- `pg_host` (optional): PostgreSQL database host. Defaults to 'bcbsmn-bd-dev-etloperations.cjwciiiianhu.us-east-1.rds.amazonaws.com'.
- `pg_port` (optional): Database port. Defaults to 5432.
- `pg_database` (optional): Database name. Defaults to 'etloperations'.
- `pg_user` (optional): Database username. Defaults to 'svc_bd_lambda_file_registry'.

Additional arguments may be inherited from the parent class `RedshiftObjectDDLExtractor` for object extraction specifics.

## Usage
The `PostgresObjectExtractor` class is used in ETL pipelines for schema extraction and migration tasks involving PostgreSQL databases. It enables automated generation of DDL scripts for tables and views, facilitating database schema replication across environments or documentation. The class is particularly useful in AWS Glue jobs for cross-database operations where DDL needs to be captured and applied elsewhere.

### Key Features
- **DDL Extraction**: Generates accurate CREATE statements for tables (including columns, types, and constraints) and views.
- **IAM Authentication**: Uses AWS RDS IAM authentication for secure, password-less database access.
- **PostgreSQL Compatibility**: Handles PostgreSQL-specific data types and schema queries.
- **Dynamic Configuration**: Builds JDBC connection strings and credentials from parameters or defaults.
- **Inheritance from Redshift Extractor**: Leverages common extraction logic while customizing for PostgreSQL.

## Exceptions
- **boto3 Exceptions**: May occur during IAM token generation, such as `ClientError` for invalid credentials or permissions.
- **Database Connection Exceptions**: Raised if JDBC connection fails due to network issues, invalid host/port, or authentication problems.
- **Query Execution Exceptions**: Propagated from SQL queries if the database schema is inaccessible or queries fail.
- **Parameter Validation Exceptions**: Inherited from parent class or Glue framework if required parameters are missing.
- General exceptions from AWS region environment variables or Glue utilities if not properly configured.

## How to Use It
1. Configure AWS credentials and ensure the Glue job has access to the PostgreSQL RDS instance via IAM.
2. Set job parameters for database connection (e.g., `pg_host`, `pg_port`) if different from defaults.
3. Instantiate `PostgresObjectExtractor` within a Glue workflow.
4. Call inherited methods (e.g., from `RedshiftObjectDDLExtractor`) to specify objects and extract DDL.
5. For standalone execution (as in the module's `__main__`):
   ```python
   if __name__ == '__main__':
       step = PostgresObjectExtractor()
       step.start()
   ```
   Note: Standalone runs require proper AWS environment and Glue setup for parameter resolution and executor access.

The extracted DDL can be used to recreate tables and views in target databases or for schema comparison tools.
___
