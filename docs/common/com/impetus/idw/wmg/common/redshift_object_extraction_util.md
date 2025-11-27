# RedshiftObjectDDLExtractor Class

## Overview
The `RedshiftObjectDDLExtractor` class is a Glue ELT step designed for extracting Data Definition Language (DDL) statements from database objects (tables and views) in Amazon Redshift. It organizes the extracted DDLs by schema, writes them to SQL files, packages them into a ZIP archive, and uploads the results to S3. The class supports extraction from specified schemas, individual tables, or views, and can optionally include dependent objects. It inherits from `GlueELTStep` and integrates with Glue executors for database operations.

## What It Does
- Connects to Redshift using provided secrets and executes SHOW TABLE/VIEW queries to retrieve DDL statements.
- Extracts DDL for specified tables and views, or discovers all objects in given schemas.
- Groups DDL statements by schema and writes them to separate SQL files (tables.sql and views.sql per schema).
- Creates a ZIP archive containing all extracted files for easy distribution.
- Generates a lineage sheet documenting object dependencies.
- Uploads the organized files and ZIP archive to the specified S3 output path.

## Arguments
The class expects job arguments retrieved via `self.get_param_value`:

- `output_path` (required): S3 path where the extracted DDL files and ZIP archive will be uploaded.
- `schema_name` (optional): Comma-separated list of schema names to extract all tables and views from.
- `table_names` (optional): Comma-separated list of specific table names to extract DDL for.
- `view_names` (optional): Comma-separated list of specific view names to extract DDL for.
- `secret_name` (required): AWS Secrets Manager secret name for Redshift connection.
- `extract_dependent_objects` (optional): Boolean flag to include dependent objects in extraction. Defaults to `True`.

At least one of `schema_name`, `table_names`, or `view_names` must be provided.

## Usage
The `RedshiftObjectDDLExtractor` class is used in ETL workflows for schema documentation, migration preparation, or backup purposes in Redshift environments. It enables automated extraction of DDL scripts, which can be used to recreate database objects in other environments or for version control. The organized output facilitates easy review and deployment of schema changes.

### Key Features
- **Flexible Extraction**: Supports schema-wide extraction or targeted extraction of specific tables/views.
- **Organized Output**: Groups DDLs by schema and type, with ZIP packaging for portability.
- **Dependency Handling**: Optionally extracts and documents dependent objects for complete schema representation.
- **S3 Integration**: Direct upload to S3 with structured directory layout.
- **Lineage Tracking**: Creates dependency sheets for understanding object relationships.

## Exceptions
- **Exception**: Raised if `output_path` is not provided.
- **Exception**: Raised if none of `schema_name`, `table_names`, or `view_names` are specified.
- **Exception**: Propagated from Redshift query executions if connections fail, permissions are insufficient, or objects do not exist.
- **Exception**: Occurs during file operations if S3 uploads fail or local file writes encounter issues.
- General exceptions from Glue utilities or secret retrieval if configurations are incorrect.

## How to Use It
1. Configure job parameters with the output S3 path, secret name, and object specifications (schemas, tables, or views).
2. Optionally set `extract_dependent_objects` to control dependency inclusion.
3. Instantiate `RedshiftObjectDDLExtractor` within the Glue workflow.
4. Run extraction via the framework's executor, which handles database queries and S3 uploads.
5. For standalone testing (as in the module's `__main__`):
   ```python
   if __name__ == '__main__':
       step = RedshiftObjectDDLExtractor()
       step.start()
   ```
   Note: Standalone runs require proper AWS credentials, Redshift access, and Glue environment setup.

The class will extract DDLs, organize them into schema folders, create a ZIP archive, and upload everything to the specified S3 path.

