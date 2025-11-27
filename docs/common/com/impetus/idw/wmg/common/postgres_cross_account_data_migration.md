# PostgresCrossAccountDataMigration Class

## Overview
The `PostgresCrossAccountDataMigration` class is a Glue ELT step designed for migrating data from a source PostgreSQL database schema to a target PostgreSQL database schema across different AWS accounts. It automates the discovery of tables within the specified schema and handles data transfer using S3 as an intermediate staging area. The class inherits from `GlueELTStep` and relies on executor objects for database queries and AWS service interactions.

## What It Does
- Validates input parameters for source and target database secrets and schema name.
- Discovers all base tables in the specified source schema using PostgreSQL's information schema.
- For each table, exports data from the source database to a temporary S3 location using PostgreSQL's COPY command.
- Imports the staged data from S3 into the corresponding table in the target database.
- Cleans up temporary S3 files after successful migration.
- Logs progress and handles errors gracefully, ensuring data integrity across cross-account migrations.

## Arguments
The class expects the following job arguments, retrieved via `self.get_param_value`:

- `source_secret` (required): Name of the AWS Secrets Manager secret containing connection details for the source PostgreSQL database.
- `target_secret` (required): Name of the AWS Secrets Manager secret containing connection details for the target PostgreSQL database.
- `schema_name` (required): Name of the schema to migrate from the source to the target database.

## Usage
The `PostgresCrossAccountDataMigration` class is used in ETL workflows for seamless data migration between PostgreSQL databases in different AWS accounts. It simplifies cross-account data transfers by leveraging S3 staging, avoiding direct database-to-database connections. This is particularly useful for data consolidation, disaster recovery, or multi-account architectures where data needs to be replicated securely.

### Key Features
- **Automatic Table Discovery**: Dynamically identifies all tables in the source schema without manual specification.
- **S3 Staging**: Uses S3 as a secure intermediate storage for data transfer, supporting cross-account access.
- **Cross-Account Support**: Handles migrations between databases in different AWS accounts using secrets for authentication.
- **Error Handling and Logging**: Provides detailed logging and raises exceptions on failures for easy troubleshooting.
- **Cleanup Mechanism**: Automatically deletes temporary S3 files post-migration to maintain security and cost efficiency.

## Exceptions
- **Exception**: Raised if `source_secret` is not provided ("Need source secret name to get connection info...").
- **Exception**: Raised if `target_secret` is not provided ("Need target secret name to get connection info...").
- **Exception**: Raised if `schema_name` is not provided ("Need schema name which needs to be migrated...").
- **Exception**: Propagated from underlying operations, such as database query failures, S3 access issues, or COPY command errors.
- General exceptions may occur from executor methods if database connections fail or permissions are insufficient.

## What It Returns
- **bool**: Returns `True` if the migration completes successfully for all tables in the schema.

## How to Use It
1. Ensure AWS Secrets Manager contains secrets for both source and target PostgreSQL databases with appropriate connection details.
2. Configure the Glue job with the required parameters:
   - `source_secret`: Secret for source DB.
   - `target_secret`: Secret for target DB.
   - `schema_name`: Schema to migrate.
3. Instantiate `PostgresCrossAccountDataMigration` and run it via the Glue framework's executor.
4. For standalone testing (as in the module's `__main__`):
   ```python
   if __name__ == '__main__':
       step = PostgresCrossAccountDataMigration()
       step.start()
   ```
   Note: Standalone runs require proper AWS credentials and Glue environment setup.

The migration process will export and import data for each table, logging progress and cleaning up staging files upon completion.

---
