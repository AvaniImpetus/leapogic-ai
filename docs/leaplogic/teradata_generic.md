### Generated Reports and Outputs

**How LeapLogic Handles It:** LeapLogic generates comprehensive reports and consolidated outputs during the conversion process to provide visibility into the migration and ensure quality assurance.

**Validation Report for Tables:**
LeapLogic generates detailed validation reports for each table that includes:

- **Column Names**: Complete list of all columns in the table
- **Data Types**: Original Teradata data types and their Iceberg-compatible conversions
- **Constraints**: NOT NULL constraints, primary keys, foreign keys, and other table constraints with their Iceberg-compatible equivalents

This validation report helps developers verify that the schema conversion is accurate and that all data types and constraints are properly migrated to Iceberg-compatible formats.

**Schema Consolidation Output:**
LeapLogic produces consolidated outputs organized at the schema level. The consolidated output contains:

- **Schema-level Grouping**: All tables and views belonging to the same schema are converted and grouped together in a single file
- **Organized Structure**: Instead of individual files for each object, the conversion of all tables/views of the same schema is consolidated, making it easier to manage and deploy

**DDL Validation Reports:**
LeapLogic generates comprehensive DDL Validation Reports to ensure the accuracy and completeness of database object conversions. These reports are organized by schema and exported in Excel format for easy review and validation.

- **File Naming Convention**: Reports are named using the schema name followed by `.xlsx` (e.g., `AIWMCMNP1.xlsx`)
- **Table Validation (Spark & Redshift)**: For each table, the report includes validation details for both Spark and Redshift target platforms, covering DDL syntax, data type mappings, and constraint conversions
- **View Validation (Redshift)**: For views, the report provides Redshift-specific validation including view definitions, dependencies, and performance considerations

**DDL Metadata CSV Generation:**
LeapLogic creates detailed metadata CSV files for each DDL statement and subquery to provide comprehensive documentation and tracking of the conversion process.

- **Metadata Folder Location**: DDL metadata is stored in a dedicated `metadata` folder within the specified `<output-path>`
- **isDDLMetadataCSVRequired Flag**: This configuration flag controls whether DDL metadata CSV generation is enabled during the conversion process
- **CSV Files per DDL**: For each DDL statement processed, LeapLogic generates a corresponding CSV file containing metadata such as object type, conversion status, dependencies, and transformation details
- **Subquery Metadata**: Separate CSV files are created for subqueries within complex DDL statements, capturing their individual conversion metadata and relationship to parent DDL objects