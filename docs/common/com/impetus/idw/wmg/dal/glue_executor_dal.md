# GlueExecutorDAL Documentation

## Overview

The `glue_executor_dal.py` file is a comprehensive Data Access Layer (DAL) implementation for AWS Glue that extends the
base `ExecutorDAL` class. It provides integration with Apache Spark, JDBC databases, and AWS services (S3, Redshift,
Secrets Manager, etc.).

**Purpose**: This module serves as the primary executor for:

- Executing Spark SQL queries (both in-memory and distributed)
- Managing connections to external databases (Redshift, PostgreSQL, MySQL, Oracle, etc.)
- Reading and writing data in various formats (CSV, Parquet, XML, Fixed-Width, Excel)
- Handling data lineage and performance tracking
- Managing parallel query execution with thread pools
- Supporting AWS Glue-specific operations
- Managing database credentials from AWS Secrets Manager
- Supporting cross-account database connections

**Key Components**:

1. `GlueExecutorDAL` - Main class extending ExecutorDAL for AWS Glue operations
2. Various configuration managers for Spark, databases, and AWS resources
3. File I/O utilities for multiple formats
4. Connection pooling and credential management

---

## Table of Methods

| Method Name                  | Brief Description                                                          |
|------------------------------|----------------------------------------------------------------------------|
| `initialize`                 | Sets up Glue context, Spark configuration, and registers custom UDFs       |
| `executeQuery`               | Main method to execute SQL queries with various options and configurations |
| `writeFile`                  | Writes DataFrame to file in specified format                               |
| `write_single_file`          | Writes DataFrame to a single consolidated file with append/overwrite modes |
| `writeFileUsingPartitionBy`  | Writes partitioned data to files                                           |
| `unload_data_from_redshift`  | Unloads Redshift query results to S3 files                                 |
| `save_dataframe_to_redshift` | Saves Spark DataFrame to Redshift table                                    |
| `readFile`                   | Reads file from S3 or local storage in specified format                    |
| `readTextFromFile`           | Reads text file and returns as Spark DataFrame                             |
| `printRecords`               | Prints DataFrame records to console for debugging                          |
| `createDataframeWithSchema`  | Creates DataFrame from records with specified schema                       |
| `executeAndGetResultset`     | Executes query and returns result as list with count                       |
| `executeQueryWithCursor`     | Executes query with implicit cursor handling for CTEs                      |
| `execute`                    | Executes a query using Spark SQL                                           |
| `read_data_using_jdbc`       | Reads data from external database via JDBC                                 |
| `saveDataframeToTable`       | Saves DataFrame to table (Iceberg, Hive, or external database)             |
| `save_df_using_jdbc`         | Saves DataFrame to external database via JDBC                              |
| `executeProcedure`           | Executes stored procedure with input/output parameters                     |
| `loadFile`                   | Placeholder for loading files into database                                |
| `execute_ddl_query`          | Executes DDL query via JDBC connection                                     |
| `execute_store_procedure`    | Executes stored procedure with parameterized inputs/outputs                |
| `closeConnection`            | Closes Spark session and all JDBC connections                              |

---

## Detailed Method Documentation

### `initialize(self, *args, **kwargs)`

**Description**:
Sets up the AWS Glue context, Spark configuration, and registers custom User Defined Functions (UDFs). Called
automatically during instantiation if Spark session doesn't exist.

**Args**:

- `*args`: Variable length argument list
- `**kwargs`: Arbitrary keyword arguments containing:
    - `default_timezone` (str, optional): Timezone to set. Defaults to 'America/Chicago'
    - `max_workers` (int, optional): Number of worker threads. Defaults to 6
    - Any configuration keys starting with 'spark.' will be applied to Spark config

**Returns**:

- `None`

**Raises**:

- May raise exceptions if GlueContext initialization fails or UDF registration fails (non-blocking)

**Example**:

```python
executor = GlueExecutorDAL()

# The initialize method is called automatically, but can be called again
executor.initialize(
    default_timezone='America/New_York',
    max_workers=12,
    **{'spark.sql.shuffle.partitions': '200'}
)
```

**Notes**:

- Creates GlueContext from SparkContext
- Sets timezone for all Spark sessions
- Registers all custom UDFs defined in custom_udf module
- Failed UDF registrations are logged but don't halt execution
- SQL type mapping is initialized for stored procedure support

---

### `executeQuery(self, query, *args, **kwargs)`

**Description**:
Main method to execute SQL queries with flexible configuration options. Supports both Spark and JDBC databases, with
optional parallel execution, error handling, and activity counting.

**Args**:

- `query` (str): SQL query to execute
- `*args`: Positional arguments (legacy support):
    - `args[0]`: activity_count_enable (bool)
    - `args[1]`: in_memory (bool)
    - `args[2]`: is_error_handling_reqd (bool)
    - `args[3]`: query_id (str)
    - `args[4]`: is_parallel (bool)
    - `args[5]`: batch_id (str)
- `**kwargs`: Keyword arguments:
    - `activity_count_enable` (bool): Enable activity counting. Defaults to `False`
    - `in_memory` (bool): Load results in memory. Defaults to `False`
    - `handle_errors` (bool): Catch and return errors instead of raising. Defaults to `False`
    - `id` (str): Unique query identifier
    - `is_parallel` (bool): Execute in parallel thread. Defaults to `False`
    - `batch_id` (str): Batch identifier for grouping queries
    - `dbType` (str): Database type ('spark', 'redshift', 'mysql', 'oracle', etc.)
    - `temporary_table` (str): Table name to store results
    - `table_names` (str): Comma-separated table names for caching decisions
    - `configurations` (str): Comma-separated Spark configurations
    - `reset_configurations` (str): Configurations to reset after execution

**Returns**:

- `QueryResult`: Object with query results or error information
- For parallel execution with activity count disabled: returns 0

**Raises**:

- May raise `ELTJobException` if error handling is not enabled

**Example**:

```python
executor = GlueExecutorDAL()

# Simple SELECT query
result = executor.executeQuery(
    "SELECT * FROM users WHERE status='active'",
    id='q001',
    temporary_table='active_users'
)

# Query with activity counting and error handling
result = executor.executeQuery(
    "INSERT INTO summary SELECT * FROM staging",
    activity_count_enable=True,
    handle_errors=True,
    dbType='spark',
    id='insert_q001'
)

# Parallel execution
result = executor.executeQuery(
    "SELECT COUNT(*) FROM large_table",
    is_parallel=True,
    batch_id='batch_1'
)

# With Spark configurations
result = executor.executeQuery(
    "SELECT * FROM big_join",
    configurations='spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true',
    reset_configurations='spark.sql.adaptive.enabled=false'
)
```

**Notes**:

- Supports both positional and keyword arguments for backward compatibility
- Configurations can be applied and reset per query for optimization
- In-memory loading is controlled by performance profile configuration
- Query IDs are used for performance tracking and lineage

---

### `remove_comments_from_query(query)` [Static Method]

**Description**:
Removes single-line (`--`) and multi-line (`/* */`) comments from SQL queries while preserving comments within quoted
strings.

**Args**:

- `query` (str): SQL query potentially containing comments

**Returns**:

- `str`: Query with comments removed

**Example**:

```python
# Single-line comments
result = GlueExecutorDAL.remove_comments_from_query(
    "SELECT * FROM users -- this is a comment\nWHERE id > 0"
)
# Result: "SELECT * FROM users  \nWHERE id > 0"

# Multi-line comments
result = GlueExecutorDAL.remove_comments_from_query(
    "SELECT /* comment */ * FROM users"
)
# Result: "SELECT   * FROM users"

# Comments in strings are preserved
result = GlueExecutorDAL.remove_comments_from_query(
    "SELECT * FROM users WHERE note LIKE '%--semicolon%' -- real comment"
)
# Result: "SELECT * FROM users WHERE note LIKE '%--semicolon%' "
```

**Notes**:

- Handles both single (') and double (") quotes
- Converts `--` inside quotes to `__DASH__` temporarily to preserve them
- Uses regex with DOTALL flag for multi-line comments
- Essential for queries containing comment-like characters in string literals

---

### `readFile(self, file_type, file_path, output_table, options)`

**Description**:
Reads data from files in various formats and creates Spark DataFrame. Supports CSV, Parquet, XML, Excel, and Fixed-Width
formats.

**Args**:

- `file_type` (str): File format type. Valid values:
    - 'csv', 'parquet', 'text', 'json', 'orc'
    - 'excel', 'xlsx', 'xlx', 'xls'
    - 'xml'
    - 'fixed_width', 'fixedwidth', 'fwf'
- `file_path` (str): S3 or local file path (s3://bucket/path or local path)
- `output_table` (str, optional): Name to save as temporary view. If None, returns DataFrame only
- `options` (dict): Format-specific options:
    - For CSV: `delimiter`, `header`, `inferSchema`, `nullValue`, etc.
    - For Excel: `dataAddress`, `header`, etc.
    - For Fixed-Width: `schema` (list of tuples) - **REQUIRED**
    - For XML: `rootTag`, `rowTag`
    - For other formats: any Spark read options

**Returns**:

- `DataFrame`: Spark DataFrame containing the file data

**Raises**:

- `Exception`: If Fixed-Width file is read without providing schema
- File not found or format reading errors

**Example**:

```python
executor = GlueExecutorDAL()

# Read CSV file
df_csv = executor.readFile(
    file_type='csv',
    file_path='s3://my-bucket/data/users.csv',
    output_table='users_temp',
    options={'delimiter': ',', 'header': True, 'inferSchema': True}
)

# Read Parquet file
df_parquet = executor.readFile(
    file_type='parquet',
    file_path='s3://my-bucket/data/sales.parquet',
    output_table='sales_data'
)

# Read Fixed-Width file (schema required)
df_fwf = executor.readFile(
    file_type='fixed_width',
    file_path='s3://my-bucket/data/records.txt',
    output_table='fixed_records',
    options={
        'schema': [
            ('ID', 10),
            ('Name', 30),
            ('Amount', 12),
            ('Date', 10)
        ]
    }
)

# Read Excel file
df_excel = executor.readFile(
    file_type='excel',
    file_path='s3://my-bucket/data/report.xlsx',
    output_table='excel_data',
    options={'header': True, 'dataAddress': 'Sheet1!A1'}
)

# Read XML file
df_xml = executor.readFile(
    file_type='xml',
    file_path='s3://my-bucket/data/feed.xml',
    output_table='xml_data',
    options={'rootTag': 'root', 'rowTag': 'item'}
)
```

**Notes**:

- File path can be S3 (s3://bucket/key) or local/mounted path
- Options are format-specific and depend on Spark's data source
- Output table is registered as temporary view in Spark session
- Fixed-Width format requires explicit schema definition
- Excel and XML readers use specialized utility modules

---

### `writeFile(self, file_type, file_path, input_table, options)`

**Description**:
Writes DataFrame to file in specified format. Supports multiple file formats and write modes.

**Args**:

- `file_type` (str): Output file format. Valid values:
    - 'csv', 'parquet', 'text', 'json', 'orc'
    - 'fwf', 'fixedwidth', 'fixed_width'
- `file_path` (str): S3 or local output path
- `input_table` (str or DataFrame): Source table name or DataFrame
- `options` (dict): Format-specific options:
    - `mode` (str): 'overwrite', 'append', 'ignore', 'error'. Defaults to 'error'
    - `delimiter` (str): For CSV files. Defaults to ','
    - `header` (bool): Include header in output
    - `encoding` (str): Character encoding
    - For Fixed-Width: `schema` - **REQUIRED** as list of tuples (column_name, width)

**Returns**:

- `None`

**Raises**:

- Exception if Fixed-Width file is written without schema
- File system errors

**Example**:

```python
executor = GlueExecutorDAL()

# Write as CSV
executor.writeFile(
    file_type='csv',
    file_path='s3://output-bucket/users.csv',
    input_table='users_staging',
    options={'mode': 'overwrite', 'header': True, 'delimiter': ','}
)

# Write as Parquet
executor.writeFile(
    file_type='parquet',
    file_path='s3://output-bucket/sales/',
    input_table=sales_df,
    options={'mode': 'append', 'compression': 'snappy'}
)

# Write as Fixed-Width
executor.writeFile(
    file_type='fixed_width',
    file_path='s3://output-bucket/fixed_records.txt',
    input_table='records_table',
    options={
        'mode': 'overwrite',
        'schema': [('ID', 10), ('Name', 30), ('Amount', 12)]
    }
)
```

**Notes**:

- For Fixed-Width format, numeric width values pad/truncate to specified length
- Different from write_single_file which consolidates partitions
- Options vary by format type

---

### `write_single_file(self, file_type, file_path, input_table, temp_location=None, options=None, encoder='utf-8', use_spark_to_append=False)`

**Description**:
Writes DataFrame to a single consolidated file by coalescing partitions. Useful for small result sets that should be in
one file. Supports append and overwrite modes.

**Args**:

- `file_type` (str): Output file format
- `file_path` (str): Final output file path (S3)
- `input_table` (str or DataFrame): Source table or DataFrame
- `temp_location` (str or dict, optional): S3 path for temporary writes. If dict, treated as options
- `options` (dict, optional): Write options:
    - `mode` (str): 'append' or 'overwrite'. Defaults to 'append'
    - Other format-specific options
- `encoder` (str, optional): Character encoding. Defaults to 'utf-8'
- `use_spark_to_append` (bool, optional): Use Spark union for append instead of string concatenation. Defaults
  to `False`

**Returns**:

- `None`

**Raises**:

- `Exception`: If more than one part file is generated
- S3 or file system errors

**Example**:

```python
executor = GlueExecutorDAL()

# Write single CSV file with overwrite
executor.write_single_file(
    file_type='csv',
    file_path='s3://output-bucket/summary.csv',
    input_table='summary_table',
    temp_location='s3://output-bucket/temp/',
    options={'mode': 'overwrite', 'header': True},
    encoder='utf-8'
)

# Append to existing file
executor.write_single_file(
    file_type='text',
    file_path='s3://output-bucket/results.txt',
    input_table=results_df,
    options={'mode': 'append', 'header': False}
)

# Using Spark mode for append (for large files)
executor.write_single_file(
    file_type='parquet',
    file_path='s3://output-bucket/data.parquet',
    input_table='staging',
    options={'mode': 'append'},
    use_spark_to_append=True
)
```

**Notes**:

- Coalesces data to single partition before writing
- Creates temporary location if not provided
- Part files are renamed to final filename after generation
- Append mode preserves existing file and concatenates new data
- Checks for file existence before append operations

---

### `unload_data_from_redshift(self, sql_query, file_path, iam_role, **kwargs)`

**Description**:
Unloads query results from Redshift to S3 files using UNLOAD command. Optionally appends to existing files or saves in
specific formats.

**Args**:

- `sql_query` (str): Query to execute and unload results
- `file_path` (str): S3 path where files will be written (without file extension)
- `iam_role` (str): IAM role ARN for Redshift to access S3
- `**kwargs`: Additional options:
    - `dbType` (str): Database type. Defaults to 'redshift'
    - `mode` (str): 'append' or 'overwrite'. Defaults to 'append'
    - `delimiter` (str): Field delimiter for CSV. Defaults to ','
    - `header` (bool): Include column headers. Defaults to `False`
    - `file_type` (str): 'CSV' or other format. Defaults to 'CSV'
    - `fixedwidth` (str): Fixed-width format specification if FWF format
    - `enclosed` (bool): Quote enclosed fields in CSV
    - `parallel` (str): 'on' or 'off' for parallel unload. Defaults to 'off'
    - `generate_manifest` (bool): Create manifest file. Defaults to `True`
    - `options` (str): Additional Redshift unload options
    - `use_spark_to_append` (bool): Use Spark for append instead of string concat
    - `encoder` (str): Character encoding. Defaults to 'utf-8'
    - `secret` (str): Secret name for Redshift connection
    - `region` (str): AWS region

**Returns**:

- `None`

**Raises**:

- Redshift execution errors
- S3 access errors

**Example**:

```python
executor = GlueExecutorDAL()

# Basic unload to CSV
executor.unload_data_from_redshift(
    sql_query="SELECT * FROM sales WHERE year=2024",
    file_path="s3://data-bucket/sales_2024/data",
    iam_role="arn:aws:iam::123456789:role/RedshiftRole",
    dbType='redshift',
    secret='redshift-secret',
    mode='overwrite'
)

# Unload with headers and custom delimiter
executor.unload_data_from_redshift(
    sql_query="SELECT id, name, amount FROM customers",
    file_path="s3://data-bucket/customers/export",
    iam_role="arn:aws:iam::123456789:role/RedshiftRole",
    mode='overwrite',
    delimiter='|',
    header=True,
    parallel='on'
)

# Unload and append to existing file
executor.unload_data_from_redshift(
    sql_query="SELECT * FROM daily_logs",
    file_path="s3://logs-bucket/combined_logs",
    iam_role="arn:aws:iam::123456789:role/RedshiftRole",
    mode='append',
    use_spark_to_append=False
)
```

**Notes**:

- Uses Redshift UNLOAD command for efficient data extraction
- Manifest file contains list of unloaded files
- Parallel unload distributes data across multiple files for faster processing
- Append mode renames generated file to avoid duplicates
- Manifest file is deleted after processing

---

### `save_dataframe_to_redshift(self, input_df, target_table, temp_location, iam_role, **kwargs)`

**Description**:
Saves Spark DataFrame to Redshift table using either COPY command or spark-redshift connector.

**Args**:

- `input_df` (str or DataFrame): Source DataFrame or table name
- `target_table` (str): Target Redshift table name (schema.table format)
- `temp_location` (str): S3 path for temporary Parquet files
- `iam_role` (str): IAM role ARN for Redshift S3 access
- `**kwargs`: Additional options:
    - `mode` (str): 'append' or 'overwrite'. Defaults to 'append'
    - `use_spark_redshift_lib` (bool): Use spark-redshift library instead of COPY. Defaults to `False`
    - `url` (str): JDBC URL for direct connection (used with spark-redshift)
    - `load_properties` (str): Additional COPY command properties
    - Connection parameters (secret, dbType, etc.)

**Returns**:

- `None`

**Raises**:

- Redshift connection or execution errors
- S3 access errors

**Example**:

```python
executor = GlueExecutorDAL()

# Save DataFrame using COPY command
executor.save_dataframe_to_redshift(
    input_df='staging_df',
    target_table='public.sales_summary',
    temp_location='s3://temp-bucket/staging/',
    iam_role='arn:aws:iam::123456789:role/RedshiftRole',
    mode='overwrite',
    secret='redshift-secret'
)

# Save using spark-redshift connector
executor.save_dataframe_to_redshift(
    input_df=sales_df,
    target_table='analytics.transactions',
    temp_location='s3://temp-bucket/spark-staging/',
    iam_role='arn:aws:iam::123456789:role/RedshiftRole',
    use_spark_redshift_lib=True,
    url='jdbc:redshift://redshift.example.com:5439/analytics',
    mode='append'
)
```

**Notes**:

- Automatically creates table if doesn't exist
- Truncates table if overwrite mode and table exists
- Uses Parquet format for efficient intermediate storage
- Two methods available: COPY (default) and spark-redshift connector

---

### `read_data_using_jdbc(self, query, *args, **kwargs)`

**Description**:
Reads data from external databases (Redshift, PostgreSQL, MySQL, Oracle, etc.) using JDBC connection.

**Args**:

- `query` (str): SELECT query to execute
- `*args`: Variable length argument list (for compatibility)
- `**kwargs`: Arbitrary keyword arguments:
    - `dbType` (str): Database type ('redshift', 'mysql', 'oracle', 'postgresql', etc.)
    - `secret` (str): Secret name in AWS Secrets Manager
    - `connection_name` (str): Glue connection name
    - `connection_config` (dict): Connection configuration
    - `url` (str): JDBC URL
    - `user` (str): Database username
    - `password` (str): Database password
    - `options` (dict): JDBC read options
    - `region` (str): AWS region

**Returns**:

- `DataFrame`: Spark DataFrame with query results

**Raises**:

- Connection errors if credentials missing or invalid
- Database or query errors

**Example**:

```python
executor = GlueExecutorDAL()

# Read from Redshift using secret
df_redshift = executor.read_data_using_jdbc(
    "SELECT * FROM public.customers WHERE status='active'",
    dbType='redshift',
    secret='redshift-secret'
)

# Read from PostgreSQL using connection name
df_postgres = executor.read_data_using_jdbc(
    "SELECT * FROM users WHERE created_date > '2024-01-01'",
    dbType='postgresql',
    connection_name='prod-postgres'
)

# Read from MySQL with explicit connection config
df_mysql = executor.read_data_using_jdbc(
    "SELECT * FROM products",
    dbType='mysql',
    connection_config={
        'url': 'jdbc:mysql://localhost:3306/ecommerce',
        'user': 'data_user',
        'password': 'secure_password'
    }
)
```

**Notes**:

- Automatically retrieves connection details from Secrets Manager, Glue connections, or provided config
- Supports cross-account connections with IAM roles
- PostgreSQL automatically generates temporary auth tokens if password not provided
- Returns full DataFrame in memory (be cautious with large result sets)

---

### `execute_ddl_query(self, query, **kwargs)`

**Description**:
Executes DDL/DML queries against JDBC databases (DDL = CREATE, ALTER, DROP; DML = INSERT, UPDATE, DELETE). Manages
connection pooling and timezone settings.

**Args**:

- `query` (str): DDL or DML query to execute
- `**kwargs`: Arbitrary keyword arguments:
    - `dbType` (str): Database type
    - `secret` (str): Secret name
    - `connection_name` (str): Glue connection name
    - Connection parameters
    - Other database-specific parameters

**Returns**:

- `list`: List of dictionaries if query returns results (SELECT within procedure), `None` otherwise

**Raises**:

- Database execution errors
- Connection errors

**Example**:

```python
executor = GlueExecutorDAL()

# Create table in Redshift
result = executor.execute_ddl_query(
    """CREATE TABLE public.staging_data (
        id INT,
        name VARCHAR(100),
        amount DECIMAL(10,2)
    )""",
    dbType='redshift',
    secret='redshift-secret'
)

# Insert data
executor.execute_ddl_query(
    "INSERT INTO public.summary SELECT * FROM staging",
    dbType='redshift',
    secret='redshift-secret'
)

# Drop table
executor.execute_ddl_query(
    "DROP TABLE IF EXISTS temp_table",
    dbType='postgresql',
    connection_name='prod-postgres'
)
```

**Notes**:

- Manages connection pool internally
- Sets timezone for Redshift on first connection
- Caches connections by database type and secret
- Results are converted to list of dictionaries for easy access

---

### `execute_store_procedure(self, proc_name, inputs: list, outputs: list, **kwargs)`

**Description**:
Executes stored procedures on external databases with parameterized input and output parameters.

**Args**:

- `proc_name` (str): Stored procedure name (with schema if needed)
- `inputs` (list): List of input parameter values (strings)
- `outputs` (list): List of output parameter types (e.g., ['varchar', 'int', 'date'])
- `**kwargs`: Database connection parameters:
    - `dbType` (str): Database type
    - `secret` (str): Secret name
    - Connection parameters

**Returns**:

- `list`: Results from output parameters as list of strings

**Raises**:

- Procedure execution errors
- Connection errors

**Example**:

```python
executor = GlueExecutorDAL()

# Call stored procedure with inputs and outputs
result = executor.execute_store_procedure(
    proc_name='public.validate_customer',
    inputs=['cust_123', 'john@example.com'],
    outputs=['varchar', 'int'],  # return message, error code
    dbType='postgresql',
    secret='postgres-secret'
)

# result = ['Customer validated successfully', '0']

# Call another procedure
result = executor.execute_store_procedure(
    proc_name='sp_GenerateReport',
    inputs=['2024-01-01', '2024-12-31'],
    outputs=['varchar', 'timestamp'],
    dbType='redshift',
    connection_name='redshift-conn'
)
```

**Notes**:

- All input parameters are treated as strings
- Output parameter types must match those defined in SQL types mapping
- Procedure name should include schema prefix if applicable
- Useful for complex business logic encapsulated in procedures

---

### `executeAndGetResultset(self, query, activity_count_enable=False, **kwargs)`

**Description**:
Executes a query and returns results as a list with row count. Thread-safe execution with configuration management.

**Args**:

- `query` (str): SQL query to execute
- `activity_count_enable` (bool, optional): Track affected rows. Defaults to `False`
- `**kwargs`: Additional parameters passed to _safe_df_execute

**Returns**:

- `QueryResult`: Contains:
    - `result`: List of Row objects
    - `resultCount`: Number of rows returned

**Raises**:

- `ELTJobException`: If query execution fails

**Example**:

```python
executor = GlueExecutorDAL()

# Execute simple query
result = executor.executeAndGetResultset("SELECT * FROM users LIMIT 100")
print(f"Retrieved {result.resultCount} rows")
for row in result.result:
    print(row)

# With activity counting
result = executor.executeAndGetResultset(
    "SELECT * FROM large_table WHERE status='active'",
    activity_count_enable=True
)
```

**Notes**:

- Results are returned as complete list (not lazy evaluation)
- Thread-safe through lock management
- Proper for smaller result sets that fit in memory

---

### `saveDataframeToTable(self, input_df, schema, table, options, **kwargs)`

**Description**:
Saves DataFrame to table supporting multiple formats (Iceberg, Hive, external databases via JDBC).

**Args**:

- `input_df` (str or DataFrame): Source table name or DataFrame
- `schema` (str): Schema name (database name)
- `table` (str): Table name
- `options` (dict): Write options:
    - `format` (str): 'iceberg', 'hive', or other. Defaults to 'hive'
    - `mode` (str): 'error', 'append', 'overwrite', 'ignore'
    - `partitionBy` (str or list): Columns to partition by
- `**kwargs`: Database parameters:
    - `dbType` (str): If provided, saves to external database via JDBC

**Returns**:

- `None`

**Raises**:

- Table creation or write errors

**Example**:

```python
executor = GlueExecutorDAL()

# Save as Iceberg table
executor.saveDataframeToTable(
    input_df='staging_df',
    schema='analytics',
    table='customers',
    options={
        'format': 'iceberg',
        'mode': 'overwrite',
        'partitionBy': 'region'
    }
)

# Save as Hive table
executor.saveDataframeToTable(
    input_df=sales_df,
    schema='default',
    table='transactions',
    options={
        'format': 'hive',
        'mode': 'append',
        'partitionBy': ['year', 'month']
    }
)

# Save to Redshift via JDBC
executor.saveDataframeToTable(
    input_df='local_staging',
    schema='public',
    table='redshift_table',
    options={'mode': 'overwrite'},
    dbType='redshift',
    secret='redshift-secret'
)
```

**Notes**:

- Supports Iceberg table versioning and ACID properties
- Can handle external database writes
- Partitioning improves query performance on large tables
- Automatic table creation based on data

---

### Basic Setup and Configuration

```python
# Initialize GlueExecutorDAL
executor = GlueExecutorDAL(
    s3_temp_location='s3://my-data-bucket/temp/',
    redshift_iam_role='arn:aws:iam::123456789:role/GlueRedshiftRole',
    enableDebug='YES'  # For detailed logging
)

# Get Spark session for advanced operations
spark = executor.getSparkSession()
```

### Common Workflows

#### Workflow 1: Execute Spark SQL Query

```python
# Execute SELECT query
result = executor.executeQuery(
    "SELECT id, name, amount FROM sales WHERE status='completed'",
    id='sales_q001',
    temporary_table='completed_sales'
)

# Check results
if result.resultObject:
    df = result.resultObject
    print(f"Retrieved {df.count()} rows")
    df.show(10)
```

#### Workflow 2: Read CSV from S3 and Process

```python
# Read CSV file
df = executor.readFile(
    file_type='csv',
    file_path='s3://data-bucket/input/customers.csv',
    output_table='customers',
    options={'delimiter': ',', 'header': True, 'inferSchema': True}
)

# Execute transformation query
result = executor.executeQuery(
    """SELECT 
        id,
        name,
        UPPER(city) as city,
        CAST(amount as DECIMAL(10,2)) as amount
    FROM customers
    WHERE amount > 100
    """,
    temporary_table='customers_transformed'
)

# Write results back to S3
executor.write_single_file(
    file_type='csv',
    file_path='s3://data-bucket/output/customers_processed.csv',
    input_table='customers_transformed',
    options={'mode': 'overwrite', 'header': True}
)
```

#### Workflow 3: Load Data from Redshift

```python
# Read from Redshift
df_redshift = executor.read_data_using_jdbc(
    "SELECT * FROM public.sales WHERE year=2024",
    dbType='redshift',
    secret='redshift-prod-secret'
)

# Save as Spark table
df_redshift.createOrReplaceTempView('sales_2024')

# Run local Spark transformation
result = executor.executeQuery(
    """SELECT 
        region,
        SUM(amount) as total_sales,
        COUNT(*) as num_transactions
    FROM sales_2024
    GROUP BY region
    """,
    temporary_table='sales_summary'
)

# Write summary back to Redshift
executor.save_dataframe_to_redshift(
    input_df='sales_summary',
    target_table='public.sales_summary_2024',
    temp_location='s3://temp-bucket/staging/',
    iam_role='arn:aws:iam::123456789:role/GlueRedshiftRole',
    mode='overwrite'
)
```

#### Workflow 4: Parallel Query Execution

```python
# Execute multiple queries in parallel
executor.executeQuery(
    "INSERT INTO summary SELECT * FROM staging_1",
    is_parallel=True,
    batch_id='batch_1',
    id='insert_1'
)

executor.executeQuery(
    "INSERT INTO summary SELECT * FROM staging_2",
    is_parallel=True,
    batch_id='batch_1',
    id='insert_2'
)

executor.executeQuery(
    "INSERT INTO summary SELECT * FROM staging_3",
    is_parallel=True,
    batch_id='batch_1',
    id='insert_3'
)

# All three queries execute in parallel
```

#### Workflow 5: Fixed-Width File Processing

```python
# Read fixed-width file
df_fwf = executor.readFile(
    file_type='fixed_width',
    file_path='s3://data-bucket/records.txt',
    output_table='fixed_records',
    options={
        'schema': [
            ('RecordID', 10),
            ('FirstName', 20),
            ('LastName', 20),
            ('Amount', 12),
            ('Date', 10)
        ]
    }
)

# Write as fixed-width
executor.writeFile(
    file_type='fixed_width',
    file_path='s3://output-bucket/formatted_records.txt',
    input_table='fixed_records',
    options={
        'mode': 'overwrite',
        'schema': [
            ('RecordID', 10),
            ('FirstName', 20),
            ('LastName', 20),
            ('Amount', 12),
            ('Date', 10)
        ]
    }
)
```

#### Workflow 6: Data from Multiple Sources

```python
# Read from PostgreSQL
df_postgres = executor.read_data_using_jdbc(
    "SELECT * FROM public.customers",
    dbType='postgresql',
    connection_name='prod-postgres'
)
df_postgres.createOrReplaceTempView('pg_customers')

# Read from MySQL
df_mysql = executor.read_data_using_jdbc(
    "SELECT * FROM ecommerce.orders",
    dbType='mysql',
    secret='mysql-secret'
)
df_mysql.createOrReplaceTempView('mysql_orders')

# Join and aggregate
result = executor.executeQuery(
    """SELECT 
        c.customer_id,
        c.name,
        COUNT(o.order_id) as num_orders,
        SUM(o.amount) as total_spent
    FROM pg_customers c
    LEFT JOIN mysql_orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.name
    """,
    temporary_table='customer_summary'
)
```

### Error Handling and Debugging

```python
# Execute with error handling
result = executor.executeQuery(
    "SELECT * FROM potentially_missing_table",
    handle_errors=True
)

if result.errorCode != 0:
    print(f"Error occurred: {result.errorMessage}")
else:
    print(f"Successfully retrieved data")

# Enable debug mode for detailed logging
executor.enable_debug = True

# View executed queries
for query in executor.executed_queries:
    print(f"Executed: {query}")
```

### Best Practices

1. **Always close connections**: Call `executor.closeConnection()` in finally blocks
2. **Use temporary tables**: Store intermediate results as temp views for reusability
3. **Enable activity counting**: For important data movement operations to track volumes
4. **Leverage parallelism**: Use `is_parallel=True` for independent queries
5. **Cache large tables**: Use `persist()` and configuration caching for repeated access
6. **Handle credentials securely**: Use AWS Secrets Manager instead of hardcoding
7. **Monitor performance**: Use performance configuration and query profiling
8. **Test with smaller data**: Use LIMIT in queries during development

---

## Error Handling

Common error scenarios and solutions:

| Error                    | Cause                            | Solution                                              |
|--------------------------|----------------------------------|-------------------------------------------------------|
| "Table not found"        | Reference to non-existent table  | Verify table exists and schema is correct             |
| Connection timeout       | Database unreachable             | Check network, security groups, host availability     |
| Authentication failed    | Invalid credentials              | Verify secret in Secrets Manager or connection config |
| Auth token expired       | PostgreSQL RDS IAM token too old | Token auto-refreshes; retry operation                 |
| S3 access denied         | Missing IAM permissions          | Verify IAM role has S3 access                         |
| Fixed-Width schema error | Schema not provided or malformed | Provide schema as list of (name, width) tuples        |

---

## Related Classes and Utilities

- **ExecutorDAL**: Parent abstract class providing base DAL functionality
- **ExecutionManager**: Manages parallel task execution
- **FrameworkConfigurations**: Centralized configuration management
- **glue_utils**: AWS Glue-specific utility functions
- **s3_utils**: S3 file operations
- **aws_utils**: General AWS service utilities
- **csv_utility, excel_utility, xml_utility, fixed_width_utility**: Format-specific utilities
- **custom_udf**: User-defined functions module

---

## Appendix: Connection Methods

### Method 1: AWS Secrets Manager (Recommended)

```python
result = executor.executeQuery(
    query="SELECT * FROM table",
    dbType='postgresql',
    secret='prod-postgres-secret'
)
```

### Method 2: Glue Connection

```python
result = executor.executeQuery(
    query="SELECT * FROM table",
    dbType='redshift',
    connection_name='redshift-prod'
)
```

### Method 3: Explicit Configuration

```python
result = executor.executeQuery(
    query="SELECT * FROM table",
    dbType='mysql',
    connection_config={
        'url': 'jdbc:mysql://localhost:3306/mydb',
        'user': 'admin',
        'password': 'password123'
    }
)
```

### Method 4: Attached Connection

```python
# Automatically uses attached connection if available
result = executor.executeQuery(
    query="SELECT * FROM table",
    dbType='postgresql'
)
```

