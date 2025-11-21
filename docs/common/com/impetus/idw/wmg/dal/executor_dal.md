# ExecutorDAL Documentation

## Overview

The `executor_dal.py` file is a Data Access Layer (DAL) module that provides an abstract framework for executing
database queries and managing data operations. It serves as a base class for various database executors and includes
utilities for query execution, result handling, and database connectivity.

**Purpose**: This module abstracts database operations by providing:

- Abstract methods for query execution and data manipulation
- Query result wrapping with error handling
- JDBC-based database operations with connection management
- Query parsing and execution (including multi-query support with semicolon splitting)
- Parallel execution capabilities
- Table availability monitoring
- Session management

**Key Components**:

1. `QueryResult` - A class to encapsulate query results with metadata
2. `ExecutorDAL` - An abstract base class for database access operations

---

## Table of Methods

| Method Name              | Brief Description                                                                           |
|--------------------------|---------------------------------------------------------------------------------------------|
| `executeAndGetResultset` | Abstract method to execute a query and retrieve results (must be implemented by subclasses) |
| `executeQuery`           | Abstract method to execute a query (must be implemented by subclasses)                      |
| `executeParallel`        | Executes queries in parallel using thread pools                                             |
| `execute_using_jdbc`     | Executes queries via JDBC connection with automatic query type detection                    |
| `read_data_using_jdbc`   | Reads data from database using JDBC and returns results as QueryResult object               |
| `wait_for_input_tables`  | Waits for specified tables to become available before proceeding                            |

---

## Detailed Method Documentation

### ExecutorDAL Class

#### `executeAndGetResultset(self, query, activity_count_enable=False, query_id="")`

**Description**:
Abstract method that must be implemented by subclasses to execute a query and return the resultset. This is the primary
method for querying the database.

**Args**:

- `query` (str): The SQL query string to execute
- `activity_count_enable` (bool, optional): Flag to enable activity counting. Defaults to `False`
- `query_id` (str, optional): Unique identifier for the query (useful for logging/tracking). Defaults to `""`

**Returns**:

- Implementation-dependent, typically returns a QueryResult object or similar result set object

**Raises**:

- Must be implemented by subclasses
- Typical exceptions: DatabaseException, SQLError, ConnectionError

**Example**:

```python
class MySQLExecutor(ExecutorDAL):
    def executeAndGetResultset(self, query, activity_count_enable=False, query_id=""):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return QueryResult(result=results, resultCount=len(results), errorCode=0)
        except Exception as e:
            return QueryResult(errorCode=1, errorMessage=str(e))


# Usage
executor = MySQLExecutor()
result = executor.executeAndGetResultset("SELECT * FROM users", activity_count_enable=True, query_id="query_001")
```

**Notes**:

- Abstract method: no implementation in ExecutorDAL
- Subclasses must provide implementation
- The query_id helps in tracking and debugging queries

---

#### `executeQuery(self, *args, **kwargs)`

**Description**:
Abstract method that must be implemented by subclasses to execute SQL queries with flexible parameters.

**Args**:

- `*args`: Variable length argument list
- `**kwargs`: Arbitrary keyword arguments containing query execution parameters

**Returns**:

- Implementation-dependent; typically returns a QueryResult or similar object

**Example**:

```python
class HiveExecutor(ExecutorDAL):
    def executeQuery(self, *args, **kwargs):
        query = args[0] if args else kwargs.get('query')
        temporary_table = kwargs.get('temporary_table')
        # Execute query and handle results
        return QueryResult(resultCount=100)


# Usage
executor = HiveExecutor()
executor.executeQuery("SELECT * FROM table1", temporary_table='temp_1')
```

**Notes**:

- Abstract method: must be implemented by subclasses
- Flexible parameter structure allows various query types
- Often combined with other utility methods like _split_by_semicolon

---

#### `executeParallel(self, system=True, batch="ALL", threads=6)`

**Description**:
Executes queries in parallel using thread pools. Delegates to the execution_manager to handle parallel execution of
multiple queries.

**Args**:

- `system` (bool, optional): Flag indicating if system-level execution. Defaults to `True`
- `batch` (str, optional): Batch identifier or "ALL" to process all queries. Defaults to `"ALL"`
- `threads` (int, optional): Number of parallel threads to use. Defaults to `6`

**Returns**:

- Result of parallel execution (typically returns execution status or results list)

**Raises**:

- May raise threading-related exceptions or query execution exceptions

**Example**:

```python
# Assuming multiple queries were added to executor
executor = MySQLExecutor()
executor.queries = [
    "SELECT * FROM table1",
    "SELECT * FROM table2",
    "SELECT * FROM table3"
]

# Execute all queries in parallel using 4 threads
result = executor.executeParallel(system=True, batch="ALL", threads=4)

# Execute specific batch with default threads
result = executor.executeParallel(system=False, batch="batch_1", threads=8)
```

**Notes**:

- Improves performance for multiple independent queries
- Thread pool size can be tuned based on system resources
- Batch parameter allows selective execution
- Non-blocking execution for long-running queries

---

#### `execute_using_jdbc(self, query, *args, **kwargs)`

**Description**:
Executes queries using JDBC connection with automatic detection of query type (SELECT vs DML). For SELECT queries, loads
data into a temporary table. For DML queries, returns the count of affected rows.

**Args**:

- `query` (str): The SQL query to execute
- `*args`: Variable length argument list
- `**kwargs`: Arbitrary keyword arguments containing:
    - `dbType` (str): Database type (e.g., 'oracle', 'mysql', 'impala'). Case-insensitive
    - `temporary_table` (str, optional): Table name for storing SELECT results. Required for SELECT queries
    - Other JDBC-related parameters

**Returns**:

- For single query: `QueryResult` object with resultCount
- For multiple queries: `list` of QueryResult objects
- For SELECT queries: `QueryResult` with result data

**Raises**:

- `ELTJobException`: If SELECT query is executed without providing temporary_table name
- `botocore.exceptions.ClientError` or similar: If JDBC operations fail
- `UnicodeDecodeError`: If data cannot be decoded

**Example**:

```python
executor = JDBCExecutor()

# Example 1: Single DML query
result = executor.execute_using_jdbc(
    "INSERT INTO users VALUES (1, 'John')",
    dbType='mysql'
)
print(result.resultCount)  # Number of rows inserted

# Example 2: SELECT query with temporary table
result = executor.execute_using_jdbc(
    "SELECT * FROM users WHERE active=1",
    dbType='hive',
    temporary_table='temp_active_users'
)

# Example 3: Multiple queries (separated by semicolon)
results = executor.execute_using_jdbc(
    "INSERT INTO users VALUES (1, 'John'); INSERT INTO users VALUES (2, 'Jane'); SELECT COUNT(*) FROM users",
    dbType='oracle',
    temporary_table='user_count'
)
# Returns list of QueryResult objects

# Example 4: Impala SELECT (special case)
result = executor.execute_using_jdbc(
    "SELECT * FROM large_table LIMIT 1000",
    dbType='impala'
)
```

**Notes**:

- Automatically splits multi-query strings using _split_by_semicolon
- SELECT queries for non-Impala databases require temporary_table parameter
- Impala SELECT queries don't require temporary_table
- Retrieves connection properties from FrameworkConfigurations
- DML = Data Manipulation Language (INSERT, UPDATE, DELETE, etc.)
- DDL = Data Definition Language (CREATE, ALTER, DROP, etc.)

---

#### `read_data_using_jdbc(self, dbType, query, jdbc_properties, *args, **kwargs)`

**Description**:
Reads data from database using JDBC connection and returns results encapsulated in a QueryResult object. Used internally
for SELECT operations.

**Args**:

- `dbType` (str): Database type (e.g., 'mysql', 'oracle', 'impala')
- `query` (str): The SELECT query to execute
- `jdbc_properties` (dict): JDBC connection properties including driver, URL, credentials
- `*args`: Variable length argument list
- `**kwargs`: Arbitrary keyword arguments

**Returns**:

- `QueryResult`: Object containing:
    - `result`: List of rows/records retrieved
    - `resultCount`: Number of rows returned

**Raises**:

- Database connection exceptions: If connection fails
- SQL exceptions: If query execution fails
- Data encoding exceptions: If data cannot be properly decoded

**Example**:

```python
executor = JDBCExecutor()
jdbc_props = {
    'driver': 'com.mysql.jdbc.Driver',
    'url': 'jdbc:mysql://localhost:3306/mydb',
    'user': 'root',
    'password': 'secret'
}

result = executor.read_data_using_jdbc(
    dbType='mysql',
    query='SELECT * FROM users WHERE active=1',
    jdbc_properties=jdbc_props
)

print(f"Retrieved {result.resultCount} rows")
for row in result.result:
    print(row)
```

**Notes**:

- Calls jdbc.execute_dml internally
- Returns all results in memory (be cautious with large datasets)
- Result count is computed from result list length
- Used by execute_using_jdbc for SELECT operations

---
---

## Usage Guide for Beginners

### Basic Setup

```python
# Import the module
from com.impetus.idw.wmg.dal.executor_dal import ExecutorDAL, QueryResult

# Note: ExecutorDAL is abstract, so you typically use a subclass
# For example, if using MySQL:
from com.impetus.idw.wmg.dal.mysql_executor_dal import MySQLExecutorDAL

# Create an executor instance
executor = MySQLExecutorDAL()

# Initialize the executor (subclass-specific setup)
executor.initialize()
```

### Common Workflows

#### Workflow 1: Execute a Single Query

```python
# Create executor
executor = MySQLExecutorDAL()

# Execute a query
result = executor.executeAndGetResultset(
    query="SELECT * FROM users WHERE id = 1",
    activity_count_enable=True,
    query_id="query_001"
)

# Check results
if result.errorCode == 0:
    print(f"Success! Retrieved {result.resultCount} rows")
    print(result.result)
else:
    print(f"Error: {result.errorMessage}")
```

#### Workflow 2: Execute Multiple Queries in Batch

```python
# Create executor
executor = MySQLExecutorDAL()

# Execute multiple queries (separated by semicolons)
results = executor.execute_using_jdbc(
    query="""
        INSERT INTO users VALUES (1, 'John');
        INSERT INTO users VALUES (2, 'Jane');
        SELECT COUNT(*) FROM users;
    """,
    dbType='mysql'
)

# Handle multiple results
for i, result in enumerate(results):
    print(f"Query {i + 1}: {result.resultCount} rows affected")
```

#### Workflow 3: Execute Queries in Parallel

```python
# Create executor
executor = MySQLExecutorDAL()

# Add multiple queries
executor.queries = [
    "SELECT COUNT(*) FROM table1",
    "SELECT COUNT(*) FROM table2",
    "SELECT COUNT(*) FROM table3"
]

# Execute in parallel with 4 threads
results = executor.executeParallel(system=True, batch="ALL", threads=4)
print("All queries executed in parallel!")
```

### Tips and Best Practices

1. **Always close connections**: Call `closeConnection()` in a finally block
2. **Use activity tracking**: Enable `activity_count_enable=True` for important queries
3. **Provide meaningful query IDs**: Use descriptive query_id values for better debugging
4. **Check error codes**: Always validate `result.errorCode` before using results
5. **Handle timeouts**: Be cautious with `wait_for_input_tables` as it has no timeout
6. **Optimize thread count**: For `executeParallel`, match thread count to system cores
7. **Clean up on failure**: Implement `cleanupOnFailure` to drop temporary tables and reset state
8. **Respect quotes in SQL**: The `_split_by_semicolon` method handles quotes correctly, so multi-query scripts work as
   expected

---

## Error Handling

Common error codes and scenarios:

| Error Code | Meaning              | Solution                                  |
|------------|----------------------|-------------------------------------------|
| 0          | Success              | Proceed with using results                |
| 1001       | Connection timeout   | Check database connectivity               |
| 1002       | Invalid SQL          | Validate query syntax                     |
| 1003       | Table not found      | Verify table names and database           |
| -1         | Uninitialized result | Call executeQuery before checking results |

---

## Related Classes

- **ExecutionManager**: Handles parallel execution of queries
- **FrameworkBase**: Parent class providing base functionality
- **FrameworkConfigurations**: Manages configuration properties
- **ELTJobException**: Exception class for ETL job errors
- **jdbc_utils**: Low-level JDBC utility functions

---