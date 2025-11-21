# jdbc_utils.py

## File Overview

This module provides utilities for executing DML (Data Manipulation Language) queries against databases using JDBC
connections. It supports both jaydebeapi (preferred) and pyodbc (fallback) libraries and handles connection property
validation, query execution, and result retrieval.

## Function Index

1. `get_connection_properties()`
2. `validate_jdbc_connection_properties()`
3. `execute_dml()`
4. `execute_dml_using_jaydebeapi()`
5. `execute_dml_using_pyodbc()`

---

## Functions

### get_connection_properties()

**Purpose:**  
Extracts JDBC connection properties (driver, URL, user, password, JAR files) from the execution properties dictionary
based on database type.

**Args:**

- `dbType` (str): Database type key (e.g., `'teradata'`, `'postgresql'`). Case-insensitive.
- `execution_properties` (dict): Dictionary containing properties keyed as `{dbType}.jdbc.driver`, `{dbType}.jdbc.url`,
  etc.

**Returns:**

- `dict`: Dictionary with keys:
    - `'driver'`: JDBC driver class name.
    - `'url'`: JDBC connection URL.
    - `'user'`: Database user.
    - `'password'`: Database password.
    - `'jars'`: Comma-separated JAR file paths.

**Raises:**

- None; returns dict with `None` values if properties not found.

**Example:**

```python
props = get_connection_properties('postgresql', execution_properties)
# Returns: {'driver': 'org.postgresql.Driver', 'url': 'jdbc:postgresql://...', ...}
```

---

### validate_jdbc_connection_properties()

**Purpose:**  
Validates that all required JDBC connection properties (driver, URL, user, password) are present and non-empty.

**Args:**

- `driver` (str): JDBC driver class name.
- `url` (str): JDBC connection URL.
- `user` (str): Database user.
- `password` (str): Database password.

**Returns:**

- None (validation passed) or raises Exception.

**Raises:**

- `Exception`: If any required property is `None` or empty string. Error message specifies which property is missing.

**Example:**

```python
validate_jdbc_connection_properties('org.postgresql.Driver', 'jdbc:postgresql://host:5432/db', 'user', 'pass')
# No exception raised
```

---

### execute_dml()

**Purpose:**  
Executes a DML query against a database using available JDBC libraries (jaydebeapi preferred, pyodbc fallback). Returns
rowcount for INSERT/UPDATE/DELETE and fetched rows for SELECT.

**Args:**

- `query` (str): SQL DML statement to execute (SELECT, INSERT, UPDATE, DELETE).
- `jdbc_properties` (dict): Dictionary with keys `'driver'`, `'url'`, `'user'`, `'password'`, `'jars'`.
- `*args`: Additional positional arguments (passed to underlying implementation).
- `**kwargs`: Additional keyword arguments (passed to underlying implementation).

**Returns:**

- For SELECT: List of tuples (rows).
- For INSERT/UPDATE/DELETE: Integer row count.

**Raises:**

- `Exception`: If neither jaydebeapi nor pyodbc is available, or if query execution fails.

**Example:**

```python
result = execute_dml('SELECT * FROM users', jdbc_properties)
# Returns: [(1, 'Alice'), (2, 'Bob')]
```

**Notes:**

- Tries jaydebeapi first; if not available, falls back to pyodbc.
- Logs query execution at INFO level.

---

### execute_dml_using_jaydebeapi()

**Purpose:**  
Executes a DML query using the jaydebeapi library (for JDBC connections).

**Args:**

- `query` (str): SQL statement to execute.
- `jdbc_properties` (dict): JDBC connection properties (driver, url, user, password, jars).
- `*args`: Additional arguments.
- `**kwargs`: Additional keyword arguments.

**Returns:**

- For SELECT: List of tuples (rows).
- For INSERT/UPDATE/DELETE: Integer row count.

**Raises:**

- `Exception`: If connection fails or query execution fails. Logs error before raising.

**Example:**

```python
props = {'driver': 'org.postgresql.Driver', 'url': '...', 'user': '...', 'password': '...', 'jars': '/path/to/jar'}
result = execute_dml_using_jaydebeapi('SELECT count(*) FROM table1', props)
```

---

### execute_dml_using_pyodbc()

**Purpose:**  
Executes a DML query using the pyodbc library (ODBC connections as fallback).

**Args:**

- `query` (str): SQL statement to execute.
- `*args`: Additional arguments.
- `**kwargs`: Keyword arguments including `'dbType'` for looking up ODBC properties.

**Returns:**

- For SELECT: List of tuples (rows).
- For INSERT/UPDATE/DELETE: Integer row count.

**Raises:**

- `Exception`: If required ODBC properties are missing or query execution fails.

**Example:**

```python
result = execute_dml_using_pyodbc('SELECT * FROM table1', dbType='teradata')
```

**Notes:**

- Retrieves ODBC properties from `FrameworkConfigurations.getConfiguration("execution_properties_dict")`.
- Constructs ODBC connection string as: `DRIVER={driver};DBCNAME={host};UID={user};PWD={password}`.
- Requires `{dbType}.odbc.driver`, `{dbType}.odbc.host`, `{dbType}.odbc.user`, `{dbType}.odbc.password` properties.

---