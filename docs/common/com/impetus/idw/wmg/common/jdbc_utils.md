# jdbc_utils.py

## File Overview

This module provides utilities for executing DML (Data Manipulation Language) queries against databases using JDBC
connections. It supports both jaydebeapi (preferred) and pyodbc (fallback) libraries and handles connection property
validation, query execution, and result retrieval.



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

