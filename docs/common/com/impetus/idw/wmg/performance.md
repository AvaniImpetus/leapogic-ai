# performance.py

## Overview

This module manages Spark table caching, performance optimization, and configuration management for the ELT framework.
It provides functionality to:

- **Cache and uncache tables** based on configurable size thresholds to improve query performance
- **Load and manage CSV configurations** that define table details and query-specific Spark settings
- **Apply query-level Spark optimizations** including configuration settings and in-memory table handling
- **Store and retrieve execution properties** that control caching behavior, thresholds, and file paths

The module uses several configuration files (CSV and property files) to define caching strategies and Spark optimization
parameters. Tables are cached in-memory when their size is below a specified threshold, reducing disk I/O and improving
query execution speed.

---

## Classes

### TableDetails

**Purpose:**

A data container that stores query-specific or global Spark configuration and caching parameters. This class
encapsulates settings related to table caching, in-memory computation, and Spark optimization properties.

**Constructor:**

```python
def __init__(self, id, spark_configuration, caching_threshold, in_memory, cache_tables, uncache_tables):
```

- Args:
    - `id` (str): Unique identifier for this configuration (typically a query ID)
    - `spark_configuration` (str): Pipe-delimited string of Spark config key=value pairs (e.g., "
      spark.sql.adaptive.enabled=true|spark.sql.shuffle.partitions=200")
    - `caching_threshold` (str/int): Size threshold (in MB) below which tables should be cached
    - `in_memory` (str): String representation of boolean ("true"/"false") indicating whether to use in-memory caching
    - `cache_tables` (str): Pipe-delimited list of table names to cache before query execution
    - `uncache_tables` (str): Pipe-delimited list of table names to uncache after query execution

**Methods:**

#### get_id()

**Purpose:** Retrieve the unique identifier for this configuration.

**Args:** None

**Returns:** str: The configuration ID

**Example:**

```python
config = TableDetails(
    id="query_001",
    spark_configuration="spark.sql.adaptive.enabled=true",
    caching_threshold="100",
    in_memory="true",
    cache_tables="users|products",
    uncache_tables="temp_table"
)
config_id = config.get_id()  # Returns "query_001"
```

---

#### get_spark_configuration()

**Purpose:** Retrieve the Spark configuration string for this query.

**Args:** None

**Returns:** str: Pipe-delimited Spark configuration settings

**Example:**

```python
spark_config = config.get_spark_configuration()
# Returns: "spark.sql.adaptive.enabled=true|spark.sql.shuffle.partitions=200"
```

---

#### get_caching_threshold()

**Purpose:** Retrieve the table size threshold (in MB) used to determine which tables should be cached.

**Args:** None

**Returns:** str/int: The size threshold value

**Example:**

```python
threshold = config.get_caching_threshold()  # Returns "100" or 100
```

---

#### get_in_memory()

**Purpose:** Retrieve the in-memory caching flag for this configuration.

**Args:** None

**Returns:** str: String representation of boolean ("true" or "false")

**Example:**

```python
in_mem = config.get_in_memory()  # Returns "true" or "false"
```

---

#### get_cache_tables()

**Purpose:** Retrieve the list of tables to cache before query execution.

**Args:** None

**Returns:** str: Pipe-delimited list of table names to cache

**Example:**

```python
tables = config.get_cache_tables()  # Returns "users|products|orders"
```

---

#### get_uncache_tables()

**Purpose:** Retrieve the list of tables to uncache after query execution.

**Args:** None

**Returns:** str: Pipe-delimited list of table names to uncache

**Example:**

```python
uncache = config.get_uncache_tables()  # Returns "temp_table|staging"
```

---

## Module-Level Functions

### load_property(path)

**Purpose:** Load application configuration properties from a text file. This function reads key-value pairs from a
Java-style properties file and stores them in the global `props` dictionary for use throughout the module.

**Args:**

- `path` (str): Absolute file path to the properties file

**Returns:** None

**Raises**

- `FileNotFoundError`: If the specified file does not exist
- `IOError`: If the file cannot be read due to permissions or other I/O errors

**Example**

```python
# Properties file content (executor.properties):
# table.size.threshold=100
# tables.csv.location=/path/to/tables.csv
# spark.config.csv.location=/path/to/spark_config.csv
# spark.optimization.property.location=/path/to/spark_optimizations.properties

load_property("/path/to/executor.properties")
# Now props dictionary contains all key-value pairs
# props['table.size.threshold'] = '100'
# props['tables.csv.location'] = '/path/to/tables.csv'
```

**Notes**

- Empty lines and comments (lines starting with `#`) are ignored
- Key names are converted to lowercase internally
- Format: `key=value` (one per line)
- A single `=` character is used to split key and value; if the value contains `=`, it is preserved
- The global `props` dictionary is updated with each key-value pair
- Call this function early in your application initialization

---

### load_tables_csv()

**Purpose:** Load table metadata (including table names and their sizes in MB) from a CSV file. This populates the
global `table_details` dictionary for use in caching decisions.

**Args:** None

**Returns:** None

**Raises**

- No exceptions are raised; errors are logged to the logger instead

**Example:**

```python
# CSV file format (tables.csv):
# Table,Size(in MB)
# users,50
# products,80
# orders,150
# customers,45

load_tables_csv()
# Now table_details contains:
# table_details = {
#     'users': '50',
#     'products': '80',
#     'orders': '150',
#     'customers': '45'
# }
```

**Notes:**

- Requires `props["tables.csv.location"]` to be set (via `load_property()`)
- File path is checked for existence before reading
- Table names are stored in lowercase for consistent lookups
- File is expected to have UTF-8-sig encoding (UTF-8 with optional BOM)
- CSV must have a header row with columns "Table" and "Size(in MB)"
- If file does not exist or is not found in props, function completes silently
- Any exceptions during parsing are caught and logged; the function does not raise

---

### load_spark_configuration_csv()

**Purpose:** Load query-specific Spark configuration settings from a CSV file. This creates `TableDetails` objects for
each query and stores them in the global `spark_config_details` dictionary.

**Args:** None

**Returns:** None

**Raises:** No exceptions are raised; errors are logged instead

**Example**

```python
# CSV file format (spark_config.csv):
# id,spark_configuration,caching_threshold,in_memory,cache_tables_before_query,uncache_tables_after_query
# query_001,spark.sql.adaptive.enabled=true|spark.sql.shuffle.partitions=200,100,true,users|products,temp_table
# query_002,spark.sql.adaptive.enabled=false,150,false,orders,staging

load_spark_configuration_csv()
# Now spark_config_details contains TableDetails objects:
# spark_config_details['query_001'] = TableDetails(...)
# spark_config_details['query_002'] = TableDetails(...)
```

**Notes**

- Requires `props["spark.config.csv.location"]` to be set
- File path is checked for existence before reading
- File is expected to have UTF-8-sig encoding
- CSV header row must contain columns: id, spark_configuration, caching_threshold, in_memory, cache_tables_before_query,
  uncache_tables_after_query
- Each row creates a TableDetails object that is stored with its `id` as the key
- If the file does not exist or path is not in props, function completes silently
- Any parsing exceptions are caught and logged; the function does not raise

---

### get_threshold_value(query_spark_config)

**Purpose:** Retrieve the effective table size threshold (in MB) used to determine whether a table should be cached. It
prioritizes query-specific thresholds over global defaults.

**Args**

- `query_spark_config` (TableDetails or None): A TableDetails object for a specific query, or None

**Returns**

int: The threshold value in MB (e.g., 100, 150)

**Raises**

- `ValueError`: If the threshold string cannot be converted to an integer
- `KeyError`: If the global default threshold is not found in `props`

**Example**

```python
# Scenario 1: Query-specific threshold
config = TableDetails(..., caching_threshold="120", ...)
threshold = get_threshold_value(config)  # Returns 120

# Scenario 2: Use global default
config = TableDetails(..., caching_threshold="", ...)  # Empty threshold
# Assumes props["table.size.threshold"] = "100"
threshold = get_threshold_value(config)  # Returns 100

# Scenario 3: No config provided
threshold = get_threshold_value(None)  # Returns global default from props
```

**Notes**

- Query-specific threshold takes precedence over global threshold
- If query_spark_config is None or has empty threshold, falls back to `props["table.size.threshold"]`
- The threshold is used by `tablewise_cache()` to decide if a table is small enough to cache
- Threshold should be set before calling this function (via `load_property()`)

---

### tablewise_cache(table_names, query_spark_config)

**Purpose**

Determine which tables from a provided list should be cached based on their size and the configured threshold. Returns a
list of Spark cache commands for tables smaller than the threshold.

**Args**

- `table_names` (str): Comma-separated list of table names to evaluate (e.g., "users, products, orders")
- `query_spark_config` (TableDetails or None): Query-specific configuration object, or None to use global threshold

**Returns**

list: List of Spark cache command strings,
e.g., ["spark.catalog.cacheTable('users')", "spark.catalog.cacheTable('products')"]

**Raises**

No exceptions are explicitly raised; errors during threshold retrieval may propagate

**Example**

```python
# Assume:
# table_details = {'users': '50', 'products': '80', 'orders': '150'}
# Threshold = 100

table_list = "users, products, orders"
commands = tablewise_cache(table_list, None)
# Returns: ["spark.catalog.cacheTable('users')", "spark.catalog.cacheTable('products')"]
# orders is not included because 150 > 100 threshold
```

**Notes**

- Table names are normalized to lowercase for comparison with table_details keys
- Whitespace around table names is stripped
- If a table name is not found in `table_details`, it is skipped
- Tables with size <= threshold are included in the cache commands
- If table_names is empty or None, returns an empty list
- Useful as a fallback when no explicit cache table list is provided in configuration

---

### get_cache_tables(id, table_names)

**Purpose**

Get a list of Spark cache commands for tables that should be cached before executing a specific query. Prioritizes
query-specific cache table list from CSV; if not defined, uses `tablewise_cache()` with the table size threshold.

**Args**

- `id` (str): Query or configuration ID (e.g., "query_001")
- `table_names` (str): Comma-separated list of table names to consider if query-specific config is not available

**Returns**

list: List of Spark cache command strings ready to be executed

**Raises**

No explicit exceptions; errors may propagate from `tablewise_cache()`

**Example**

```python
# Scenario 1: Query-specific cache tables defined
# Assume spark_config_details['query_001'].get_cache_tables() = "users|products"
commands = get_cache_tables("query_001", "users, products, orders")
# Returns: ["spark.catalog.cacheTable('users')", "spark.catalog.cacheTable('products')"]

# Scenario 2: No query-specific config; use threshold-based caching
commands = get_cache_tables("query_999", "users, products, orders")
# Uses tablewise_cache() to determine which tables to cache based on size
```

**Notes**

- Check `spark_config_details` first for query-specific settings
- Query-specific cache table list (if present) overrides threshold-based decisions
- Pipe-delimited format is used for query-specific cache tables, but comma-delimited for the fallback `table_names`
- If no query config exists or it has an empty cache table list, delegates to `tablewise_cache()`

---

### get_uncache_tables(id)

**Purpose**

Get a list of Spark uncache commands for tables that should be removed from cache after query execution, based on
query-specific configuration.

**Args**

- `id` (str): Query or configuration ID (e.g., "query_001")

**Returns**

list: List of Spark uncache command strings, e.g., ["spark.catalog.uncacheTable('temp_table')"]

**Raises**

No explicit exceptions are raised

**Example**

```python
# Assume spark_config_details['query_001'].get_uncache_tables() = "temp_table|staging"
commands = get_uncache_tables("query_001")
# Returns: ["spark.catalog.uncacheTable('temp_table')", "spark.catalog.uncacheTable('staging')"]
```

**Notes**

- Returns an empty list if spark_config_details is empty or the query ID is not found
- Returns an empty list if the TableDetails has an empty uncache_tables field
- Uncache commands are typically executed after a query completes to free memory

---

### get_query_level_spark_configuration(id)

**Purpose**

Retrieve the Spark configuration settings (key=value pairs) that should be applied before executing a specific query.

**Args**

- `id` (str): Query or configuration ID

**Returns**

list: List of Spark configuration strings with spaces removed,
e.g., ["spark.sql.adaptive.enabled=true", "spark.sql.shuffle.partitions=200"]

**Raises**

- Re-raises any exception caught during processing (after logging)

**Example**

```python
# Assume spark_config_details['query_001'].get_spark_configuration() = "spark.sql.adaptive.enabled=true | spark.sql.shuffle.partitions=200"
configs = get_query_level_spark_configuration("query_001")
# Returns: ["spark.sql.adaptive.enabled=true", "spark.sql.shuffle.partitions=200"]
```

**Notes**

- Configuration is pipe-delimited in the CSV; each pipe-separated entry becomes a separate list item
- Spaces around the `=` are removed for clean config strings
- If query config is not found or is empty, returns an empty list
- Any exceptions are logged and re-raised

---

### get_reset_query_level_spark_configuration(id)

**Purpose**

Retrieve the Spark configuration settings in a "reset" format by substituting the current values (stored
in `spark_optimization_props`) back into the configuration strings. This is used to restore previous Spark settings
after a query completes.

**Args**

- `id` (str): Query or configuration ID

**Returns**

list: List of Spark reset configuration strings with original/backup values,
e.g., ["spark.sql.adaptive.enabled = false", "spark.sql.shuffle.partitions = 100"]

**Raises**

- Re-raises any exception caught during processing (after logging)

**Example**

```python
# Assume:
# spark_config_details['query_001'].get_spark_configuration() = "spark.sql.adaptive.enabled=true|spark.sql.shuffle.partitions=200"
# spark_optimization_props = {'spark.sql.adaptive.enabled': 'false', 'spark.sql.shuffle.partitions': '100'}

reset_configs = get_reset_query_level_spark_configuration("query_001")
# Returns: ["spark.sql.adaptive.enabled = false", "spark.sql.shuffle.partitions = 100"]
```

**Notes**

- Extracts the config keys from the query-specific configuration
- For each key, looks up the original value in `spark_optimization_props` (which is loaded at startup)
- Only includes keys that exist in `spark_optimization_props`
- Used to undo temporary Spark configuration changes after query execution
- Any exceptions are logged and re-raised

---

### get_query_level_in_memory(id, in_memory)

**Purpose**

Determine whether in-memory computation should be enabled for a specific query. Prioritizes query-specific setting over
the global default.

**Args**

- `id` (str): Query or configuration ID
- `in_memory` (bool): Global default for in-memory computation (fallback if query config is not found)

**Returns**

bool: True if in-memory computation should be enabled, False otherwise

**Raises**

- Re-raises any exception caught during processing (after logging)

**Example**

```python
# Scenario 1: Query-specific setting exists
# Assume spark_config_details['query_001'].get_in_memory() = "true"
result = get_query_level_in_memory("query_001", in_memory=False)
# Returns: True (query-specific overrides global default)

# Scenario 2: No query-specific setting
result = get_query_level_in_memory("query_999", in_memory=True)
# Returns: True (uses provided default)
```

**Notes**

- The in-memory string value ("true"/"false") is converted to boolean using `Utils.str_to_bool()`
- Query-specific setting takes precedence over the provided default
- If query config is not found or has empty in_memory field, uses the provided default
- Any exceptions are logged and re-raised

---

### load_and_execute_spark_optimization_property(executor)

**Purpose**

Load Spark optimization properties from a file and immediately apply them to the Spark session via the executor. This
sets baseline Spark configuration that can be later modified per-query and then reset.

**Args**

- `executor` (Executor object): Executor instance that provides the `execute()` method to apply Spark configurations

**Returns**

None

**Raises**

No exceptions are raised; errors are logged instead

**Example**

```python
# Properties file (spark_optimizations.properties):
# spark.sql.adaptive.enabled=true
# spark.sql.shuffle.partitions=200
# spark.sql.adaptive.skewJoin.enabled=true

executor = ExecutorImpl(spark_session)
load_and_execute_spark_optimization_property(executor)
# Each property is applied: executor.execute("set spark.sql.adaptive.enabled = true")
# Properties are also stored in spark_optimization_props for later reset
```

**Notes**

- Requires `props["spark.optimization.property.location"]` to be set (via `load_property()`)
- File path is checked for existence before reading
- Empty lines and comments (starting with `#`) are ignored
- Format: `key=value` (one per line)
- Each property is executed immediately via `executor.execute()` using the syntax `set key = value`
- All properties are also stored in the global `spark_optimization_props` dictionary for reference during reset
  operations
- If file does not exist, function completes silently
- Any exceptions during file reading or execution are caught and logged; the function does not raise

---

## Usage Pattern

### Typical Initialization Sequence

```python
# 1. Load main properties file
from com.impetus.idw.wmg.performance import load_property, load_tables_csv, load_spark_configuration_csv, load_and_execute_spark_optimization_property

load_property("/path/to/executor.properties")

# 2. Load table metadata
load_tables_csv()

# 3. Load query-specific configurations
load_spark_configuration_csv()

# 4. Load and apply baseline Spark optimizations
load_and_execute_spark_optimization_property(executor)

# 5. For each query, get caching and configuration commands
from com.impetus.idw.wmg.performance import (
    get_cache_tables, 
    get_uncache_tables, 
    get_query_level_spark_configuration,
    get_reset_query_level_spark_configuration,
    get_query_level_in_memory
)

query_id = "query_001"
tables_to_consider = "users, products, orders"

# Get cache commands
cache_cmds = get_cache_tables(query_id, tables_to_consider)
for cmd in cache_cmds:
    executor.execute(cmd)

# Get query-specific configs
query_configs = get_query_level_spark_configuration(query_id)
for cfg in query_configs:
    executor.execute("set " + cfg)

# Execute query...
# result = executor.executeQuery(query_string)

# Reset Spark configs
reset_configs = get_reset_query_level_spark_configuration(query_id)
for cfg in reset_configs:
    executor.execute("set " + cfg)

# Get uncache commands
uncache_cmds = get_uncache_tables(query_id)
for cmd in uncache_cmds:
    executor.execute(cmd)
```

---

## Configuration File Formats

### executor.properties

```properties
# Main execution configuration file
table.size.threshold=100
tables.csv.location=/path/to/tables.csv
spark.config.csv.location=/path/to/spark_config.csv
spark.optimization.property.location=/path/to/spark_optimizations.properties
```

### tables.csv

```csv
Table,Size(in MB)
users,50
products,80
orders,150
customers,45
```

### spark_config.csv

```csv
id,spark_configuration,caching_threshold,in_memory,cache_tables_before_query,uncache_tables_after_query
query_001,spark.sql.adaptive.enabled=true|spark.sql.shuffle.partitions=200,100,true,users|products,temp_table
query_002,spark.sql.adaptive.enabled=false,150,false,orders,staging
```

### spark_optimizations.properties

```properties
# Baseline Spark configuration applied at startup
spark.sql.adaptive.enabled=true
spark.sql.shuffle.partitions=200
spark.sql.adaptive.skewJoin.enabled=true
```

---

## Key Points for Beginners

- **Global State**: This module uses global
  dictionaries (`props`, `table_details`, `spark_config_details`, `spark_optimization_props`) to store configuration.
  Load all configuration files early in your application.
- **CSV-Based Configuration**: Most settings come from CSV files. Ensure these files exist at the paths specified in the
  properties file.
- **Caching Strategy**: Tables smaller than the threshold are automatically cached to speed up queries. Larger tables
  may use slower disk-based execution.
- **Query-Specific vs. Global**: Most functions support both query-specific configuration (from CSV) and global
  defaults (from properties file). Query-specific settings override global defaults.
- **Error Handling**: This module logs exceptions but generally does not raise them, allowing the application to
  continue even if configuration files are missing or malformed.
- **Performance Impact**: Caching improves performance for small tables but consumes memory. Uncache tables after use to
  free memory for other queries.

