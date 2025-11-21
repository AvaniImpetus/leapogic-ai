# postgres_object_extrator_util.py

## Overview

This module provides the `PostgresObjectExtractor` class for extracting SQL DDL (Data Definition Language) statements
from PostgreSQL database objects (tables and views). It extends `RedshiftObjectDDLExtractor` and provides
PostgreSQL-specific queries for DDL extraction.

## Classes

### PostgresObjectExtractor

**Purpose:**  
Extracts DDL statements for PostgreSQL tables and views from a PostgreSQL database and generates create statements that
can be used for schema migration or documentation.

**Constructor: `__init__()`**

**Constructor Initializes:**

- `self.db_name`: Set to `'postgresql'`.
- `self.ddl_extraction_column_name`: Set to `'{object_type}_statement'`.
- `self.get_view_ddl_query`: PostgreSQL query for view DDL extraction.
- `self.get_table_ddl_query`: PostgreSQL query for table DDL extraction.
- `self.conn_config`: Database connection configuration (populated by `__create_conn_configuration__`).

---

## Module Entry Point

```python
if __name__ == '__main__':
    step = PostgresObjectExtractor()
    step.start()
```

When executed directly, instantiates the extractor and calls `start()` method (inherited from `GlueELTStep`).
___
