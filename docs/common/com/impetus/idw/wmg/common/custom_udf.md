## custom_udf.py — Spark UDF helpers

This module provides a small set of user-defined functions (UDFs) intended for PySpark usage and general utility within
ETL flows. The functions implement common behaviors found in database/ETL systems:

- `instr` — find the Nth occurrence of a substring (Teradata-like behavior);
- `sequence` / `reset_sequence` / `remove_sequence` — in-memory sequence generator helpers for simple numeric
  sequencing;
- `to_decimal_informatica` — parse a string into a decimal/float similar to Informatica's TO_DECIMAL semantics.

The module also exposes `custom_udf_mapping` which maps user-friendly UDF names to Python callables and their Spark
return types for convenient registration in Spark sessions.

Notes
: - These helpers are lightweight and in-memory. `sequence` and its helpers store state in a module-level dictionary and
are intended for short-lived processes (not distributed, not persistent across executors without additional handling).
- `instr` and `to_decimal_informatica` are pure functions and safe to use as Spark UDFs (stateless). `sequence` is
stateful — if used with Spark UDFs across executors you will not get a cluster-wide monotonic counter.

---

## List of functions

- `instr` — Find the N-th occurrence position of a substring; 1-based index; returns 0 when not found.
- `sequence` — Return a numeric sequence value for a named sequence (in-memory counter).
- `reset_sequence` — Initialize or reset a named sequence to a start value.
- `remove_sequence` — Remove a named sequence from in-memory state.
- `to_decimal_informatica` — Convert free-form strings to decimal/float following Informatica-like rules.
- `custom_udf_mapping` — Mapping of UDF names to callables and Spark return types for registration.

---

## `instr()`

Purpose
: Mimics Teradata's INSTR behavior. Finds the 1-based position of the N-th occurrence of `search_string`
inside `input_string` starting from `start_position`. Returns 0 if not found or when inputs are invalid.

Args
: 
- `input_string (str)`: String to search within.
- `search_string (str)`: Substring to search for.
- `start_position (int, optional)`: 1-based index to start searching from (default `1`).
- `occurrence (int, optional)`: The N-th occurrence to locate (default `1`).

Returns
: `int` — 1-based index position of the matched substring, or `0` if not found or invalid input.

Raises
: None explicitly; function handles invalid inputs and returns `0` for common invalid cases.

Example
: 
```py
instr('1A:2A:3A:4', 'A:', 1, 2)
# returns 5 (the second occurrence starts at character 5)

instr('hello world', 'x', 1, 1)

#### returns 0 (not found)

```

Notes:
- Inputs where `start_position` is out-of-range, `occurrence < 1`, or strings are falsy will return `0`.


---

## `sequence()`

Purpose
: Provide a simple in-memory numeric sequence generator keyed by `seq_name`.

Args
: 
  - `seq_name (str)`: Identifier for the sequence (kept in module-level state).
  - `start_value (int)`: Initial value when the sequence is first created.
  - `step (int)`: Amount to increment on subsequent calls (can be negative).

Returns
: `int` — Current value for the sequence after the call (first call returns `start_value`).

Raises
: None explicitly; the function assumes numeric inputs for `start_value` and `step`.

Example
: 
```py
sequence('s1', 1, 1)  # returns 1
sequence('s1', 1, 1)  # returns 2
sequence('s2', 100, -2)  # returns 100
sequence('s2', 100, -2)  # returns 98
```

Notes
: 
- State is stored in the module-level `seq_hash` dictionary. This is process-local and not synchronized across
distributed workers. Use only for local/sequential tasks or testing.

---

## `reset_sequence()`

Purpose
: Initialize or reset a named sequence to a given start value.

Args
: 
- `seq_name (str)`: Sequence identifier.
- `start_value (int)`: Value to set for next `sequence` call.

Returns
: `None` — updates module-level state.

Raises
: None explicitly.

Example
: 
```py
reset_sequence('s1', 1)
sequence('s1', 1, 1)  # returns 1
```

Notes
: 
- Useful to deterministically reset counters during tests or before a batch of processing.

---

## `remove_sequence()`

Purpose
: Remove a named sequence from the module-level state.

Args
: - `seq_name (str)`: Sequence identifier to remove.

Returns
: `None` — removes the key if present.

Raises
: None explicitly; non-existent sequence is ignored.

Example
: 
```py
remove_sequence('s1')
```

Notes
: 
- After removal, calling `sequence(seq_name, ...)` will re-create the sequence starting at the provided `start_value`.

---

## `to_decimal_informatica()`

Purpose
: Convert a free-form string into a Python float replicating common Informatica `TO_DECIMAL` behaviors.

Behavior summary
: - Returns `None` when input is `None`.
- Returns `0.0` for empty string or when the first character is non-numeric (excluding leading `-`).
- Replaces `,` with `.` to support comma decimal separators before parsing.
- Extracts the leading numeric token (optional fractional part) and returns it as a float.

Args
: - `input_str (str or None)`: The input string to convert.

Returns
: `float` or `None` — Parsed numeric value, or `None` if input was `None`.

Raises
: None explicitly; function uses defensive checks and returns `0.0` for non-numeric starts.

Example
: 
```py
to_decimal_informatica('123abc456')  # -> 123.0
to_decimal_informatica('abc123')     # -> 0.0
to_decimal_informatica(None)         # -> None
to_decimal_informatica('')           # -> 0.0
to_decimal_informatica('12,34')      # -> 12.0
to_decimal_informatica('-45.67xyz')  # -> -45.67
```

Notes
: 
- Uses a regex to extract a leading `-?\d+(\.\d+)?` token. It is conservative: only the leading numeric portion is
returned.
- All whitespace is stripped before processing.

---

## custom_udf_mapping

Purpose
: A convenience dictionary mapping short UDF registration names to a mapping of the Python callable and its expected
Spark `DataType`. This can be used to register UDFs with PySpark as follows:

Example (registering with SparkSession)
: 
```py
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from com.impetus.idw.wmg.common import custom_udf as cu

spark = SparkSession.builder.getOrCreate()
for udf_name, m in cu.custom_udf_mapping.items():
for func, spark_type in m.items():
spark.udf.register(udf_name, func, spark_type)

```

Notes
: 
  - The mapping preserves the original function objects and the Spark return types (e.g., `IntegerType()`, `DoubleType()`).
  - `udf_sequence` maps to a stateful Python function — using it as a Spark UDF across executors does not provide a global monotonic sequence and is generally unsafe in distributed contexts.

---
