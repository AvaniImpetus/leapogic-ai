## custom_udf.py — Spark UDF helpers

This module provides a small set of user-defined functions (UDFs) intended for PySpark usage and general utility within
ETL flows. The functions implement common behaviors found in database/ETL systems:

The module also exposes `custom_udf_mapping` which maps user-friendly UDF names to Python callables and their Spark
return types for convenient registration in Spark sessions.

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
    Mimics Teradata INSTR behavior:
    Returns the position (1-based index) of the N-th occurrence of a substring starting from StartPosition.
    Returns 0 if not found.

Args
: 
- `input_string (str)`: String to search within.
- `search_string (str)`: Substring to search for.
- `start_position (int, optional)`: 1-based index to start searching from (default `1`).
- `occurrence (int, optional)`: The N-th occurrence to locate (default `1`).

Returns
:  1-based index of the found substring, or `0` if not found or invalid input.

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
  - `seq_name (str)`: Identifier for the sequence.
  - `start_value (int)`: Initial value when the sequence is first created.
  - `step (int)`: Amount to increment on subsequent calls (can be negative).

Returns
: `int` —next value in sequence.

Raises
: None explicitly; the function assumes numeric inputs for `start_value` and `step`.

Example
: 
```py
sequence('s1', 1, 1)  # returns 1,2,3 ...
sequence('s2', 100, -2)  # returns 100. 98, 96
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
: Converts a string to a decimal value, replicating the behavior of Informatica's TO_DECIMAL function.


Behavior summary
:   - If the string starts with numeric characters (or a minus sign), it parses until the first non-numeric character.
    - If the first character is non-numeric (excluding a minus sign), it returns 0.0.
    - If the input is NULL (None), it returns None.
    - If the input is an empty string, it returns 0.0.
    - If the string uses a comma (,) as a decimal separator, it is converted to a dot (.) before parsing.

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

