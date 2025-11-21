# eltjob_exception.py

## Overview

The `eltjob_exception.py` module defines a custom exception class `ELTJobException` used across the ELT framework to
capture, merge, and present exception details in a structured, human-friendly way. This class extends Python's
built-in `Exception` and adds utilities to accumulate multiple exception details, pretty-print multi-step failures, and
extract contextual frame-level information.

Why use `ELTJobException`:

- Consolidates multiple related exceptions into one container
- Captures traceback and contextual code snippets for quick debugging
- Provides helper to merge exception details from nested operations
- Presents readable, numbered exception details in `__str__()`

---

## Methods

- `mergeExceptionDetails(exception)` - Merge exception details from another `ELTJobException` into this one
- `getExceptionDetails(e)` - Extract detailed contextual information (frames, locals) for an exception

---

## Detailed Method Documentation

### mergeExceptionDetails(exception)

Purpose:

Merge exception detail information from another `ELTJobException` instance into this one. Useful when multiple failures
occur in a pipeline and you want to present a consolidated view.

Args:
exception (ELTJobException): The other exception whose details should be appended into this exception's details.

Returns:
None (operates by side-effect)

Raises:
None explicitly; method checks type of provided argument before merging.

Example:

```python
try:
    step1()
except ELTJobException as e1:
    try:
        step2()
    except ELTJobException as e2:
        e1.mergeExceptionDetails(e2)
        raise e1
```

Notes:

- Only merges when the provided `exception` is an instance of `ELTJobException` and when `self.exceptionDetails` is a
  list.
- Does not create defensive copies of strings (strings are immutable), simply extends the list.

---

### getExceptionDetails(e)

Purpose:

Extract contextual details for a caught exception `e`, including file, line number, function name, code snippet, and
local variables from the relevant stack frame. This helps developers quickly identify what data and logic were present
when the exception occurred.

Args:
e (Exception): The exception instance to extract details from.

Returns:
str: A multi-line formatted string containing frame and local variable information for the most relevant stack frame(s).

Raises:
None explicitly; uses Python `inspect` and `sys` modules to introspect the stack.

Example:

```python
try:
    do_work()
except Exception as e:
    details = ELTJobException().getExceptionDetails(e)
    print(details)
```

Notes:

- The function inspects the traceback, reverses frames to present the deepest call first, and stops after the first
  non-skipped frame.
- It skips frames whose local variables include a sentinel `__lgw_marker_local__` to reduce clutter from wrapper frames.
- For the selected frame, it records file, line, function name, and a code snippet and then lists each local variable
  with its stringified value.
- The function returns only the first matching frame's details (it `break`s after processing one frame).
- Local variables are stringified via `str(v)`; be careful with very large local objects (may be costly to stringify).

---

## Best Practices & Usage Tips

- Use `ELTJobException` to wrap lower-level exceptions when propagating errors up the pipeline. Its merge capability is
  handy when collecting multiple errors across parallel branches.
- Avoid passing large objects into exception local contexts if they will be stringified by `getExceptionDetails()`; this
  can bloat logs.
- Prefer raising `ELTJobException` with a clear message and the original exception passed in as `exception` so its
  traceback is preserved.

---
