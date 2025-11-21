# elt_process.py

## Overview

The `elt_process.py` module defines the abstract base class `ELTProcess` which is the minimal contract for ELT step
implementations in this framework. It extends `FrameworkBase` and declares lifecycle methods that concrete ELT step
classes must implement. It also provides helpers to obtain exception persistence handlers and to query restart indices
with optional runtime configuration matching.

This file is intentionally small and focused: concrete behavior is implemented in subclasses (for
example, `DataProcessingStep` and `GlueELTStep`).

---

## Method Index

- `execute(executor, recovery_hierarchy=None, *args, **kwargs)` - Abstract: main execution entry for subclasses.
- `executeFlow(executor, *args, **kwargs)` - Abstract: implement the flow logic that performs ETL/ELT.
- `getExceptionPersistenceHandler(*args, **kwargs)` - Return an exception persistence handler instance from factory.
- `getRestartIndex(step, restart_hierarchy, step_runtime_config=[])` - Lookup restart index for a step, optionally
  matching runtime config.

---

## Detailed Method Documentation

### execute(executor, recovery_hierarchy=None, *args, **kwargs)

Purpose:

Abstract method signifying the main entry point for running an ELT process. Concrete implementations must implement
orchestration of the step, including parameter resolution, executor usage, restart semantics and invoking `executeFlow`.

Args:
executor (object): Execution engine or data access layer provided by the framework (type depends on DAL implementation).
recovery_hierarchy (list, optional): Restart/recovery information used to resume failed executions.
*args, **kwargs: Additional implementation-defined runtime parameters.

Returns:
Implementation-defined (typically None). Side-effects include reading/writing datasets and registering status.

Raises:
Implementation-specific exceptions. Parent orchestration may wrap exceptions for restart handling.

Notes:

- Declared abstract; subclasses must provide a concrete implementation.

---

### executeFlow(executor, *args, **kwargs)

Purpose:

Abstract method to contain the actual ETL/ELT business logic (reading, transforming, writing). Implemented by
subclasses.

Args:
executor (object): Execution engine (e.g., Spark executor) for data operations.
*args, **kwargs: Implementation-specific parameters.

Returns:
Implementation-defined outputs or side-effects.

Raises:
Any exception thrown by logic inside the method.

Notes:

- This method is invoked by concrete `execute()` implementations after setup.

---

### getExceptionPersistenceHandler(*args, **kwargs)

Purpose:

Return an exception persistence handler instance used to persist or handle exception information. This delegates
to `ExceptionPersistenceHandlerFactory` and accepts optional kwargs to configure the factory.

Args:
*args, **kwargs: Optional parameters passed to factory. Recommended kwarg:

- `exceptionPersistenceHandler` (str): Name/key of the handler to instantiate (default `'spark'`).

Returns:
An instance returned by `ExceptionPersistenceHandlerFactory.get_instance(...)`.

Raises:
Any exception raised by the underlying factory method.

Example:

```python
handler = self.getExceptionPersistenceHandler(exceptionPersistenceHandler='custom')
handler.persist(exception_obj)
```

Notes:

- The default handler name is `'spark'` if not provided.

---

### getRestartIndex(step, restart_hierarchy, step_runtime_config=[])

Purpose:

Search the supplied `restart_hierarchy` sequence for an entry matching the provided step name and optionally matching
runtime configuration. Returns the recorded restart index if a match is found.

Args:
step (str): Step identifier to search for (usually class name).
restart_hierarchy (list): Iterable of restart tuples typically in the
form `(step_name, restart_index, [optional_runtime_config_list])`.
step_runtime_config (list, optional): List of runtime config tokens. When provided, a match is only returned if the
runtime config tokens match the third element of the restart tuple.

Returns:
int or None: The restart index for the step if matched; otherwise `None`.

Raises:
None

Example:

```python
restart_hierarchy = [('MyStep', 3, ['A','B']), ('OtherStep', 2, [])]
index = getRestartIndex('MyStep', restart_hierarchy, ['A','B'])  # returns 3
```

Notes:

- If `step_runtime_config` is empty, the function returns the first matching step restart index regardless of the
  runtime config.
- Performs exact string join comparison when matching runtime config lists.

---

## Usage Notes

- `ELTProcess` is intentionally abstract; you will normally use concrete subclasses (
  e.g., `DataProcessingStep`, `GlueELTStep`) for executing ETL flows.
- The helper methods in this file are minimal; most orchestration logic resides in subclasses.

---
