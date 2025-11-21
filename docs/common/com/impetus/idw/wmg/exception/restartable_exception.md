## restartable_exception.py

Overview

`RestartableException` is a specialized ELT job exception that captures a restart hierarchy. It records which step/class
failed along with an execution index so the framework can pick up processing from the appropriate point.

Class index

- RestartableException (subclasses ELTJobException)

Method index

- mergeExceptionDetails(exception)

---

### RestartableException

Purpose

Wraps error information with restart metadata so an ELT orchestration can persist and later reconstruct what should be
restarted. The restart hierarchy is a list of tuples capturing information about failing steps.

Methods

- mergeExceptionDetails(exception)
    - Purpose: Merge another RestartableException's restart hierarchy into this instance.
    - Args:
        - exception: Another RestartableException instance.
    - Behavior: Extends this object's `restart_hierarchy` with the other's hierarchy and forwards to
      ELTJobException.mergeExceptionDetails for merging other exception details.

- getKeyArgs()
    - Purpose: Return `key_args` captured at construction.
    - Returns: Dictionary of additional keyword arguments passed to the exception.

- getHierarchy()
    - Purpose: Accessor for the restart hierarchy (note: implementation appears to just
      reference `self.restart_hierarchy` — callers should use the attribute directly or this method can be extended
      to `return self.restart_hierarchy`).
    - Returns: (currently the method does not explicitly return; consider using the attribute or
      adding `return self.restart_hierarchy`).

Example

- Raising a restartable exception inside a step:

  raise RestartableException("Failure in step", step_name='MyStep', execution_index=2, runtime_step_config={'param':1})

- Merging details when wrapping exceptions:

  first = RestartableException("First failure", step_name='StepA', execution_index=0)
  second = RestartableException(first, step_name='StepB', execution_index=1)
  second.mergeExceptionDetails(first)

Notes / Suggestions

- `getHierarchy` currently does not explicitly return the hierarchy; if you rely on it, consider
  adding `return self.restart_hierarchy` to the implementation.
- The constructor stores a `trace` via `sys.exc_info()[1]`—this will capture the active exception object if called from
  inside an `except` block; otherwise `trace` may be None.
- The restart tuple format is `(step_name, index, hierarchy_level, message, trace, key_args)`. Code that reads persisted
  details (handlers) expects certain fields; check compatibility when changing this structure.
