## exception_persistence_handler.py

Overview

This module defines the abstract base class used for persisting and loading exception restart details. Implementations
store and retrieve the restart hierarchy (typically written as JSON or a small dataset) so an ELT job can resume after
failure.

Class index

- ExceptionPersistenceHandler

Method index

- persist_exception_details(restartable_exception)
- load_exception_details()
- getFileName()

---

### ExceptionPersistenceHandler

Purpose

A lightweight framework base class that sets the contract for concrete persistence handlers. Subclasses implement how
exception/restart details are written and read (for example to HDFS, local filesystem or via Spark).

Methods

- persist_exception_details(restartable_exception)
    - Purpose: Persist the restart information contained in the provided RestartableException instance so a later run
      can resume from the recorded step(s).
    - Args:
        - restartable_exception: an instance of
          com.impetus.idw.wmg.exception.restartable_exception.RestartableException (or None). Implementations should
          validate the type before using.
    - Returns: None
    - Raises: Implementation-specific (IO errors, serialization errors, etc.)
    - Notes: This method is intentionally left as a no-op in the base class. Concrete handlers must override it.

- load_exception_details()
    - Purpose: Load any persisted restart details and return them in a canonical format (typically a list of tuples
      like (scriptClass, index, runtimeStepConfig)).
    - Args: None
    - Returns: A list representing saved restart entries. Each concrete handler decides the exact tuple/structure
      returned, but code in this project expects tuples of (scriptClass, index[, runtimeStepConfig]).
    - Raises: Implementation-specific.
    - Notes: Implementations should return an empty list when there is nothing to load.

- getFileName()
    - Purpose: Provide a default filename used by many handlers.
    - Args: None
    - Returns: The string "exception.json" by default.
    - Notes: Subclasses can keep or override this if they use a different filename or naming scheme.

Example usage

- A concrete implementation (HDFS, local or Spark-based) will subclass ExceptionPersistenceHandler and implement
  persist_exception_details and load_exception_details. The factory uses such concrete classes via registration.

Notes

- This class inherits from the project's FrameworkBase; it primarily exists to define the handler contract and supply a
  default file name.
