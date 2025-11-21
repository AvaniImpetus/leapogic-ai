## local_persistence_handler.py

Overview

A simple ExceptionPersistenceHandler implementation that writes and reads exception restart details to a local JSON
file. Useful for local testing or deploys where a distributed filesystem is not available.

Class index

- LocalPersistencehandler

Method index

- persist_exception_details(restartable_exception)
- load_exception_details()

---

### LocalPersistencehandler

Purpose

Persist restart hierarchy to a local JSON file and read it back. This handler serializes a list of objects (each with
scriptClass and index) to JSON.

Methods

- persist_exception_details(restartable_exception)
    - Purpose: Convert the restart hierarchy inside a RestartableException into a compact list of dicts and write to the
      local JSON file.
    - Args:
        - restartable_exception: RestartableException instance.
    - Returns: None
    - Details: For each restart hierarchy item the code extracts `scriptClass` and `index`, calls `__SaveDetails` to
      write JSON, and logs the saved content.

- load_exception_details()
    - Purpose: Read the local JSON file, parse it and return a list of tuples (scriptClass, index). After reading, the
      method resets the file to an empty list (cleanup).
    - Args: None
    - Returns: A list of (scriptClass, index) tuples, or an empty list if the file is missing or contains no meaningful
      content.
    - Notes: The method tolerates missing file and catches exceptions silently (returns empty list). If the file
      contains only `[]` or whitespace it does not produce results. The handler writes an empty list back to the file
      after reading to avoid stale data.

Example

handler = LocalPersistencehandler(exceptionPersistencePath='/tmp')
handler.persist_exception_details(restartable_exception)
items = handler.load_exception_details()

Notes

- This implementation is not synchronized across processes. If multiple processes could write/read the same file
  concurrently you may encounter races or corrupt JSON.
- The method suppresses exceptions when reading; if you need stricter behavior, adapt the code to raise or log errors
  instead of passing quietly.
- File path defaulting uses `os.path.dirname(os.path.abspath(os.getcwd()))` which gives the parent of the current
  working directory; adapt if you need a different default.
