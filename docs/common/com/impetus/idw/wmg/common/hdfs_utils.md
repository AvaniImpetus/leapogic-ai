# hdfs_utils.py

## File Overview

This module provides utilities for reading, writing, creating, and deleting files on HDFS (Hadoop Distributed File
System). It uses the Hadoop Java libraries through Spark's JVM bridge to interact with HDFS directly.

## Function Index

1. `saveContent()`
2. `readContent()`
3. `createFile()`
4. `fileExists()`
5. `deleteFile()`
6. `rename_file()`
7. `list_files()`
8. `list_files_hdfs()`
9. `readFileListWithoutRecursive()`
10. `readFileListWithRecursive()`

---

## Functions

### saveContent()

**Purpose:**  
Saves content to a file on HDFS. Creates the file if it doesn't exist, or overwrites/appends based on the mode
parameter.

**Args:**

- `executor` (object): The executor object with a spark_session property.
- `content` (str): The content to write to the file.
- `target_file_path` (str): HDFS file path (e.g., `hdfs://namenode:8020/path/to/file.txt`).
- `mode` (str, optional): Write mode: `"write"` (create/overwrite) or `"append"` (append to existing).
  Default: `"write"`.

**Returns:**

- None. Writes file.

**Raises:**

- Exception if file operations fail (propagated from Hadoop).

**Example:**

```python
saveContent(executor, "Hello World", "hdfs:///data/output.txt", mode="write")
```

---

### readContent()

**Purpose:**  
Reads and returns the complete contents of a file from HDFS.

**Args:**

- `executor` (object): The executor object with a spark_session property.
- `input_file_path` (str): HDFS file path to read.

**Returns:**

- `str`: The file contents as a string. Returns empty string if file doesn't exist.

**Raises:**

- Exception if read operation fails.

**Example:**

```python
content = readContent(executor, "hdfs:///data/input.txt")
print(content)
```

---

### createFile()

**Purpose:**  
Creates a new file on HDFS. Returns `False` if file already exists (with specific exception handling).

**Args:**

- `executor` (object): The executor object.
- `input_file_path` (str): HDFS file path to create.

**Returns:**

- `bool`: `True` if file created successfully; `False` if file already exists.

**Raises:**

- Other exceptions if file creation fails for reasons other than "file already exists".

**Example:**

```python
created = createFile(executor, "hdfs:///data/newfile.txt")
if created:
    print("File created")
else:
    print("File already exists")
```

---

### fileExists()

**Purpose:**  
Checks whether a file or directory exists on HDFS.

**Args:**

- `executor` (object): The executor object.
- `input_file_path` (str): HDFS path to check.

**Returns:**

- `bool`: `True` if the path exists; `False` otherwise.

**Raises:**

- Exception if the check fails.

**Example:**

```python
exists = fileExists(executor, "hdfs:///data/input.txt")
if exists:
    print("File exists")
```

---

### deleteFile()

**Purpose:**  
Deletes a file or directory from HDFS.

**Args:**

- `executor` (object): The executor object.
- `input_file_path` (str): HDFS path to delete.

**Returns:**

- None.

**Raises:**

- Exception if deletion fails.

**Example:**

```python
deleteFile(executor, "hdfs:///data/oldfile.txt")
```

---

### rename_file()

**Purpose:**  
Renames or moves a file on HDFS.

**Args:**

- `executor` (object): The executor object.
- `source_path` (str): Current HDFS file path.
- `target_path` (str): New HDFS file path.

**Returns:**

- None.

**Raises:**

- Exception if rename fails.

**Example:**

```python
rename_file(executor, "hdfs:///data/oldname.txt", "hdfs:///data/newname.txt")
```

---

### list_files()

**Purpose:**  
Lists files on HDFS with optional filtering, sorting, and recursive listing.

**Args:**

- `executor` (object): The executor object.
- `input_file_path` (str): HDFS directory path to list.
- `recursive` (bool, optional): If `True`, recursively list subdirectories. Default: `False`.
- `include_file_patterns` (list, optional): List of fnmatch patterns to include (e.g., `['*.txt', '*.csv']`).
- `exclude_file_pattern` (str, optional): fnmatch pattern for files to exclude.
- `list_file_type` (str, optional): Filter by type: `'files'`, `'directory'`, or `None` (all). Default: `None`.

**Returns:**

- `list`: List of matching HDFS file paths.

**Raises:**

- Exception if listing fails.

**Example:**

```python
files = list_files(executor, 'hdfs:///data', recursive=True, include_file_patterns=['*.txt'], list_file_type='files')
```

---

### list_files_hdfs()

**Purpose:**  
Lists files in HDFS with support for recursive listing and returns metadata (filename, size, modification date).

**Args:**

- `executor` (object): The executor object.
- `input_file_path` (str): HDFS directory path to list.
- `recursive` (bool, optional): If `True`, recursively list. Default: `False`.
- `list_file_type` (str, optional): File type filter (currently not used in implementation). Default: `None`.

**Returns:**

- `list`: List of dictionaries with keys `'fileName'`, `'fileSize'`, and `'fileModifiedDate'`.

**Raises:**

- Exception if listing fails.

**Example:**

```python
file_list = list_files_hdfs(executor, 'hdfs:///data', recursive=True)
for file_info in file_list:
    print(f"{file_info['fileName']} - Size: {file_info['fileSize']}")
```

---

### readFileListWithoutRecursive()

**Purpose:**  
Internal helper function. Lists files in a directory (non-recursive) and returns metadata.

**Args:**

- `hdfs` (Java object): HDFS path object.
- `file_system` (Java object): Hadoop FileSystem object.

**Returns:**

- `list`: List of file metadata dictionaries.

**Raises:**

- None (called internally).

**Notes:**

- Called by `list_files_hdfs()` when recursive=False.

---

### readFileListWithRecursive()

**Purpose:**  
Internal helper function. Recursively lists all files in a directory and returns metadata.

**Args:**

- `hdfs` (Java object): HDFS path object.
- `hadoop_conf` (Java object): Hadoop configuration object.

**Returns:**

- `list`: List of file metadata dictionaries.

**Raises:**

- None (called internally).

**Notes:**

- Called by `list_files_hdfs()` when recursive=True.

