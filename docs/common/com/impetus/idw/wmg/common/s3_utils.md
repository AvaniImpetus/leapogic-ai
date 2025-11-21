# s3_utils.py — S3 utilities reference

## Overview

This file documents the helper functions provided in `com/impetus/idw/wmg/common/s3_utils.py`. The module implements a
set of convenience utilities for working with Amazon S3 from Python using `boto3` and a few wrapper/helper behaviors
used throughout the codebase. It includes functions to list, read, write, copy, delete, compress and extract files on
S3, plus a number of small text-processing helpers that operate on file contents (or strings/lists) in a way similar to
common shell tools (grep, sed, awk, etc.).

## Methods (brief index)

- `list_files_2(input_path, recursive=False, include_file_patterns=None, exclude_file_pattern=None, list_file_type=None, order_by=None, is_desc=False)` —
  convenience wrapper that accepts full s3:// path.
- `list_files(bucket_name, input_path, recursive=False, include_file_patterns=None, exclude_file_pattern=None, list_file_type=None, order_by=None, is_desc=False)` —
  main file listing helper.
- `find_files(input_path, include_pattern=None, exclude_pattern=None, last_modified_time=None, execute_command=None, **options)` —
  find files with optional last-modified filtering and delete action.
- `get_file_md5(source_path, algo="md5sum")` — download a file and compute a checksum using a system utility.
- `rename_s3_file(source_path, target_path)` — rename (copy + delete) an object within S3.
- `move_file(src, tgt)` — move single file or wildcard set to target folder.
- `object_exists(file_path)` — check whether an S3 object exists.
- `create_object(bucket_name, file_path)` — create an empty object key in a bucket.
- `create_file(file_path)` — convenience wrapper to create an S3 object via full path.
- `read_file(file_path, encode='utf-8')` — read content of an S3 object (supports wildcard patterns).
- `read_file_lines(file_path, encode='utf-8')` — read file and return list of lines.
- `write_file(content, file_path, mode='append', encode='utf-8')` — write content to S3; supports append mode.
- `delete_object(src_bucket, src_key)` — delete objects (supports wildcard).
- `delete_file(file_path, options=None)` — delete object specified by s3 path.
- `copy(source_path, target_path, delete_source=False)` — copy one or more objects to a new location.
- `grep_lines_from_file(file_path, text)` — return lines matching text (delegates to `utils.grep_lines`).
- `truncate(input_path, output_path, character)` — remove lines containing given character and write to output.
- `change_dir(file_path)` — placeholder (no implementation).
- `sed_command(scripts, file_path)` — apply sed-like scripts to file/string/list and return transformed lines.
- `extract_nth_field(file_path, option, indexes, complement=False, delimiter="\t")` — extract specified fields from
  lines.
- `awk_from_file(file_path, separator, begin_operations=None, iter_operations=None, end_operations=None, options=None)` —
  awk-like processing over records.
- `convert_excel_to_csv(input_s3_path, output_s3_prefix, delimiter, quote_char)` — convert XLSX sheets to CSV-like text
  files and upload to S3.
- `gzip_file(executor, source_path, target_path=None, delete_source=False)` — gzip one or more S3 files.
- `gzip_file_single_file(executor, source_path, target_path=None, delete_source=False)` — gzip a single S3 file using a
  local temp file and helper glue utils.
- `zip_file(input_location, output_location, files_to_zip=None)` — zip files using `aws_utils.ZipFiles` helper.
- `zip_large_file(source_path, target_path=None, chunk_size=104857600)` — wrapper for large-file zip compression helper.
- `gunzip_file(source_path, target_path)` — download a .gz S3 object, decompress and upload decompressed content to
  target S3 prefix.
- `unzip_file(zip_s3_path, target_path=None)` — unzip a zip archive stored on S3 and upload the extracted files to the
  target S3 prefix.

---

## Detailed function reference

Note: Exceptions listed below are common possibilities. Many functions call into `boto3` or other helpers which can
raise `botocore.exceptions.ClientError` (permission, missing bucket/object), `ValueError`, `IOError` or
generic `Exception` depending on the failure.

### `list_files_2()`

Purpose: Lists files from an S3 path in the simplified format (s3://bucket/path syntax).

Args:

- input_path (str): Full S3 path in format `s3://bucket-name/path/to/directory`
- recursive (bool, optional): If True, lists all files recursively in subdirectories. Defaults to False
- include_file_patterns (list, optional): List of file name patterns to include (e.g., `['*.csv', '*.txt']`). Supports
  wildcard patterns. Defaults to None (include all files)
- exclude_file_pattern (str, optional): Pattern for files to exclude (e.g., `*.log`). Defaults to None
- list_file_type (str, optional): Type of items to list. Use `'directory'` to list only directories, or None/'file' to
  list files. Defaults to None
- order_by (str, optional): Sort results by `'name'`, `'size'`, or `'last_modified'`. Defaults to None (unsorted)
- is_desc (bool, optional): If True, sort in descending order. Defaults to False

Returns:

- list: List of file paths in S3 format (e.g., `['s3://bucket/file1.txt', 's3://bucket/file2.txt']`)

Raises:

- botocore.exceptions.ClientError: If S3 bucket doesn't exist or access is denied
- ValueError: If invalid path format is provided

Example:

```python
# List all CSV files in a directory
csv_files = list_files_2(
    input_path="s3://my-bucket/data/",
    include_file_patterns=['*.csv']
)

# List files sorted by modification time (newest first)
recent_files = list_files_2(
    input_path="s3://my-bucket/logs/",
    order_by='last_modified',
    is_desc=True
)

# List only directories
directories = list_files_2(
    input_path="s3://my-bucket/",
    list_file_type='directory'
)
```

Notes:

- Supports wildcard patterns in `include_file_patterns` and `exclude_file_pattern`.
- S3 paths are always returned with the `s3://` prefix.

### `list_files()`

Purpose: Core function to list S3 objects (or directories) under a bucket/prefix with optional filtering and sorting.

Args:

- bucket_name (str): S3 bucket name
- input_path (str): Key prefix inside the bucket
- recursive (bool, optional): If True, list objects recursively. Defaults to False
- include_file_patterns (list, optional): List of file name patterns to include (e.g., `['*.csv', '*.txt']`). Supports
  wildcard patterns. Defaults to None (include all files).
- exclude_file_pattern (str, optional): Pattern for files to exclude (e.g., `*.log`). Defaults to None
- list_file_type (str, optional): If `'directory'`, calls the directory lister. Defaults to None
- order_by (str, optional): 'name', 'size', or 'last_modified'
- is_desc (bool, optional): reverse ordering if True

Returns:

- list: Filtered list of `s3://...` URIs.

Raises:

- ValueError: If `order_by` is unsupported (the function logs an error and continues without sorting)
- botocore.exceptions.ClientError: On boto3 failures

Example:

```python
files = list_files('my-bucket', 'data/2025/', include_file_patterns=['*.parquet'], order_by='name')
```

Notes:

- Delegates pattern filtering to `utils.__get_file_based_on_patterns__` (internal utility).

### `find_files()`

Purpose: Find files under an S3 path matching include/exclude patterns and optionally filter by last modified time or
remove them.

Args:

- input_path (str): `s3://...` path to search
- include_pattern (list or str, optional): List of file name patterns to include (e.g., `['*.csv', '*.txt']`). Supports
  wildcard patterns. Defaults to None (include all files)
- exclude_pattern (str, optional): Pattern for files to exclude (e.g., `*.log`). Defaults to None
- last_modified_time (str, optional): time filter as days, supports prefix `+`/`-` (e.g., `+30` for files older than 30
  days)
- execute_command (str, optional): If `'rm'`, delete matched files; otherwise only returns matches
- **options: Additional options passed through (not used in current implementation)

Returns:

- list: List of matching S3 paths (or an empty list if files were removed and none remain).

Raises:

- Exception: If `execute_command` is provided but not equal to `'rm'` (unsupported)
- botocore.exceptions.ClientError

Example:

```python
# Find files changed exactly 7 days ago
files = find_files('s3://my-bucket/logs/', include_pattern=['*.log'], last_modified_time='7')

# Remove files older than 30 days
removed = find_files('s3://my-bucket/archive/', include_pattern=['*.csv'], last_modified_time='+30',
                     execute_command='rm')
```

Notes:

- The function computes day-differences using UTC-aware timestamps and compares integer day values.

### `get_file_md5()`

Purpose: Download an S3 object to a local temporary path and compute a checksum using a system utility (like `md5sum`).

Args:

- source_path (str): S3 path to the object (`s3://bucket/key`)
- algo (str, optional): System command for hashing. Supported: `md5sum`, `sha1sum`, `sha256sum`, `sha512sum`. Defaults
  to `md5sum`.

Returns:

- str: Hash string (hex) returned by the system hashing utility, or `None` if parsing fails.

Raises:

- Exception: If an unsupported `algo` is supplied.
- Called subprocesses may raise `FileNotFoundError` if `aws` or the hash command is not available on PATH.

Example:

```python
md5 = get_file_md5('s3://my-bucket/data/file.parquet', algo='md5sum')
```

Notes:

- This function depends on external system tools: the AWS CLI and the checksum binary. It is not pure-Python and may not
  work on systems without those tools.
- Temporary files are written to `/tmp/` which on Windows may not behave the same; this helper is primarily for
  Linux-like environments.

### `rename_s3_file()`

Purpose: Rename (copy then delete) an S3 object by copying it to the target key and removing the source.

Args:

- source_path (str): Full S3 path of the source object
- target_path (str): Full S3 path of the target object

Returns:

- None

Raises:

- botocore.exceptions.ClientError

Example:

```python
rename_s3_file('s3://my-bucket/foo/a.txt', 's3://my-bucket/bar/a.txt')
```

### `move_file()`

Purpose: Move files from source to target. Supports wildcard source paths.

- If the `src` contains `*`, list matching files and rename each into the `tgt` folder. Otherwise treats `src` as a
  single path and renames it into `tgt`.

Args:

- src (str): S3 path or pattern (e.g., `s3://bucket/path/*.csv`)
- tgt (str): Target S3 directory (S3 path without filename) where files will be moved

Returns:

- None

Raises:

- botocore.exceptions.ClientError

Example:

```python
# Move all CSVs
move_file('s3://my-bucket/data/*.csv', 's3://my-bucket/processed/')

# Move single file
move_file('s3://my-bucket/data/file1.csv', 's3://my-bucket/processed')
```

Notes:

- If `tgt` does not end with a slash, the implementation still constructs target path by appending filename.

### `object_exists()`

Purpose: Check whether an S3 object exists.

Args:

- file_path (str): Full S3 path `s3://bucket/key`

Returns:

- bool: True when the object exists, False otherwise.

Raises:

- None; exceptions are caught and False is returned.

Example:

```python
if object_exists('s3://my-bucket/data/file.csv'):
    print('exists')
```

Notes:

- The function swallows all exceptions and returns False on any unexpected error (including permission issues). If you
  need the exact exception, directly use `boto3` calls.

### `create_object()`

Purpose: Create an empty object at the specified `file_path` (key) inside a bucket.

Args:

- bucket_name (str): Bucket name
- file_path (str): Key to create inside the bucket

Returns:

- None

Raises:

- botocore.exceptions.ClientError

Example:

```python
create_object('my-bucket', 'path/to/placeholder.txt')
```

Notes:

- This creates a zero-byte object. Use `write_file` to create objects with content.

### `create_file()`

Purpose: Convenience wrapper to create an empty object by passing a full S3 path.

Args:

- file_path (str): `s3://bucket/key`

Returns:

- None

Example:

```python
create_file('s3://my-bucket/path/placeholder.txt')
```

### `read_file()`

Purpose: Read the content of an S3 object or a set of objects matching a wildcard and return the combined content as a
string.

Description: Supports passing a single full S3 path or a wildcard pattern containing `*`. If `*` is present, lists all
the files and concatenates their contents (separated by `\n`).

Args:

- file_path (str): Full S3 path (`s3://bucket/key`) or wildcard pattern
- encode (str, optional): Text encoding, default `'utf-8'`

Returns:

- str: Combined contents of the target file(s).

Raises:

- botocore.exceptions.ClientError

Example:

```python
content = read_file('s3://my-bucket/data/file1.txt')

# Read all txt files in a folder
content = read_file('s3://my-bucket/data/*.txt')
```

Notes:

- When reading multiple files the function prefixes the concatenation with a leading newline for each appended file;
  callers may want to `strip()` or otherwise sanitize the result.

### `read_file_lines()`

Purpose: Return the content of a file (or matching files) as a list of lines.

Args:

- file_path (str): `s3://...` path or pattern
- encode (str, optional): Encoding used by `read_file`

Returns:

- list[str]: lines of the file(s)

Example:

```python
lines = read_file_lines('s3://my-bucket/data/file.txt')
```

Notes:

- Preserves line content as returned by `splitlines()` (no trailing newline characters).

### `write_file()`

Purpose: Write content to an S3 object. Supports append mode.

Description: Accepts content as `str`, `int`, `list` or `tuple`. For list/tuple it joins with `\n`. If `mode='append'`
and the object already exists, it reads existing content and prepends it to the new content separated by `\n`.

Args:

- content (str | int | list | tuple): Data to write
- file_path (str): `s3://bucket/key` destination
- mode (str, optional): `'append'` or other; append will combine existing content. Defaults to `'append'`.
- encode (str, optional): Encoding for bytes. Defaults to `'utf-8'`.

Returns:

- None

Raises:

- botocore.exceptions.ClientError

Example:

```python
write_file('hello', 's3://my-bucket/tmp/hello.txt', mode='overwrite')
write_file(['a', 'b', 'c'], 's3://my-bucket/tmp/list.txt', mode='append')
```

### `delete_object()`

Purpose: Delete one or more objects from a bucket; supports wildcard patterns.

Description: If the `src_key` contains `*`, lists matching files and deletes each. Otherwise deletes the single key.

Args:

- src_bucket (str): bucket name
- src_key (str): key or pattern to delete

Returns:

- None

Raises:

- botocore.exceptions.ClientError

Example:

```python
delete_object('my-bucket', 'tmp/*.log')
delete_object('my-bucket', 'tmp/file.txt')
```

### `delete_file()`

Purpose: Delete an S3 object specified by full `s3://` path.

Args:

- file_path (str): `s3://bucket/key`
- options (dict, optional): reserved for future use

Returns:

- None

Example:

```python
delete_file('s3://my-bucket/path/file.csv')
```

### `copy()`

Purpose: Copy one or more S3 objects from a source to a target location; optionally delete source.

Description: Parses the source and target s3 paths. If the source contains a wildcard `*`, it iterates matched files and
copies each file into the target prefix. Otherwise it copies a single file.

Args:

- source_path (str): `s3://` source object path or pattern
- target_path (str): `s3://` target prefix or key
- delete_source (bool, optional): If True, delete the source after copying. Defaults to False

Returns:

- None

Raises:

- ValueError: If target path contains `*` (invalid)
- botocore.exceptions.ClientError

Example:

```python
copy('s3://my-bucket/data/*.csv', 's3://other-bucket/ingest/', delete_source=False)
```

Notes:

- The function constructs the target key by appending filenames; callers must ensure `target_path` is correct.

### `grep_lines_from_file()`

Purpose: Return lines matching a text pattern from an S3 file or local string/list by delegating to `utils.grep_lines`.

Args:

- file_path (str | list): file path or inline content
- text (str): search text or pattern

Returns:

- list[str]: matching lines

Example:

```python
matches = grep_lines_from_file('s3://my-bucket/logs/app.log', 'ERROR')
```

### `truncate()`

Purpose: Remove any lines that contain a specific character and write the filtered content to `output_path`.

Args:

- input_path (str): path to read content from (supports s3://)
- output_path (str): destination S3 path where filtered file will be written
- character (str): character to filter out lines containing it

Returns:

- None

Example:

```python
truncate('s3://my-bucket/data/input.txt', 's3://my-bucket/data/output.txt', '|')
```

### `sed_command()`

Purpose: Apply sed-like scripts to a file, a list of lines, or inline strings and return the transformed lines.

Description: Accepts `scripts` (sed-like instructions) and `file_path` which can be:

- a `s3://...` string possibly containing `*` — it will read all matching files and append their lines,
- a single object path — it will read lines from that object,
- a simple string or int — wrap into a one-element list,
- a list/tuple — use as-is.

Args:

- scripts (list | str): sed-style script definitions
- file_path (str | int | list | tuple): input data or file path

Returns:

- list[str]: transformed lines returned by `utils.sed_lines`

Raises:

- Exception: If `file_path` is of unsupported type

Example:

```python
out_lines = sed_command(["s/foo/bar/g"], 's3://my-bucket/data/*.txt')
```

Notes:

- This function delegates actual sed semantics to `utils.sed_lines`.

### `extract_nth_field()`

Purpose: Extract field(s) from lines using a delimiter and field indexes.

Args:

- file_path (str | list): input S3 path, string, or list of lines
- option (str):
    - 'field': extract/cut string field by field

  or

    - 'char': extract/cut string character by character
- indexes (list[int] or int): field/char indexes to extract from
- complement (bool, optional): If True, return complement of indexes. Defaults to False
- delimiter (str, optional): field/char delimiter, default `\t`

Returns:

- list[str]: processed lines

Example:

```python
fields = extract_nth_field('s3://my-bucket/data/file.txt', None, [1,3], delimiter=',')
```

### `awk_from_file()`

Purpose: Run an awk-like sequence over records from a file, string or list.

Description: Accepts begin/iter/end operations and an optional record separator (options['record_separator']).

Args:

- file_path (str|int|list): input S3 path, inline string/int, or list/tuple
- separator (str): field separator
- begin_operations (list/str, optional): operations run before iterating records
- iter_operations (list/str, optional): operations applied per record
- end_operations (list/str, optional): operations applied after processing
- options (dict, optional): supports `record_separator` to split input differently

Returns:

- `str` : string content after operations

Example:

```python
rows = awk_from_file('s3://my-bucket/data/file.csv', ',', iter_operations=['{ print $1 }'])
```

Notes:

- For multi-file inputs the function reads and concatenates all matching files' lines before processing.

### `convert_excel_to_csv()`

Purpose: Convert an Excel (`.xlsx`) file stored on S3 to text files (one per sheet), and upload them to a target S3
prefix.

Args:

- input_s3_path (str): full S3 path of the `.xlsx` file
- output_s3_prefix (str): S3 prefix where output files will be uploaded (e.g., `s3://bucket/out/`)
- delimiter (str): field delimiter (e.g., `,` or `\t`)
- quote_char (str): quote character to surround each cell value

Returns:

- None

Raises:

- Exception: If S3 download or workbook parsing fails

Example:

```python
convert_excel_to_csv('s3://my-bucket/reports/sales.xlsx', 's3://my-bucket/reports_txt/', ',', '"')
```

Notes:

- Requires `openpyxl` available in the environment.
- Non-text cell values are stringified via Python's `str()` conversion.

### `gzip_file()` and `gzip_file_single_file()`

Purpose: Compress one or more S3 objects using gzip. Optionally delete the original(s).

Description: `gzip_file` supports wildcard source patterns and delegates to `gzip_file_single_file` per
file. `gzip_file_single_file` downloads the source file to a temporary local file, gzips it locally, then uploads
the `.gz` file. If `delete_source=True` the original is removed.

Args (gzip_file):

- executor (object): executor required by glue utils (passed through to glue helper)
- source_path (str): `s3://...` source or pattern
- target_path (str, optional): destination path for gzipped file; if None, `.gz` appended to source
- delete_source (bool, optional): whether to remove source after gzip

Args (gzip_file_single_file): same as above but for `single file only`

Returns:

- str: Target S3 path of gzipped file (for single-file variant)

Raises:

- Exception: if downloading, compressing, or uploading fails

Example:

```python
gzip_file(executor, 's3://my-bucket/logs/log1.txt', delete_source=False)
gzip_file(executor, 's3://my-bucket/data/*.log', delete_source=True)
```

### `zip_file()`

Purpose: Create zip archives present on the location `input_location`.

Usage: Pass `input_location`, `output_location`, and optionally `files_to_zip` to the helper class `ZipFiles` provided
by `aws_utils`.

Args:

- input_location (str): location to files to be zipped
- output_location (str): location to save the zip of files
- files_to_zip (list): files to be zipped

Example:

```python
zip_file('/tmp/folder', 's3://my-bucket/archive/output.zip', files_to_zip=['file1.txt', 'file2.txt'])
```

### `zip_large_file()`

Purpose: Compress very large files using `zip_large_files.S3LargeFileCompression` helper.

Args:

- source_path (str): `s3://...` source file
- target_path (str, optional): where to write compressed result
- chunk_size (int, optional): chunk size for streaming compression. Default is 104857600

Example:

```python
zip_large_file('s3://my-bucket/largefile.dat', 's3://my-bucket/largefile.zip', chunk_size=100*1024*1024)
```

### `gunzip_file()`

Purpose: Download a gzipped S3 object, decompress it in memory, and upload the decompressed content to the target S3
prefix.

Args:

- source_path (str): `s3://bucket/path/file.gz`
- target_path (str): `s3://bucket/prefix/` where decompressed file will be uploaded

Returns:

- True on success

Raises:

- Exception on failure (download, decompression, or upload)

Example:

```python
gunzip_file('s3://my-bucket/backups/db.sql.gz', 's3://my-bucket/backups/uncompressed/')
```

Notes:

- The function constructs the target key by stripping the `.gz` suffix from the source filename.

### `unzip_file()`

Purpose: Unzip a zip archive stored on S3 and upload each extracted file to the target S3 prefix.

Args:

- zip_s3_path (str): `s3://bucket/path/archive.zip`
- target_path (str, optional): target prefix `s3://bucket/target_prefix/`. If omitted, uses the zip file's containing
  prefix.

Returns:

- None

Raises:

- Exception on download or upload failures

Example:

```python
unzip_file('s3://my-bucket/archives/data.zip', 's3://my-bucket/archives/extracted/')
```

Notes:

- Extraction happens in memory; large zip archives may require large memory. For large archives consider streaming or
  using a temporary file.

---

Generated from `com/impetus/idw/wmg/common/s3_utils.py` (functions listed reflect that source file).

---

## Related Modules

- `com.impetus.idw.wmg.common.utils` - Utility functions for pattern matching, text processing
- `com.impetus.idw.wmg.common.aws_utils` - AWS-specific utilities including `ZipFiles` class
- `com.impetus.idw.wmg.common.glue_utils` - AWS Glue job utilities
- `com.impetus.idw.wmg.common.zip_large_files` - Large file compression utilities