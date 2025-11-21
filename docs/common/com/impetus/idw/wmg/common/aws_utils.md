# aws_utils.py — AWS utilities reference

## Overview

This module contains helper functions for common S3 operations, simple SES email sending, and basic archive/file
manipulations used across the codebase. Functions operate on S3 paths (formatted as `s3://bucket/key...`) and use boto3.
Examples below are beginner-friendly and show how to call each function; real runs require AWS credentials and
appropriate IAM permissions.

---

## Methods (brief index)

- `merge_files`
- `gets3List`
- `gets3ByPattern`
- `ZipFiles`
- `unZipFiles`
- `readS3FileAsString`
- `sendEmail`
- `renameS3Object`
- `touchfile`
- `FileOperations`
- `removeS3Object`
- `write_text_to_file`

---

## `merge_files()`

Purpose
: Read two text files from S3, concatenate them with a newline, and upload the result back to S3.

Args:

- `bucket_name (str)` — S3 bucket name containing both source files.
- `file1_key (str)` — Key (path) of the first file in the bucket.
- `file2_key (str)` — Key (path) of the second file in the bucket.
- `destination_key (str)` — Key where the merged file will be written.

Returns
: None (uploads merged bytes to S3).

Raises
: 
- `botocore.exceptions.ClientError` — on S3 errors (missing bucket/key, permissions).
- `UnicodeDecodeError` — if file bytes cannot be decoded as UTF-8.

Example:

```
merge_files(
    bucket_name='my-bucket',
    file1_key='path/to/file1.txt',
    file2_key='path/to/file2.txt',
    destination_key='path/to/merged.txt'
)
```

Notes:

- Intended for reasonably small text files (function loads both files fully into memory).
- Overwrites `destination_key` if it exists.

---

## `gets3List()`

Purpose: List objects under an S3 prefix with options for recursive listing, returning full paths, date filtering, and
including folders.

Args:

- `Path (str)` — S3 path like `s3://bucket/prefix/`.
- `fullPath (str, optional)` — `'yes'` (default) to return full `s3://...` paths or `'no'` to return relative keys.
- `recursive (str, optional)` — `'yes'` to include nested folders, `'no'` (default) for current level only.
- `daysbefore (int, optional)` — return objects older than N days.
- `folderFlag (str, optional)` — `'Y'` to include folder keys (objects ending with `/`) or `'N'` (default) to exclude.

Returns: `list` — matching S3 paths or keys depending on `fullPath`.

Raises:

- `botocore.exceptions.ClientError` — if S3 list operation fails.
- `ValueError` — if `Path` does not follow expected `s3://bucket/...` format (may raise indirectly).

Example:

```py
files = gets3List('s3://my-bucket/data/', recursive='yes', fullPath='no', daysbefore=7)
```

Notes:

- Uses `list_objects_v2` and inspects `LastModified` for date filtering. Timezone info is normalized by dropping tz.
- Not a paginator-aware implementation: if a prefix has many (>1000) objects, consider paginating or partitioning the
  prefix.

---

## `gets3ByPattern()`

Purpose: Find objects under an S3 prefix whose full path matches a regular expression pattern.

Args:

- `Path (str)` — Base S3 path (e.g. `s3://bucket/prefix/`).
- `Pattern (str)` — Regular expression used to match the full path (pattern is appended to `Path` internally).
- `**kwargs` — Any argument supported by `gets3List` (filtering options are passed through).

Returns: `list` — S3 paths that match the provided regex.

Raises:

- `re.error` — if the provided regex is invalid.
- `botocore.exceptions.ClientError` — if S3 listing fails.

Example:

```py
csvs = gets3ByPattern('s3://my-bucket/data/', r'.*\\.csv', recursive='yes')
```

Notes:

- Pattern matching is case-sensitive and applied against the full path.
- The function builds a regex of the form `(Path + Pattern)$`, so supply a pattern fragment (e.g. `r'.*\\.csv'`).

---

## `ZipFiles()`

Purpose:
Compress files from an S3 prefix into an in-memory ZIP and upload the ZIP to S3.

Args: Keyword args (passed as `**kwargs`):

- `location (str)` — Required. Source S3 directory (`s3://bucket/prefix/`).
- `filename (str, optional)` — Base name for zip (no `.zip`). If not provided and `zip_location` omitted, behavior will
  set a key using prefix.
- `filetype (str, optional)` — `'csv'`, `'all'`, or specific extension (default `'all'`).
- `files_to_zip (list, optional)` — Explicit list of file name fragments to include.
- `zip_location (str, optional)` — S3 path where ZIP should be written instead of the source bucket/prefix.

Returns: None (uploads created ZIP to S3).

Raises:

- `botocore.exceptions.ClientError` — for S3 errors.
- `zipfile.BadZipFile` — if ZIP creation fails unexpectedly.
- `ValueError` — on missing required parameters.

Example:

```py
ZipFiles(location='s3://my-bucket/reports/', filename='monthly_reports', filetype='csv')
```

Notes:

- Only files at the immediate prefix level (not nested) are included.
- ZIP is built in memory via `BytesIO`, so large payloads may increase memory usage.
- When `zip_location` is provided the ZIP key is taken from that path; otherwise it's saved under the source prefix.

---

## `unZipFiles()`

Purpose:
Download a ZIP stored in S3, extract files, and upload extracted files back to the same S3 prefix.

Args:
Keyword args (passed as `**kwargs`):

- `location (str)` — Required S3 directory containing the ZIP (e.g. `s3://bucket/prefix/`).
- `file (str)` — ZIP filename (e.g. `archive.zip`).

Returns: None

Raises:

- `botocore.exceptions.ClientError` — for S3 read/write errors.
- `zipfile.BadZipFile` — if the file is not a valid ZIP.

Example:

```py
unZipFiles(location='s3://my-bucket/archives/', file='backup.zip')
```

Notes:

- Directory structure inside the ZIP is flattened: only the filename is preserved.
- Folders inside the ZIP are skipped.
- The original ZIP file is not deleted by this function.

---

## `readS3FileAsString()`

Purpose:
Read a file object from S3 and return its raw bytes. (Despite the name, it returns bytes — callers should decode if they
need text.)

Args:

- `filePath (str)` — Full S3 path to the object (`s3://bucket/key`).

Returns: `bytes` — Raw content of the file.

Raises:

- `botocore.exceptions.ClientError` — if object is missing or permissions prevent read.
- `ValueError` — if provided path is malformed (may raise indirectly).

Example:

```py
data = readS3FileAsString('s3://my-bucket/config/settings.json')
text = data.decode('utf-8')
```

Notes:

- Loads entire object into memory — for very large objects prefer streaming.

---

## `sendEmail()`

Purpose:
Send an email using AWS SES v2 with optional S3-based attachments.

Args:

- Keyword args (passed as `**kwargs`):
    - `sender (str)` — Sender email (must be SES-verified in some regions/accounts).
    - `recipient (str)` — Semicolon-separated recipients (e.g. `'a@x.com;b@y.com'`).
    - `subject (str)` — Email subject.
    - `body_txt (str)` — Plain-text message body.
    - `attachments (list[str], optional)` — List of S3 paths to files or folders. Folder paths ending with `/` will
      expand to files in that prefix.
    - `aws_region (str, optional)` — SES region. Falls back to `AWS_DEFAULT_REGION` environment variable if omitted.

Returns
: None (sends email via SES).

Raises:

- `ValueError` — if required parameters are missing.
- `botocore.exceptions.ClientError` — on SES failures (unverified sender, quota, etc.).
- `Exception` — for generic failures while building or sending the message.

Example:

```py
sendEmail(
    sender='noreply@example.com',
    recipient='user@example.com;other@example.com',
    subject='Monthly Report',
    body_txt='See attached',
    attachments=['s3://my-bucket/reports/report.pdf']
)
```

Notes:

- Attachments are fetched by reading S3 objects into memory and attached to a multipart MIME message.
- Sender must be verified in SES when required; recipients may also be restricted in sandbox accounts.
- For large attachments consider uploading to a shared S3 link instead of embedding in email.

---

## `renameS3Object()`

Purpose
: Rename a single `part-*` file inside a directory to the directory's name. Commonly used to tidy Spark/Hadoop output
that creates `part-xxxx` files.

Args
: Keyword args (passed as `**kwargs`):
- `renamePartFile (str)` — Must be `'yes'` to enable behavior.
- `Path (str)` — Directory path where the part file lives (e.g. `s3://bucket/prefix/dirname/`).

Returns
: None

Raises:

- `botocore.exceptions.ClientError` — on S3 operations.
- `IndexError` / `ValueError` — if path parsing fails or assumptions about structure are violated.

Example:

```py
renameS3Object(renamePartFile='yes', Path='s3://my-bucket/output/daily_report/')
```

Notes:

- Function only proceeds when exactly one `part.*` object is found in the directory; otherwise it returns silently.
- It performs a copy (to new name) then deletes the original part file.

---

## `touchfile()`

Purpose
: Create an empty (0-byte) object on S3 if it doesn't exist, similar to the Unix `touch` command.

Args:

- `path (str)` — Full S3 path to create (e.g. `s3://bucket/markers/done.flag`).

Returns
: `bool` — `True` if object exists or was created successfully; `False` on error.

Raises
: None explicitly (exceptions are logged and the function returns `False`).

Example
: 
```py
ok = touchfile('s3://my-bucket/markers/job_complete.marker')
```

Notes
: 
- Uses `list_objects_v2` to detect presence. If absent it calls `put_object` with empty body.
- Returns `True` even when object already exists (mirrors Unix touch semantics).

---

## `FileOperations()`

Purpose
: Copy/move/rename files or directories in S3. Also supports a `renamePartFile` helper path for Spark-like outputs.

Args
: Keyword args (passed as `**kwargs`):
- `srcFilePath (str)` — Source S3 path (file or directory, trailing `/` denotes directory).
- `tgtFilePath (str)` — Target S3 path.
- `Action (str, optional)` — One of `'copy'`, `'move'`, or `'rename'` (default `'copy'`).
- `renamePartFile (str, optional)` — `'yes'` triggers the part-rename flow (requires `Path`).
- `Path (str, optional)` — Directory path used when `renamePartFile='yes'`.
- `Error (str, optional)` — `'Y'` to raise exceptions on missing source, `'N'` (default) to log and return.

Returns
: None

Raises
: 
- `Exception` — re-raised for unexpected failures or when `Error='Y'` and a missing source is detected.
- `botocore.exceptions.ClientError` — on S3 errors.

Example
: Copy a file to a directory (filename appended automatically):
```py
FileOperations(srcFilePath='s3://a-bucket/data/file.txt', tgtFilePath='s3://b-bucket/archive/', Action='copy')
```

Notes
: 
- Directory-to-directory moves copy all objects under the source prefix to the destination prefix and delete the source
objects for `move`/`rename`.
- When copying a single file to a directory, the source filename is appended to the destination prefix.
- The implementation assumes simple path parsing (does not validate every edge-case for malformed S3 URLs) and
uses `copy_from` + optional delete for atomicity.

---

## `removeS3Object()`

Purpose
: Delete an S3 file or all files at a given prefix. Designed to support both single-file and directory deletions.

Args
: Keyword args (passed as `**kwargs`):
- `Path (str)` — S3 path to delete (`s3://bucket/file` or `s3://bucket/prefix/`).
- `removeFolderFlag (str, optional)` — `'Y'` to remove folder objects, `'N'` to skip; note current implementation sets
this to `'N'` internally.

Returns
: None

Raises:

- `botocore.exceptions.ClientError` — on S3 errors.
- `KeyError` — if the S3 `list_objects_v2` response does not contain expected keys.

Example
: 
```py
removeS3Object(Path='s3://my-bucket/temp/')
removeS3Object(Path='s3://my-bucket/data/file.txt')
```

Notes:

- Deletion is immediate and irreversible. Use carefully in production.
- For large prefixes consider batch deletes or S3 lifecycle rules to avoid large request volumes.

---

## `write_text_to_file()`

Purpose
: Write text content to an S3 object (create or overwrite).

Args
: 
- `path (str)` — S3 path like `s3://bucket/key` where the object should be written.
- `body (Any, optional)` — Content to write. If omitted, an empty string is written.

Returns
: `bool` — `True` on success; may return `False` if the path format is invalid.

Raises
: 
- `botocore.exceptions.ClientError` — on S3 write errors.
- `botocore.exceptions.NoCredentialsError` — if AWS credentials are missing.
- `ValueError` — if `path` is malformed (may be raised indirectly).

Example
: 
```py
write_text_to_file('s3://my-bucket/reports/daily.txt', body='Job finished successfully')
```

Notes
: 
- Overwrites any existing object at the given key. There is no automatic versioning or backup.
- Useful for small logs, JSON blobs, or marker files. Avoid for very large binary payloads.

---
