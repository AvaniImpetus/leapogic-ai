# domain_doc_validator.py

## Overview

`domain_doc_validator.py` provides the `DomainDocumentValidator` step (subclass of `GlueELTStep`) to validate
domain-level documentation files in S3 against a set of template files.

It is intended to help ensure that documentation artifacts (Excel workbooks, CSVs, PDFs, DOCX, etc.) provided for a
domain match an expected set of templates. The step compares file presence, workbook sheet names and column headers and
generates a single Excel validation report. It can also email the resulting report when email parameters are supplied.

This guide explains the step's purpose, how it works, the parameters it expects, the key methods and their behavior,
examples for testing, best practices, and troubleshooting tips.

---

## High-level behavior

- Lists files in a `template` S3 prefix and `domain` S3 prefix.
- Normalizes filenames so minor differences (spaces, underscores, domain part) don't break matching.
- For Excel files, compares sheet names and header columns per sheet.
- Flags missing files, missing sheets, missing columns, extra columns and extra files present in domain folder.
- Produces an Excel report with per-template-row outcomes and uploads it to the configured output prefix.
- Optionally sends the report by email when `SENDER_EMAIL` and `RECIPIENT_EMAIL` parameters are provided.

---

## Expected parameters

Callers set job parameters (the step reads them using `self.get_param_value`). The important keys are:

- `BUCKET_NAME` (str) — S3 bucket where template and domain files live (required)
- `TEMPLATE_PREFIX` (str) — S3 prefix (folder) for template files; step lists files under this prefix (required)
- `DOMAIN_PREFIX` (str) — S3 prefix for domain files to validate (required)
- `OUTPUT_PREFIX` (str) — S3 prefix where the generated Excel report will be saved (required)
- `DOMAIN_NAME` (str) — short name of the domain; used when normalizing filenames and constructing report names (
  required)
- `SENDER_EMAIL` (str) — optional; if provided (and `RECIPIENT_EMAIL` present) the step attempts to email the generated
  report
- `RECIPIENT_EMAIL` (str) — optional; recipient for the emailed report

Notes:

- The step raises ValueError when any of the required values is missing.
- `SENDER_EMAIL` and `RECIPIENT_EMAIL` are optional — if either is missing the step skips sending email but still writes
  the report.

---

## Key methods and behavior

This section describes each method and what it does.

### executeFlow(self, executor, **kwargs)

The single public entry that orchestrates the validation. Steps performed:

1. Read and validate required parameters.
2. Initialize an S3 client via `boto3.client('s3')`.
3. Use `get_file_names(bucket, prefix)` to list files under template and domain prefixes.
4. Normalize domain filenames with `normalize_filename` so templates and domain files compare more robustly.
5. Iterate over template files:
    - If file is an Excel workbook (`.xlsx`/`.xls`) then call `get_excel_sheet_columns` for both template and matching
      domain file (if present).
    - Compare sheet-level columns and build report rows
      indicating `MATCHED`, `MISSING_FILE`, `MISSING_SHEET`, `MISSING_COLUMNS` or similar statuses.
    - For non-Excel files, only file presence/absence is validated.
6. Find and report extra files that are present in the domain prefix but not in template list.
7. Build a pandas DataFrame with report rows and write an Excel report to S3.
8. Optionally send an email with the report path as an attachment if `SENDER_EMAIL` and `RECIPIENT_EMAIL` are provided.

Important implementation notes:

- The method uses localized IST (UTC+5:30) timestamps to construct the report name.
- Uploading is performed by `pandas.DataFrame.to_excel(output_file_path, ...)` in the code; ensure the environment
  permits writing to S3 paths or adapt this to use the project's `s3_utils` helper to perform a safe upload when
  required.

---

## Output / Report format

The Excel report contains one row per template-sheet (or per template file for non-excel types) with the following
columns:

- Template File — file name from template prefix
- Domain File — matched file name from domain prefix or `MISSING`
- Sheet Name — sheet name (for Excel) or `N/A`
- Status — one of `MATCHED`, `MISSING_FILE`, `MISSING_SHEET`, `MISSING_COLUMNS`, `EXTRA_FILE`, etc.
- Available Columns — comma-separated columns present in domain file that match template
- Missing Columns — comma-separated missing columns relative to template
- Extra Columns — comma-separated extra columns present in domain file but not in template

The file is named like: `{DOMAIN_NAME}_validation_report_<YYYYMMDD_HHMMSS>.xlsx` and is written
to `s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/{file}`.

---

## Examples

### Example 1 — Quick local test with a mock s3_utils

You can unit-test the core logic by mocking `s3u.list_files` and the S3 client responses for Excel bytes.

```py
from com.impetus.idw.wmg.common.domain_doc_validator import DomainDocumentValidator

class FakeS3Client:
    def get_object(self, Bucket, Key):
        # Return a simple Excel workbook bytes for testing
        import pandas as pd
        from io import BytesIO
        df = pd.DataFrame({'col1': [], 'col2': []})
        bio = BytesIO()
        with pd.ExcelWriter(bio) as w:
            df.to_excel(w, sheet_name='Sheet1', index=False)
        bio.seek(0)
        return {'Body': BytesIO(bio.read())}

# Monkeypatch s3u.list_files to return test file names
import com.impetus.idw.wmg.common.s3_utils as s3u
s3u.list_files = lambda bucket, prefix: ['templates/Account_Template.xlsx']

# Instantiate and set parameters
step = DomainDocumentValidator()
step.set_param_value('BUCKET_NAME', 'my-bucket')
step.set_param_value('TEMPLATE_PREFIX', 'templates/')
step.set_param_value('DOMAIN_PREFIX', 'domains/account/')
step.set_param_value('OUTPUT_PREFIX', 'reports/')
step.set_param_value('DOMAIN_NAME', 'Account')
# Provide emails if you want to test send_mail flow; otherwise omit

# Call executeFlow with a fake executor (not used directly for S3 reads in this step)
step.executeFlow(executor=None)

# Verify that the report is generated (inspect s3u output or where to_file writes)
```

---
