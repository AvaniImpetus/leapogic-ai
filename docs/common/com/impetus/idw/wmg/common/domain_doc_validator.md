# DomainDocumentValidator Class

## Overview
The `DomainDocumentValidator` class is a Glue ELT step that validates domain-level documentation files stored in S3 against a set of template files. It ensures that documentation artifacts (such as Excel workbooks, CSVs, PDFs, DOCX files, etc.) for a specific domain match the expected templates by comparing file presence, workbook sheet names, and column headers. The class generates an Excel validation report and optionally sends it via email. It inherits from `GlueELTStep` and relies on S3 utilities for file operations.

## What It Does
- Lists files in specified template and domain S3 prefixes.
- Normalizes filenames to handle minor differences (e.g., spaces, underscores, domain name variations) for accurate matching.
- For Excel files, compares sheet names and column headers between template and domain files, identifying matches, missing sheets, missing columns, and extra columns.
- For non-Excel files, checks only for file presence or absence.
- Flags discrepancies such as missing files, extra files in the domain folder, and column mismatches.
- Produces a timestamped Excel report summarizing validation outcomes and uploads it to the configured S3 output prefix.
- Optionally sends the report as an email attachment if sender and recipient email parameters are provided.

## Arguments
The class expects the following job arguments, retrieved via `self.get_param_value`:

- `BUCKET_NAME` (required): S3 bucket name where template and domain files are located.
- `TEMPLATE_PREFIX` (required): S3 prefix (folder path) for template files.
- `DOMAIN_PREFIX` (required): S3 prefix for domain files to be validated.
- `OUTPUT_PREFIX` (required): S3 prefix where the generated Excel report will be saved.
- `DOMAIN_NAME` (required): Short name of the domain, used for filename normalization and report naming.
- `SENDER_EMAIL` (optional): Email address for sending the report. Required for email functionality.
- `RECIPIENT_EMAIL` (optional): Recipient email address for the report. Required for email functionality.

## Usage
The `DomainDocumentValidator` class is designed for ETL workflows to automate documentation validation. It compares domain-specific files against templates, highlighting inconsistencies to ensure compliance. The output is a comprehensive Excel report that can be reviewed manually or integrated into further processing. Email notification provides immediate alerts for validation results.

### Key Features
- **Filename Normalization**: Handles variations in filenames (e.g., spaces vs. underscores, domain name inclusion) to improve matching accuracy.
- **Excel-Specific Validation**: Deep comparison of Excel workbooks, including sheet names and column headers.
- **Comprehensive Reporting**: Generates detailed Excel reports with statuses like MATCHED, MISSING_FILE, MISSING_SHEET, MISSING_COLUMNS, and EXTRA_FILE.
- **Email Integration**: Optional email sending of the report for notifications.
- **S3 Integration**: Direct reading from and writing to S3 buckets using boto3 and custom S3 utilities.

## Exceptions
- **ValueError**: Raised if any required parameters (`BUCKET_NAME`, `TEMPLATE_PREFIX`, `DOMAIN_PREFIX`, `OUTPUT_PREFIX`, `DOMAIN_NAME`) are missing or empty.
- **S3-Related Exceptions**: May occur during file listing, object retrieval, or upload operations (e.g., boto3 exceptions for invalid buckets, keys, or permissions).
- **Excel Parsing Errors**: Logged as errors if Excel files cannot be read (e.g., corrupted files), but processing continues with "ERROR" placeholders in the report.
- **Email Sending Failures**: If email parameters are provided but sending fails (e.g., SMTP issues), it is logged as an error, but the report is still generated and uploaded.

## How to Use It
1. Prepare template files in an S3 bucket under the specified `TEMPLATE_PREFIX`.
2. Place domain files to validate under the `DOMAIN_PREFIX` in the same bucket.
3. In the Glue/ELT framework, configure the job with the required parameters:
   - `BUCKET_NAME`: The S3 bucket.
   - `TEMPLATE_PREFIX`, `DOMAIN_PREFIX`, `OUTPUT_PREFIX`: Respective S3 prefixes.
   - `DOMAIN_NAME`: Domain identifier.
   - Optionally, `SENDER_EMAIL` and `RECIPIENT_EMAIL` for notifications.
4. Instantiate `DomainDocumentValidator` and run it via the framework's executor.


