# Teradata to PySpark (AWS Glue) Conversion Guide - LeapLogic

## Overview

This document details how **LeapLogic** converts Teradata database objects to **PySpark** for AWS Glue. LeapLogic's automated migration engine analyzes Teradata schemas, queries, and logic, then generates equivalent PySpark code following AWS best practices.

---

## 1. Data Type Conversion

LeapLogic automatically converts Teradata data types to their PySpark equivalents during migration. Below are the detailed conversions performed by LeapLogic.

### Integer Type Conversions

**BYTEINT → int**  
LeapLogic converts Teradata's BYTEINT data type (storing values from -128 to 127) to PySpark's standard `int` type. When LeapLogic encounters a column like `status BYTEINT`, it generates PySpark code with `status int`. This is commonly used for boolean-like flags, status codes, or small counters. For example, values like 1 (active), 0 (inactive), or -1 (error state) are preserved during conversion.

**SMALLINT → int**  
When LeapLogic processes Teradata's SMALLINT type (handling integers from -32,768 to 32,767), it converts them to PySpark's generic `int` type. This conversion is applied to year values, department codes, or IDs in smaller lookup tables. For instance, LeapLogic transforms `year SMALLINT` to `year int`, maintaining values like 2025.

**INTEGER → int**  
LeapLogic converts Teradata's INTEGER type (supporting values from -2,147,483,648 to 2,147,483,647) directly to PySpark's `int` type. This is the most frequently encountered integer conversion, used for primary keys, foreign keys, customer IDs, and counting operations. A column like `customer_id INTEGER` is converted by LeapLogic to `customer_id int`, preserving values such as 123456.

**BIGINT → bigint**  
LeapLogic converts Teradata's BIGINT type to PySpark's `bigint` type for handling very large integers (range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807). This conversion is essential for very large transaction IDs, timestamps in milliseconds, or massive aggregation results. LeapLogic transforms `transaction_id BIGINT` to `transaction_id bigint`, preserving values like 9876543210.

### Decimal and Floating-Point Type Conversions

**DECIMAL(18,2) → decimal(18,2)**  
LeapLogic preserves the exact precision of Teradata's DECIMAL type when converting to PySpark. For DECIMAL(18,2) (18 total digits with 2 decimal places), LeapLogic generates PySpark code with `decimal(18,2)`, maintaining precision critical for monetary values. When LeapLogic converts a `price DECIMAL(18,2)` column, it ensures values like 1234.56 remain exact, preventing floating-point rounding errors in financial calculations.

**NUMERIC(10,5) → double**  
LeapLogic converts Teradata's NUMERIC type to PySpark's `double` type for greater flexibility in mathematical calculations. When encountering NUMERIC(10,5) (10 total digits with 5 decimal places), LeapLogic generates `double` type in PySpark, though this sacrifices the exact precision guarantee. For example, `tax_rate NUMERIC(10,5)` is converted by LeapLogic to `tax_rate double`, storing values like 0.08750 (8.75% tax rate).

**FLOAT → float**  
LeapLogic converts Teradata's FLOAT type directly to PySpark's `float` type for approximate floating-point calculations. This conversion is suitable for scientific calculations where some precision loss is acceptable. LeapLogic transforms columns like `measurement FLOAT` to maintain approximate values such as 3.14159, commonly used in physics calculations or statistical measurements.

**REAL → double**  
When LeapLogic encounters Teradata's REAL type (single-precision floating-point), it upgrades to PySpark's `double` type for better precision and to avoid potential accuracy issues. LeapLogic converts `coefficient REAL` to `coefficient double`, preserving values like 2.71828 (Euler's number) with improved precision.

**DOUBLE PRECISION → String**  
In certain cases, LeapLogic converts Teradata's DOUBLE PRECISION type to PySpark's `String` type rather than `double`. This conversion strategy preserves full precision where floating-point representation might introduce errors. LeapLogic transforms `precise_value DOUBLE PRECISION` to `precise_value String`, storing "3.141592653589793" as text to maintain exact representation.

### Character and String Type Conversions

**CHAR(10) → VARCHAR(10)**  
LeapLogic converts Teradata's fixed-length CHAR type to PySpark's `VARCHAR` type with the same length constraint. While CHAR always uses fixed storage (padding with spaces), LeapLogic generates VARCHAR code in PySpark for more flexible handling. For example, `country_code CHAR(10)` storing "USA" (padded in Teradata) is converted by LeapLogic to `country_code VARCHAR(10)` in PySpark.

**VARCHAR(100) → varchar**  
LeapLogic converts Teradata's VARCHAR type directly to PySpark's `varchar` type, maintaining the maximum length constraint. This straightforward conversion handles variable-length strings efficiently. LeapLogic transforms `customer_name VARCHAR(100)` to `customer_name varchar`, preserving names like "John Doe" with identical behavior.

**CLOB → string**  
LeapLogic converts Teradata's CLOB (Character Large Object) type to PySpark's `string` type for handling very large text data. When migrating columns storing document contents, detailed descriptions, JSON payloads, or XML data, LeapLogic generates `string` type code. For example, `description CLOB` is converted by LeapLogic to `description string`, capable of storing entire article contents or log files.

**BLOB → binary**  
LeapLogic converts Teradata's BLOB (Binary Large Object) type to PySpark's `binary` type for handling byte arrays. This conversion is used for images, PDFs, audio files, encrypted data, or any non-text binary content. LeapLogic transforms `profile_image BLOB` to `profile_image binary`, preserving the raw bytes of image files or other binary content.

### Date and Time Type Conversions

**DATE → date**  
LeapLogic converts Teradata's DATE type directly to PySpark's `date` type with no transformation needed. This conversion preserves calendar dates (year, month, day) for birthdates, order dates, and deadline dates. LeapLogic transforms `order_date DATE` to `order_date date`, maintaining values like 2025-11-18.

**TIME → timestamp**  
LeapLogic converts Teradata's TIME type (storing time of day without a date component) to PySpark's `timestamp` type, since PySpark doesn't have a separate TIME type. The conversion includes both date and time components, with the date typically set to a default epoch date. LeapLogic transforms `login_time TIME` to `login_time timestamp`, representing 14:30:00 as a full timestamp like 1970-01-01 14:30:00.

**TIMESTAMP → timestamp**  
LeapLogic converts Teradata's TIMESTAMP type directly to PySpark's `timestamp` type, preserving both date and time components. This conversion is crucial for audit trails, transaction records, and event logging. LeapLogic transforms `created_at TIMESTAMP` to `created_at timestamp`, maintaining precise moments like 2025-11-18 14:30:00.123.

### Interval Type Conversions

**INTERVAL YEAR(2) → INTERVAL**  
LeapLogic converts Teradata's INTERVAL YEAR type (representing durations in years) to PySpark's generic `INTERVAL` type. When encountering interval types with precision (2 means up to 99 years), LeapLogic generates appropriate PySpark INTERVAL code for age calculations, employment duration, or year-based time spans. For example, `age INTERVAL YEAR(2)` is converted by LeapLogic to `age INTERVAL`, representing durations like 25 years.

**INTERVAL MONTH(2) → INTERVAL**  
LeapLogic converts Teradata's INTERVAL MONTH type to PySpark's `INTERVAL` type for representing durations in months. This conversion handles subscription periods, contract durations, or warranty lengths with precision up to 99 months. LeapLogic transforms `subscription INTERVAL MONTH(2)` to `subscription INTERVAL`, storing durations like 12 months (1 year subscription).

---

## 2. Query Type Conversion

LeapLogic automatically analyzes and converts different SQL query types from Teradata to PySpark-compatible syntax.

### UPDATE Queries → UPDATE or MERGE INTO

LeapLogic intelligently converts Teradata UPDATE statements based on query complexity. For simple UPDATE operations on single tables, LeapLogic maintains the UPDATE syntax. However, when UPDATE involves subqueries or multiple tables, LeapLogic converts to MERGE INTO statements for better performance in distributed systems.

**Simple UPDATE (LeapLogic maintains UPDATE syntax):**

_Teradata:_

```sql
UPDATE employees
SET salary = salary * 1.10,
    last_updated = CURRENT_TIMESTAMP
WHERE department = 'Engineering'
  AND performance_rating >= 4;
```

_LeapLogic Converts to PySpark:_

```sql
UPDATE default.employees
SET employees.salary = salary * 1.10,
    employees.last_updated = CURRENT_TIMESTAMP()
WHERE department = 'Engineering'
  AND performance_rating >= 4;
```

For straightforward updates on single tables without subqueries, LeapLogic preserves the UPDATE syntax as it performs efficiently in PySpark.

**Complex UPDATE with Subqueries/Multiple Tables (LeapLogic converts to MERGE INTO):**

_Teradata:_

```sql
UPDATE target
FROM OEAETLP1_T.NS_HCI_INOV_CLM_LAG target,
     (
         SELECT REC_SEQ_NBR,
                CLM_NBR,
                CASE
                    WHEN LENGTH(CLM_LN_NBR) = 1
                    THEN '0' || CLM_LN_NBR
                    ELSE CLM_LN_NBR
                END AS CLM_LN_NBR
         FROM (
             SELECT REC_SEQ_NBR,
                    CLM_NBR,
                    CAST(ROW_NUMBER() OVER(
                        PARTITION BY CLM_NBR
                        ORDER BY CLM_NBR, CLM_LN_NBR, DOS, REC_SEQ_NBR
                    ) AS VARCHAR(4)) AS CLM_LN_NBR
             FROM OEAETLP1.NSV_HCI_INOV_CLM_LAG
         ) A
     ) AS source
SET CLM_LN_NBR = source.CLM_LN_NBR
WHERE target.REC_SEQ_NBR = source.REC_SEQ_NBR;
```

_LeapLogic Converts to PySpark:_

```sql
MERGE INTO {self.get_param_value("glue_catalog")}.{self.get_param_value("schemas.oeaetlp1_t")}.ns_hci_inov_clm_lag AS target
USING (
    SELECT rec_seq_nbr,
           clm_nbr,
           CASE
               WHEN rtrim(length(clm_ln_nbr)) = 1 THEN '0' || clm_ln_nbr
               ELSE clm_ln_nbr
           END AS clm_ln_nbr
    FROM (
        SELECT rec_seq_nbr,
               clm_nbr,
               RPAD(ROW_NUMBER() OVER (
                   PARTITION BY rtrim(clm_nbr)
                   ORDER BY clm_nbr, clm_ln_nbr, dos, rec_seq_nbr
               ), 4) AS clm_ln_nbr
        FROM {self.get_param_value("glue_catalog")}.{self.get_param_value("schemas.oeaetlp1_t")}.ns_hci_inov_clm_lag
    ) AS a
) source
ON target.rec_seq_nbr = source.rec_seq_nbr
WHEN MATCHED THEN
    UPDATE SET target.clm_ln_nbr = source.clm_ln_nbr;
```

When LeapLogic detects subqueries or multi-table UPDATE operations, it converts to MERGE INTO to ensure atomic operations and better handle concurrent modifications in distributed environments.

### DELETE Queries → MERGE INTO or DELETE

LeapLogic intelligently determines the best conversion strategy for DELETE statements based on query complexity. For simple DELETE operations, LeapLogic maintains the DELETE syntax. For complex scenarios involving joins or subqueries, LeapLogic converts to MERGE INTO patterns for better performance.

**Simple DELETE (LeapLogic maintains syntax):**

_Teradata:_

```sql
DELETE FROM order_archive
WHERE order_date < DATE '2023-01-01';
```

_LeapLogic Converts to PySpark:_

```sql
DELETE FROM glue_catalog.default.order_archive
WHERE order_date < DATE('2023-01-01');
```

**Complex DELETE (LeapLogic converts to MERGE INTO):**

_Teradata:_

```sql
DELETE FROM inventory
WHERE product_id IN (
    SELECT product_id
    FROM products
    WHERE discontinued = 1
      AND last_sale_date < CURRENT_DATE - INTERVAL '2' YEAR
);
```

_LeapLogic Converts to PySpark:_

```sql
MERGE INTO glue_catalog.default.inventory alias_t1
USING (
    SELECT DISTINCT product_id
    FROM glue_catalog.default.products
    WHERE discontinued = 1
      AND last_sale_date < DATE_SUB(CURRENT_DATE(), CAST(<<INTERVAL_CONSTANT>>SECOND_STRING'2 null' AS int))
) AS alias_t2
ON alias_t1.product_id = alias_t2.product_id
WHEN MATCHED THEN DELETE;
```

LeapLogic's intelligent conversion provides better performance for complex delete operations and ensures consistency in distributed systems.

### INSERT Queries → INSERT

LeapLogic converts Teradata INSERT statements to PySpark with appropriate data type casting and NULL handling. While basic INSERT syntax remains similar, LeapLogic adds necessary transformations for data type compatibility and constraint handling.

**Basic INSERT (LeapLogic maintains structure):**

_Teradata:_

```sql
INSERT INTO AIWMRGDLP1_T.AS_POC_HMK_ENROLLMENT (
    REC_INS_USER_ID,
    MAIL_ID,
    MBR_ENRL_VERS_KEY
)
VALUES (123, 'jane.doe@gmail.com', 321);
```

_LeapLogic Converts to PySpark:_

```sql
INSERT INTO TABLE glue_catalog.aiwmrgdlp1_t.as_poc_hmk_enrollment (
    rec_ins_user_id,
    rec_ins_ts,
    mbr_key
)
VALUES (123, 'jane.doe@gmail.com', 321);
```

**INSERT with SELECT (LeapLogic adds casting/coalescing):**

_Teradata:_

```sql
 INSERT
      INTO
          AIWMRGDLP1_T.AS_POC_HMK_ENROLLMENT
          ( REC_INS_USER_ID, MBR_KEY, MBR_ENRL_VERS_KEY) SELECT
              REC_INS_USER_ID,
              MBR_KEY,
              MBR_ENRL_VERS_KEY
          FROM
              AIWMRGDLP1_T.AW_M_HMK_ENROLLMENT_00 QUALIFY ROW_NUMBER() OVER (PARTITION
          BY
              MBR_KEY
          ORDER BY
              REC_INS_TS DESC) = 1;



```

_LeapLogic Converts to PySpark:_

```sql
INSERT
    INTO
        TABLE
        glue_catalog.aiwmrgdlp1_t.as_poc_hmk_enrollment              (rec_ins_user_id, mbr_key, mbr_enrl_vers_key)     SELECT
            CAST ( COALESCE( derivedTable.rec_ins_user_id , '')  AS string )  AS rec_ins_user_id ,
            derivedTable.mbr_key ,
            COALESCE( derivedTable.mbr_enrl_vers_key , 0)  AS mbr_enrl_vers_key
        FROM
            (SELECT
                rec_ins_user_id AS rec_ins_user_id,
                mbr_key AS mbr_key,
                mbr_enrl_vers_key AS mbr_enrl_vers_key,
                ROW_NUMBER(  )  OVER ( PARTITION
            BY
                mbr_key
            ORDER BY
                rec_ins_ts DESC ) as windowFunction
            FROM
                glue_catalog.aiwmrgdlp1_t.aw_m_hmk_enrollment_00     ) as derivedTable
        WHERE
            windowFunction = 1;
```

LeapLogic ensures proper data type compatibility and handles NULL values according to target table constraints.

### TRUNCATE Queries → TRUNCATE

LeapLogic converts Teradata TRUNCATE statements directly to PySpark with minimal changes. This operation efficiently removes all rows from a table without logging individual row deletions.

**Teradata Input:**

```sql
TRUNCATE TABLE staging_customer_data;
```

**LeapLogic Converts to PySpark:**

```sql
TRUNCATE TABLE staging_customer_data;
```

LeapLogic preserves the TRUNCATE behavior across platforms, maintaining fast data removal, identity column resets, and atomic operations. In scenarios with Iceberg tables, LeapLogic applies additional considerations for maintaining table metadata and history.

---

## 3. Logical Transformations and Conversion Rules

LeapLogic applies intelligent transformation rules to ensure migrated code maintains functional equivalence while optimizing for PySpark's distributed architecture.

### QUALIFY Clause Conversion -> Derived column

**How LeapLogic Handles It:** Since PySpark/Iceberg doesn't support the QUALIFY clause, LeapLogic automatically creates a subquery (named `DerivedTable`) and moves the qualify condition to a WHERE clause in the outer query.

_Teradata:_

```sql
INSERT INTO AIWMRGDLP1_T.AS_POC_HMK_ENROLLMENT (
    REC_INS_USER_ID,
    MBR_KEY,
    MBR_ENRL_VERS_KEY
)
SELECT REC_INS_USER_ID,
       MBR_KEY,
       MBR_ENRL_VERS_KEY
FROM AIWMRGDLP1_T.AW_M_HMK_ENROLLMENT_00
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY MBR_KEY
    ORDER BY REC_INS_TS DESC
) = 1;
```

_LeapLogic Converts to PySpark:_

```sql
INSERT INTO TABLE glue_catalog.aiwmrgdlp1_t.as_poc_hmk_enrollment (
    rec_ins_user_id,
    mbr_key,
    mbr_enrl_vers_key
)
SELECT CAST(COALESCE(derivedTable.rec_ins_user_id, '') AS string) AS rec_ins_user_id,
       derivedTable.mbr_key,
       COALESCE(derivedTable.mbr_enrl_vers_key, 0) AS mbr_enrl_vers_key
FROM (
    SELECT rec_ins_user_id AS rec_ins_user_id,
           mbr_key AS mbr_key,
           mbr_enrl_vers_key AS mbr_enrl_vers_key,
           ROW_NUMBER() OVER (
               PARTITION BY mbr_key
               ORDER BY rec_ins_ts DESC
           ) AS windowFunction
    FROM glue_catalog.aiwmrgdlp1_t.aw_m_hmk_enrollment_00
) AS derivedTable
WHERE windowFunction = 1;
```

### Casting Application

**How LeapLogic Handles It:** When LeapLogic detects data type mismatches between source and target columns during INSERT operations, it automatically applies CAST functions to ensure type compatibility.

**Example:** LeapLogic generates `CAST(col as int)` when inserting into integer columns from string sources.

_Teradata:_

```sql
INSERT INTO AIWMRGDLP1_T.AS_POC_HMK_ENROLLMENT (
    REC_INS_USER_ID,
    MBR_KEY,
    MBR_ENRL_VERS_KEY
)
SELECT REC_INS_USER_ID,
       MBR_KEY,
       MBR_ENRL_VERS_KEY
FROM AIWMRGDLP1_T.AW_M_HMK_ENROLLMENT_00
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY MBR_KEY
    ORDER BY REC_INS_TS DESC
) = 1;
```

_LeapLogic Converts to PySpark:_

```sql
INSERT INTO TABLE glue_catalog.aiwmrgdlp1_t.as_poc_hmk_enrollment (
    rec_ins_user_id,
    mbr_key,
    mbr_enrl_vers_key
)
SELECT CAST(COALESCE(derivedTable.rec_ins_user_id, '') AS string) AS rec_ins_user_id,
       derivedTable.mbr_key,
       COALESCE(derivedTable.mbr_enrl_vers_key, 0) AS mbr_enrl_vers_key
FROM (
    SELECT rec_ins_user_id AS rec_ins_user_id,
           mbr_key AS mbr_key,
           mbr_enrl_vers_key AS mbr_enrl_vers_key,
           ROW_NUMBER() OVER (
               PARTITION BY mbr_key
               ORDER BY rec_ins_ts DESC
           ) AS windowFunction
    FROM glue_catalog.aiwmrgdlp1_t.aw_m_hmk_enrollment_00
) AS derivedTable
WHERE windowFunction = 1;
```

### COALESCE Application

**How LeapLogic Handles It:** For tables with NOT NULL constraints, LeapLogic automatically wraps columns with COALESCE functions to handle null entries appropriately.

**Example:** LeapLogic generates `COALESCE(col, 'Default Value')` or `COALESCE(col, 0)` based on data type.

_Teradata:_

```sql
INSERT INTO AIWMRGDLP1_T.AS_POC_HMK_ENROLLMENT (
    REC_INS_USER_ID,
    MBR_KEY,
    MBR_ENRL_VERS_KEY
)
SELECT REC_INS_USER_ID,
       MBR_KEY,
       MBR_ENRL_VERS_KEY
FROM AIWMRGDLP1_T.AW_M_HMK_ENROLLMENT_00
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY MBR_KEY
    ORDER BY REC_INS_TS DESC
) = 1;
```

_LeapLogic Converts to PySpark:_

```sql
INSERT INTO TABLE glue_catalog.aiwmrgdlp1_t.as_poc_hmk_enrollment (
    rec_ins_user_id,
    mbr_key,
    mbr_enrl_vers_key
)
SELECT CAST(COALESCE(derivedTable.rec_ins_user_id, '') AS string) AS rec_ins_user_id,
       derivedTable.mbr_key,
       COALESCE(derivedTable.mbr_enrl_vers_key, 0) AS mbr_enrl_vers_key
FROM (
    SELECT rec_ins_user_id AS rec_ins_user_id,
           mbr_key AS mbr_key,
           mbr_enrl_vers_key AS mbr_enrl_vers_key,
           ROW_NUMBER() OVER (
               PARTITION BY mbr_key
               ORDER BY rec_ins_ts DESC
           ) AS windowFunction
    FROM glue_catalog.aiwmrgdlp1_t.aw_m_hmk_enrollment_00
) AS derivedTable
WHERE windowFunction = 1;
```

### Collate Handling (PySpark-Specific)

**How LeapLogic Handles It:** Teradata handles case sensitivity automatically, but PySpark requires explicit handling. LeapLogic applies `rtrim()` for collation and `upper()` for case-insensitive comparisons.

**Conversion Pattern:** LeapLogic wraps string comparisons with `rtrim(upper(col1)) = upper('value')` for case-insensitive matching.

### View to Table Replacement

**How LeapLogic Handles It:** Since INSERT into views doesn't work in PySpark, LeapLogic automatically identifies view references in INSERT statements and replaces them with the underlying table names.

### Multi-Argument COALESCE Conversion

**How LeapLogic Handles It:** Teradata's COALESCE supports multiple arguments like `COALESCE(col1, col2, 0)`, but PySpark requires nested COALESCE functions. LeapLogic automatically nests these functions.

**Conversion Pattern:** LeapLogic transforms `COALESCE(col1, col2, col3, 0)` to `COALESCE(COALESCE(col3, COALESCE(col1, col2)),0)`.

### Asterisk (\*) Replacement

**How LeapLogic Handles It:** For view conversions, LeapLogic replaces `SELECT *` with explicit column names from their underlying tables for performance enhancement. This is not applied to procedure conversions.

### Identity Column Conversion

**How LeapLogic Handles It:** Since PySpark doesn't have direct identity column support like Teradata's IDENTITY,

**Example Conversions:**

**Teradata Identity Declaration:**

```sql
GENERATED ALWAYS AS IDENTITY(
    START WITH 1
    INCREMENT BY 1
    MINVALUE -2147483647
    MAXVALUE 2147483647
    NO CYCLE
)
```

**LeapLogic Converts to PySpark:**

```sql
IDENTITY (1, 1)
```

**Teradata Unicode Escape:**

```sql
U&'\0000\0000' UESCAPE '\'
```

**LeapLogic Converts to PySpark:**

```python
chr(0)
```

LeapLogic simplifies complex Teradata identity specifications into concise PySpark equivalents while preserving the auto-incrementing behavior.

### Naming Conventions

**How LeapLogic Handles It:** LeapLogic standardizes naming conventions by converting all columns and tables to lowercase, following Python best practices. File names (object names without schema) are converted to lowercase, while class names are converted to uppercase.

### Casting & Coalesce in Nested Queries

**How LeapLogic Handles It:** LeapLogic optimizes by applying casting and COALESCE only in the outermost query, not in inner subqueries, improving performance.

### Database Name Parameterization and External Schema Declaration

**How LeapLogic Handles It:** LeapLogic parameterizes database names in converted queries to ensure flexibility across different environments. Database names are replaced with parameterized references that can be configured during deployment.

**Database Parameterization:** In the converted PySpark queries, database names are parameterized using syntax like `{self.get_param_value("glue_catalog")}.{self.get_param_value("schemas.oeaetlp1_t")}` instead of hard-coded values. This allows the same code to work across development, staging, and production environments by simply changing configuration parameters.

**External Schema Declaration:** When the query target and database reside on different target locations (e.g., cross-database queries or queries accessing external data sources), LeapLogic automatically declares external databases in the converted code. This ensures that PySpark can properly resolve table references across different storage locations or catalogs.

**Example:**

- Teradata: `OEAETLP1_T.NS_HCI_INOV_CLM_LAG`
- LeapLogic converts to: `{self.get_param_value("glue_catalog")}.{self.get_param_value("schemas.oeaetlp1_t")}.ns_hci_inov_clm_lag`

This approach provides environment-agnostic code and supports complex multi-database architectures in AWS Glue.

### Cross-Platform Query Conversion Strategy (Iceberg & Redshift)

**How LeapLogic Handles It:** LeapLogic determines the optimal conversion strategy based on the location of target and source tables across different platforms (Iceberg and Redshift). The conversion approach varies depending on where the data resides and where it needs to be processed.

**Conversion Logic:**

**When Target Table is on Iceberg:**

- If the source table resides on Redshift, LeapLogic creates a **redshift_copy table** of the source data. This intermediate copy operation efficiently transfers data from Redshift to make it accessible for the Iceberg target table operation.
- If the source table is also on Iceberg, LeapLogic proceeds with standard Iceberg-compatible PySpark query conversion.

**When Target Table is on Redshift:**

- LeapLogic generates **Redshift-compatible queries** (e.g., UPDATE, DELETE, INSERT statements optimized for Redshift's query engine and architecture).
- If the source table resides on Iceberg, LeapLogic adds **ext\_** to the source schema. This creates an external schema declaration that allows Redshift to access Iceberg tables through Redshift Spectrum, enabling seamless cross-platform queries without data duplication.
- If the source table is also on Redshift, LeapLogic generates standard Redshift-optimized SQL queries.

**Key Conversion Scenarios:**

- **Iceberg Target + Iceberg Source**: Standard PySpark SQL with Iceberg format
- **Iceberg Target + Redshift Source**: Creates Redshift COPY table to transfer source data, then processes with Iceberg
- **Redshift Target + Redshift Source**: Redshift-optimized SQL queries
- **Redshift Target + Iceberg Source**: Adds external schema declaration to Iceberg source + generates Redshift-compatible SQL

This intelligent approach ensures optimal performance by selecting the appropriate execution engine, minimizing data movement, and leveraging native capabilities of each platform.

---

## 4. Function Conversion

LeapLogic's function conversion engine automatically translates Teradata SQL functions to their PySpark equivalents. Below are detailed examples of how LeapLogic performs these conversions.

### String and Text Manipulation Functions

#### replacestr/reg_replace → regexp_replace

**What the function does:** Searches for patterns in text strings and replaces them with new values.

**How LeapLogic converts it:** LeapLogic transforms both `replacestr` and `reg_replace` to PySpark's `regexp_replace` function, maintaining pattern matching capabilities.

**Teradata Input:**

```sql
SELECT replacestr(phone_number, '-', '') AS clean_phone
FROM customers;

SELECT reg_replace(email, '@.*$', '@company.com') AS normalized_email
FROM employees;
```

**LeapLogic Converts to:**

```sql
SELECT regexp_replace(phone_number, '-', '') AS clean_phone
FROM customers;

SELECT regexp_replace(email, '@.*$', '@company.com') AS normalized_email
FROM employees;
```

#### numsonly → REGEXP_REPLACE

**What the function does:** Extracts only numeric characters from a string, removing all non-numeric characters.

**How LeapLogic converts it:** LeapLogic replaces `numsonly` with `REGEXP_REPLACE` using a pattern that removes all non-digit characters.

**Teradata Input:**

```sql
SELECT phone, numsonly(phone) AS digits_only
FROM contacts;
-- Result: '(555) 123-4567' becomes '5551234567'
```

**LeapLogic Converts to:**

```sql
SELECT phone, REGEXP_REPLACE(phone, '[^0-9]', '') AS digits_only
FROM contacts;
```

### Hashing and Encryption Functions

#### hash_md5 → md5

**What the function does:** Generates an MD5 hash (128-bit cryptographic hash) of input values.

**How LeapLogic converts it:** LeapLogic directly translates `hash_md5` to PySpark's `md5` function.

**Teradata Input:**

```sql
SELECT customer_id, hash_md5(email) AS hashed_email
FROM customers;
```

**LeapLogic Converts to:**

```sql
SELECT customer_id, md5(email) AS hashed_email
FROM customers;
```

### Type Conversion Functions

#### TO_DATE → CAST(col as date)

**What the function does:** Converts strings or other data types to DATE type.

**How LeapLogic converts it:** LeapLogic replaces `TO_DATE` with standard SQL CAST function.

**Teradata Input:**

```sql
SELECT TO_DATE('2025-11-18', 'YYYY-MM-DD') AS parsed_date;

SELECT order_id, TO_DATE(order_date_str) AS order_date
FROM staging_orders;
```

**LeapLogic Converts to:**

```sql
SELECT CAST('2025-11-18' AS date) AS parsed_date;

SELECT order_id, CAST(order_date_str AS date) AS order_date
FROM staging_orders;
```

#### TO_CHAR → CAST(col as String)

**What the function does:** Converts numbers, dates, or other data types to character strings.

**How LeapLogic converts it:** LeapLogic transforms `TO_CHAR` to CAST with String type.

**Teradata Input:**

```sql
SELECT customer_id, TO_CHAR(customer_id) AS customer_id_str,
       TO_CHAR(signup_date) AS signup_str
FROM customers;
```

**LeapLogic Converts to:**

```sql
SELECT customer_id, CAST(customer_id AS String) AS customer_id_str,
       CAST(signup_date AS String) AS signup_str
FROM customers;
```

#### TO_INTEGER → CAST(col as int)

**What the function does:** Converts string or numeric values to integer type.

**How LeapLogic converts it:** LeapLogic replaces `TO_INTEGER` with CAST to int.

**Teradata Input:**

```sql
SELECT product_id, TO_INTEGER(price) AS price_int
FROM products;
```

**LeapLogic Converts to:**

```sql
SELECT product_id, CAST(price AS int) AS price_int
FROM products;
```

#### TO_TIMESTAMP → CAST(col as timestamp)

**What the function does:** Converts strings or other types to timestamp data type.

**How LeapLogic converts it:** LeapLogic transforms `TO_TIMESTAMP` to CAST with timestamp type.

**Teradata Input:**

```sql
SELECT event_id, TO_TIMESTAMP(event_time_str) AS event_timestamp
FROM events;
```

**LeapLogic Converts to:**

```sql
SELECT event_id, CAST(event_time_str AS timestamp) AS event_timestamp
FROM events;
```

#### TO_NUMBER → CAST(col as decimal)

**What the function does:** Converts strings or other types to numeric/decimal values.

**How LeapLogic converts it:** LeapLogic transforms `TO_NUMBER` to CAST with decimal type.

**Teradata Input:**

```sql
SELECT order_id, TO_NUMBER(amount_str) AS amount
FROM orders_staging;
```

**LeapLogic Converts to:**

```sql
SELECT order_id, CAST(amount_str AS decimal) AS amount
FROM orders_staging;
```

### Pattern Matching and Extraction Functions

#### REG_EXTRACT → REGEXP_EXTRACT

**What the function does:** Uses regular expressions to extract specific portions of text matching a pattern.

**How LeapLogic converts it:** LeapLogic transforms `REG_EXTRACT` directly to `REGEXP_EXTRACT`.

**Teradata Input:**

```sql
SELECT log_entry,
       REG_EXTRACT(log_entry, 'ERROR: ([^;]+)', 1) AS error_message
FROM system_logs;
```

**LeapLogic Converts to:**

```sql
SELECT log_entry,
       REGEXP_EXTRACT(log_entry, 'ERROR: ([^;]+)', 1) AS error_message
FROM system_logs;
```

### Date and Time Functions

#### GETUTCDATE → convert_timezone('UTC', getdate())

**What the function does:** Returns the current date and time in UTC.

**How LeapLogic converts it:** LeapLogic transforms `GETUTCDATE` to a timezone conversion combination.

**Teradata Input:**

```sql
INSERT INTO audit_log (event_time, action)
VALUES (GETUTCDATE(), 'User login');
```

**LeapLogic Converts to:**

```sql
INSERT INTO audit_log (event_time, action)
VALUES (convert_timezone('UTC', getdate()), 'User login');
```

#### NOW → CURRENT_TIMESTAMP

**What the function does:** Returns the current date and time when the query executes.

**How LeapLogic converts it:** LeapLogic transforms `NOW` to standard SQL `CURRENT_TIMESTAMP`.

**Teradata Input:**

```sql
UPDATE sessions
SET last_activity = NOW()
WHERE session_id = 'abc123';
```

**LeapLogic Converts to:**

```sql
UPDATE sessions
SET last_activity = CURRENT_TIMESTAMP
WHERE session_id = 'abc123';
```

### NULL Handling Functions

#### NULLIFZERO → NULLIF

**What the function does:** Converts zero values to NULL to avoid division by zero errors.

**How LeapLogic converts it:** LeapLogic transforms `NULLIFZERO` to `NULLIF(col, 0)`.

**Teradata Input:**

```sql
SELECT product_id,
       sales_amount / NULLIFZERO(quantity) AS average_price
FROM sales;
```

**LeapLogic Converts to:**

```sql
SELECT product_id,
       sales_amount / NULLIF(quantity, 0) AS average_price
FROM sales;
```

#### ZEROIFNULL → COALESCE

**What the function does:** Converts NULL values to zero (0).

**How LeapLogic converts it:** LeapLogic transforms `ZEROIFNULL` to `COALESCE(col, 0)`.

**Teradata Input:**

```sql
SELECT customer_id,
       ZEROIFNULL(loyalty_points) AS points
FROM customers;
```

**LeapLogic Converts to:**

```sql
SELECT customer_id,
       COALESCE(loyalty_points, 0) AS points
FROM customers;
```

---

## Additional Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [PySpark SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html)
- [LeapLogic Documentation](https://www.leaplogic.io/)
- [Teradata to AWS Migration Guide](https://aws.amazon.com/solutions/implementations/teradata-migration/)

---

Generated by HarshPratap - Translation Engineer
