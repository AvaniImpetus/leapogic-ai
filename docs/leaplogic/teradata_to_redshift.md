# Teradata to Amazon Redshift Conversion Guide - LeapLogic

## Overview

This document details how **LeapLogic** converts Teradata database objects to **Amazon Redshift**. LeapLogic's automated migration engine analyzes Teradata schemas, queries, and logic, then generates equivalent Redshift SQL code following AWS best practices and optimized for Redshift's columnar storage architecture.

---

## 1. Data Type Conversion

LeapLogic automatically converts Teradata data types to their Redshift equivalents during migration. Below are the detailed conversions performed by LeapLogic.

### Integer Type Conversions

**BYTEINT → SMALLINT**  
LeapLogic converts Teradata's BYTEINT type (storing values from -128 to 127) to Redshift's SMALLINT, since Redshift doesn't have an exact one-byte integer equivalent. While this uses slightly more storage (2 bytes instead of 1), LeapLogic ensures all original values are maintained without data loss. When LeapLogic encounters `status BYTEINT`, it generates `status SMALLINT`, preserving values like 1 (active), 0 (inactive), or -1 (error).

**SMALLINT → SMALLINT**  
LeapLogic performs a direct conversion of Teradata's SMALLINT to Redshift's SMALLINT, both storing integers from -32,768 to 32,767 identically. This straightforward conversion is commonly applied to year values, small ID ranges, or enumerated types. LeapLogic maintains `year SMALLINT` as `year SMALLINT`, preserving values like 2025.

**INTEGER → INTEGER**  
LeapLogic converts Teradata's INTEGER type to Redshift's INTEGER with perfect one-to-one mapping, both supporting the same range of -2,147,483,648 to 2,147,483,647. This is the most common integer conversion LeapLogic performs for ID columns, counters, and numeric keys. When LeapLogic processes `customer_id INTEGER`, it generates `customer_id INTEGER`, maintaining values like 123456.

**BIGINT → BIGINT**  
LeapLogic converts Teradata's BIGINT directly to Redshift's BIGINT with no changes, supporting the massive range needed for very large transaction IDs, microsecond timestamps, or large aggregations. LeapLogic transforms `transaction_id BIGINT` to `transaction_id BIGINT`, preserving values like 9876543210.

### Decimal and Floating-Point Type Conversions

**DECIMAL(18,2) → DECIMAL(18,2)**  
LeapLogic preserves the exact numeric precision of Teradata's DECIMAL type when converting to Redshift. For financial data where rounding errors are unacceptable, LeapLogic maintains both precision (18) and scale (2) exactly. When LeapLogic converts `price DECIMAL(18,2)`, it generates `price DECIMAL(18,2)`, ensuring values like 1234.56 remain perfectly accurate for currency calculations.

**NUMERIC(10,5) → numeric(10,5)**  
LeapLogic converts Teradata's NUMERIC type directly to Redshift's numeric type with identical precision and scale. Since NUMERIC and DECIMAL are functionally identical in both systems, LeapLogic ensures exact numeric values with decimal places are preserved without any loss of accuracy. LeapLogic transforms `tax_rate NUMERIC(10,5)` to `tax_rate numeric(10,5)`, maintaining precise values like 0.08750 for an 8.75% tax rate.

**FLOAT → DOUBLE PRECISION**  
LeapLogic converts Teradata's FLOAT type to Redshift's DOUBLE PRECISION to provide better range and accuracy. While both are approximate types (meaning they can have rounding errors), LeapLogic's conversion to DOUBLE PRECISION offers superior precision for scientific calculations. LeapLogic transforms `measurement FLOAT` to `measurement DOUBLE PRECISION`, preserving approximate values like 3.14159.

**REAL → REAL**  
LeapLogic performs a direct conversion of Teradata's REAL type to Redshift's REAL, both representing single-precision floating-point numbers identically. This conversion offers less precision than DOUBLE PRECISION but uses less storage. LeapLogic maintains `coefficient REAL` as `coefficient REAL`, preserving values like 2.71828.

**DOUBLE PRECISION → String**  
In specific cases, LeapLogic converts Teradata's DOUBLE PRECISION to Redshift's String (VARCHAR) type rather than to DOUBLE PRECISION. This strategic conversion avoids potential precision loss from floating-point representation issues. By storing the value as text, LeapLogic maintains the exact string representation. LeapLogic transforms `precise_value DOUBLE PRECISION` to `precise_value String`, storing "3.141592653589793" as text.

### Character and String Type Conversions

**CHAR(10) → VARCHAR(10)**  
LeapLogic converts Teradata's fixed-length CHAR type to Redshift's VARCHAR with the same length constraint. While CHAR always uses fixed storage (padding with spaces), LeapLogic generates more storage-efficient VARCHAR code in Redshift. The maximum length constraint is preserved to maintain data integrity. LeapLogic transforms `country_code CHAR(10)` to `country_code VARCHAR(10)`, storing "USA" without the space padding that Teradata would add.

**VARCHAR(100) → VARCHAR(100)**  
LeapLogic performs a perfect one-to-one conversion of Teradata's VARCHAR to Redshift's VARCHAR, maintaining the maximum length constraint. This is one of the most straightforward conversions LeapLogic performs. When processing `customer_name VARCHAR(100)`, LeapLogic generates `customer_name VARCHAR(100)`, preserving names like "John Doe" with identical behavior.

**CLOB → VARCHAR(65000)**  
LeapLogic converts Teradata's CLOB type (which can store massive text objects) to Redshift's VARCHAR with a maximum size of 65,000 bytes. This is the practical maximum for VARCHAR in Redshift and handles most text storage needs. LeapLogic transforms `description CLOB` to `description VARCHAR(65000)`, accommodating large text like entire documents or detailed logs.

**BLOB → VARCHAR**  
LeapLogic converts Teradata's Binary Large Objects (BLOB) to Redshift's VARCHAR. Since BLOBs store binary data and VARCHAR stores text, LeapLogic's conversion strategy assumes binary data will be encoded as text (such as base64 encoding) in Redshift. LeapLogic transforms `data BLOB` to `data VARCHAR`, where binary image data can be stored as a base64-encoded string.

### Date and Time Type Conversions

**DATE → DATE**  
LeapLogic performs a direct, perfect conversion of Teradata's DATE type to Redshift's DATE, storing calendar dates (year, month, day) identically in both systems. No conversion logic is needed. LeapLogic maintains `order_date DATE` as `order_date DATE`, preserving values like 2025-11-18.

**TIME → TIME**  
LeapLogic converts Teradata's TIME type directly to Redshift's TIME type, both storing time of day (hours, minutes, seconds, and fractions) without a date component. This conversion is used for scheduling, time tracking, or scenarios requiring time without date. LeapLogic maintains `login_time TIME` as `login_time TIME`, preserving values like 14:30:00.

**TIMESTAMP → TIMESTAMP**  
LeapLogic performs a perfect one-to-one conversion of Teradata's TIMESTAMP to Redshift's TIMESTAMP, storing both date and time together with high precision. This is essential for audit trails, transaction logs, and event tracking. LeapLogic maintains `created_at TIMESTAMP` as `created_at TIMESTAMP`, preserving precise moments like 2025-11-18 14:30:00.123456.

### Interval Type Conversions

**INTERVAL YEAR(2) → <<INTERVAL_CONSTANT>> SECOND_STRING INTERVAL year**  
LeapLogic converts Teradata's INTERVAL YEAR type to Redshift's interval format, which handles intervals differently. LeapLogic generates conversion code using placeholder constants and special interval syntax. The notation `<<INTERVAL_CONSTANT>>` indicates that LeapLogic transforms the actual interval value to Redshift's interval format. For `age INTERVAL YEAR(2)` representing 25 years, LeapLogic generates appropriate Redshift interval representation.

**INTERVAL MONTH(2) → <<INTERVAL_CONSTANT>> SECOND_STRING INTERVAL month**  
LeapLogic converts Teradata's INTERVAL MONTH to Redshift's interval syntax with appropriate handling. The interval representing months is transformed by LeapLogic using Redshift's interval notation. For `subscription INTERVAL MONTH(2)` storing 12 months, LeapLogic generates the appropriate Redshift interval code rather than a direct type mapping.

---

## 2. Query Type Conversion

LeapLogic automatically analyzes and converts different SQL query types from Teradata to Redshift-optimized syntax.

### UPDATE Queries → MERGE INTO

LeapLogic converts Teradata UPDATE statements to MERGE INTO statements in Redshift for better performance in data warehouse environments. When LeapLogic encounters UPDATE operations, it transforms them into upsert patterns (update if exists, insert if not) that are more efficient in Redshift's columnar storage.

**Teradata Input:**

```sql
UPDATE employees
SET salary = salary * 1.10,
    last_updated = CURRENT_TIMESTAMP
WHERE department = 'Engineering'
  AND performance_rating >= 4;
```

**LeapLogic Converts to Redshift:**

```sql
MERGE INTO employees AS target
USING (
    SELECT employee_id, salary * 1.10 AS new_salary, CURRENT_TIMESTAMP AS update_time
    FROM employees
    WHERE department = 'Engineering'
      AND performance_rating >= 4
) AS source
ON target.employee_id = source.employee_id
WHEN MATCHED THEN
    UPDATE SET
        target.salary = source.new_salary,
        target.last_updated = source.update_time;
```

LeapLogic's conversion ensures atomic operations and optimal performance in Redshift's architecture.

### DELETE Queries → MERGE INTO or DELETE

LeapLogic intelligently determines the best conversion strategy for DELETE statements based on query complexity. For simple DELETE operations, LeapLogic maintains the DELETE syntax. For complex scenarios involving joins or subqueries, LeapLogic converts to MERGE INTO patterns for better Redshift performance.

**Simple DELETE (LeapLogic maintains syntax):**

_Teradata:_

```sql
DELETE FROM order_archive
WHERE order_date < DATE '2023-01-01';
```

_LeapLogic Converts to Redshift:_

```sql
DELETE FROM order_archive
WHERE order_date < DATE '2023-01-01';
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

_LeapLogic Converts to Redshift:_

```sql
MERGE INTO inventory AS target
USING (
    SELECT DISTINCT i.product_id
    FROM inventory i
    INNER JOIN products p ON i.product_id = p.product_id
    WHERE p.discontinued = 1
      AND p.last_sale_date < CURRENT_DATE - INTERVAL '2' YEAR
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN DELETE;
```

LeapLogic's intelligent conversion provides better performance for complex delete operations in Redshift.

### INSERT Queries → INSERT

LeapLogic converts Teradata INSERT statements to Redshift with appropriate data type casting and NULL handling. While basic INSERT syntax remains similar, LeapLogic adds necessary transformations for data type compatibility and constraint handling.

**Basic INSERT (LeapLogic maintains structure):**

_Teradata:_

```sql
INSERT INTO customers (customer_id, customer_name, email, signup_date)
VALUES (12345, 'Jane Smith', 'jane.smith@example.com', CURRENT_DATE);
```

_LeapLogic Converts to Redshift:_

```sql
INSERT INTO customers (customer_id, customer_name, email, signup_date)
VALUES (12345, 'Jane Smith', 'jane.smith@example.com', CURRENT_DATE);
```

**INSERT with SELECT (LeapLogic adds casting/coalescing):**

_Teradata:_

```sql
INSERT INTO sales_summary
SELECT region_id, product_id, SUM(amount), COUNT(*)
FROM sales
WHERE sale_date = CURRENT_DATE
GROUP BY region_id, product_id;
```

_LeapLogic Converts to Redshift:_

```sql
INSERT INTO sales_summary (region_id, product_id, total_amount, transaction_count)
SELECT
    CAST(region_id AS INT),
    CAST(product_id AS INT),
    COALESCE(SUM(amount), 0.00),
    COALESCE(COUNT(*), 0)
FROM sales
WHERE sale_date = CURRENT_DATE
GROUP BY region_id, product_id;
```

LeapLogic ensures proper data type compatibility and handles NULL values according to target table constraints.

### TRUNCATE Queries → TRUNCATE

LeapLogic converts Teradata TRUNCATE statements directly to Redshift with minimal changes. This operation efficiently removes all rows from a table without logging individual row deletions.

**Teradata Input:**

```sql
TRUNCATE TABLE staging_customer_data;
```

**LeapLogic Converts to Redshift:**

```sql
TRUNCATE TABLE staging_customer_data;
```

LeapLogic preserves the TRUNCATE behavior: fast data removal, identity column resets, and atomic operations.

---

## 3. Logical Transformations and Conversion Rules

LeapLogic applies intelligent transformation rules to ensure migrated code maintains functional equivalence while optimizing for Redshift's columnar storage architecture.

### QUALIFY Clause Conversion

**How LeapLogic Handles It:** Redshift supports the QUALIFY clause, hence no conversion is required for qualify clause.

### Casting Application

**How LeapLogic Handles It:** When LeapLogic detects data type mismatches between source and target columns during INSERT operations, it automatically applies CAST functions to ensure type compatibility.

**Example:** LeapLogic generates `CAST(col as int)` when inserting into integer columns from string sources.

### COALESCE Application

**How LeapLogic Handles It:** For tables with NOT NULL constraints, LeapLogic automatically wraps columns with COALESCE functions to handle null entries appropriately.

**Example:** LeapLogic generates `COALESCE(col, 'Default Value')` or `COALESCE(col, 0)` based on data type.

### Collation Handling (Redshift-Specific)

**How LeapLogic Handles It:** Teradata handles case sensitivity automatically, but Redshift requires explicit collation functions. LeapLogic applies `collate(column, 'case_sensitive')` or `collate(column, 'case_insensitive')` as needed.

**Conversion Pattern:** LeapLogic wraps string comparisons with appropriate collation functions based on the original Teradata query intent.

### Default Value / Identity Column Conversion

**How LeapLogic Handles It:** Teradata's default values and identity columns work differently than Redshift's. LeapLogic performs specific conversions:

- **Teradata IDENTITY:** `GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1 MINVALUE -2147483647 MAXVALUE 2147483647 NO CYCLE)`
- **LeapLogic Converts to Redshift:** `IDENTITY (1,1)`

- **Teradata Unicode:** `U&'\0000\0000' UESCAPE '\'`
- **LeapLogic Converts to Redshift:** `chr(0)`

### Asterisk (\*) Replacement

**How LeapLogic Handles It:** For view conversions, LeapLogic replaces `SELECT *` with explicit column names for performance enhancement. This is not applied to procedure conversions.

### Naming Conventions

**How LeapLogic Handles It:** LeapLogic standardizes naming conventions by converting all columns and tables to lowercase, following best practices. File names (object names without schema) are converted to lowercase, while class names are converted to uppercase.

### Casting & Coalesce in Nested Queries

**How LeapLogic Handles It:** LeapLogic optimizes by applying casting and COALESCE only in the outermost query, not in inner subqueries, improving Redshift query performance.

### Glass View Definition

**How LeapLogic Handles It:** LeapLogic identifies views that don't contain any joins or transformations (known as Glass view definitions) and generates reports for all such views.

**Example:** `CREATE VIEW view1 AS SELECT * FROM tbl;`

LeapLogic documents these simple views for review and potential optimization.

### Index Conversion

**How LeapLogic Handles It:** LeapLogic converts Teradata indexes to appropriate Redshift equivalents based on their type and usage.

**Unique Primary Index → UNIQUE**

LeapLogic converts Teradata's Unique Primary Index to Redshift's UNIQUE constraint. This ensures data integrity by preventing duplicate values in the specified columns.

**Teradata Input:**

```sql
CREATE TABLE customers (
    customer_id INTEGER,
    email VARCHAR(100),
    UNIQUE PRIMARY INDEX (customer_id, email)
);
```

**LeapLogic Converts to Redshift:**

```sql
CREATE TABLE customers (
    customer_id INTEGER,
    email VARCHAR(100),
    UNIQUE (customer_id, email)
);
```

**Primary Index → Sort Key (when partition clause present)**

LeapLogic removes Primary Index declarations unless they include a partition clause. When a partition clause is present, LeapLogic converts the Primary Index to a Sort Key for optimal query performance in Redshift's columnar storage.

**Teradata Input (without partition):**

```sql
CREATE TABLE orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    PRIMARY INDEX (order_id)
);
```

**LeapLogic Converts to Redshift:**

```sql
CREATE TABLE orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE
);
-- Primary Index removed as no partition clause
```

**Teradata Input (with partition):**

```sql
CREATE TABLE sales (
    sale_id INTEGER,
    product_id INTEGER,
    sale_date DATE,
    amount DECIMAL(10,2),
    PRIMARY INDEX (sale_id) PARTITION BY RANGE(sale_date) (
        START ('2020-01-01') END ('2026-01-01') EVERY INTERVAL '1' MONTH
    )
);
```

**LeapLogic Converts to Redshift:**

```sql
CREATE TABLE sales (
    sale_id INTEGER,
    product_id INTEGER,
    sale_date DATE,
    amount DECIMAL(10,2)
)
SORTKEY (sale_id);
-- Primary Index converted to Sort Key due to partition clause
```

### Logging in Stored Procedures

**How LeapLogic Handles It:** LeapLogic implements comprehensive logging in converted Redshift stored procedures using the RAISE statement. These logs provide critical information about procedure execution, including the procedure name, affected table name, type of query, and status. This ensures proper audit trails and debugging capabilities in the migrated code.

---

## 4. Function Conversion

LeapLogic's function conversion engine automatically translates Teradata SQL functions to their Redshift equivalents. Below are detailed examples of how LeapLogic performs these conversions.

### String and Text Manipulation Functions

#### replacestr/reg_replace → regexp_replace

**What the function does:** Searches for patterns in text strings and replaces them with new values.

**How LeapLogic converts it:** LeapLogic transforms both `replacestr` and `reg_replace` to Redshift's `regexp_replace` function.

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

#### CHAR/CHARACTER/CHARS → LENGTH

**What the function does:** Returns the length of a string (number of characters).

**How LeapLogic converts it:** LeapLogic transforms the CHAR function to Redshift's `LENGTH` function.

**Teradata Input:**

```sql
SELECT customer_name, CHAR(customer_name) AS name_length
FROM customers
WHERE CHAR(customer_name) > 50;
```

**LeapLogic Converts to:**

```sql
SELECT customer_name, LENGTH(customer_name) AS name_length
FROM customers
WHERE LENGTH(customer_name) > 50;
```

#### instr → STRPOS

**What the function does:** Finds the position of a substring within a string.

**How LeapLogic converts it:** LeapLogic transforms `instr` to Redshift's `STRPOS` function.

**Teradata Input:**

```sql
SELECT email, instr(email, '@') AS at_position
FROM users
WHERE instr(email, '@') > 0;
```

**LeapLogic Converts to:**

```sql
SELECT email, STRPOS(email, '@') AS at_position
FROM users
WHERE STRPOS(email, '@') > 0;
```

#### numsonly → REGEXP_REPLACE

**What the function does:** Extracts only numeric characters from a string.

**How LeapLogic converts it:** LeapLogic transforms `numsonly` to `REGEXP_REPLACE` with a pattern that removes all non-numeric characters.

**Teradata Input:**

```sql
SELECT phone, numsonly(phone) AS digits_only
FROM contacts;
```

**LeapLogic Converts to:**

```sql
SELECT phone, REGEXP_REPLACE(phone, '[^0-9]', '') AS digits_only
FROM contacts;
```

### Hashing and Encryption Functions

#### hash_md5 → md5

**What the function does:** Generates an MD5 hash of the input value.

**How LeapLogic converts it:** LeapLogic directly translates `hash_md5` to Redshift's `md5` function.

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

#### hash8 → hash8expression

**What the function does:** Creates an 8-byte hash value from the input.

**How LeapLogic converts it:** LeapLogic transforms `hash8` to a Redshift hash expression.

**Teradata Input:**

```sql
SELECT customer_id, hash8(customer_id, order_date) AS distribution_key
FROM orders;
```

**LeapLogic Converts to:**

```sql
SELECT customer_id, hash8expression AS distribution_key
FROM orders;
```

### Type Conversion Functions

#### TO_DATE → CAST(col as date)

**What the function does:** Converts a string or other data type to a DATE type.

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

**How LeapLogic converts it:** LeapLogic transforms `TO_CHAR` to CAST with String/VARCHAR type.

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

#### ~~* → ILIKE

**What the operator does:** Performs case-insensitive pattern matching using wildcards.

**How LeapLogic converts it:** LeapLogic transforms the `~~*` operator to Redshift's `ILIKE` operator.

**Teradata Input:**

```sql
SELECT customer_name
FROM customers
WHERE customer_name ~~* 'john%';
```

**LeapLogic Converts to:**

```sql
SELECT customer_name
FROM customers
WHERE customer_name ILIKE 'john%';
```

#### !~~ → NOT LIKE

**What the operator does:** Performs case-sensitive pattern matching negation.

**How LeapLogic converts it:** LeapLogic transforms the `!~~` operator to Redshift's `NOT LIKE` operator.

**Teradata Input:**

```sql
SELECT product_name
FROM products
WHERE product_name !~~ 'temp%';
```

**LeapLogic Converts to:**

```sql
SELECT product_name
FROM products
WHERE product_name NOT LIKE 'temp%';
```

#### ~~ → LIKE

**What the operator does:** Performs case-sensitive pattern matching using wildcards.

**How LeapLogic converts it:** LeapLogic transforms the `~~` operator to Redshift's `LIKE` operator.

**Teradata Input:**

```sql
SELECT email
FROM users
WHERE email ~~ '%@company.com';
```

**LeapLogic Converts to:**

```sql
SELECT email
FROM users
WHERE email LIKE '%@company.com';
```

#### !~~* → NOT ILIKE

**What the operator does:** Performs case-insensitive pattern matching negation.

**How LeapLogic converts it:** LeapLogic transforms the `!~~*` operator to Redshift's `NOT ILIKE` operator.

**Teradata Input:**

```sql
SELECT department
FROM employees
WHERE department !~~* 'sales%';
```

**LeapLogic Converts to:**

```sql
SELECT department
FROM employees
WHERE department NOT ILIKE 'sales%';
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

#### FORMAT → TO_CHAR

**What the function does:** Formats date, time, or numeric values into a specified string format.

**How LeapLogic converts it:** LeapLogic transforms the FORMAT function to Redshift's TO_CHAR function with the same format string.

**Teradata Input:**

```sql
SELECT order_date FORMAT 'dd/mm/yyyy' AS formatted_date
FROM orders;
```

**LeapLogic Converts to:**

```sql
SELECT TO_CHAR(order_date, 'dd/mm/yyyy') AS formatted_date
FROM orders;
```

### Random Number Functions

#### RANDOM → RAND()

**What the function does:** Generates a random number, typically between 0 and 1.

**How LeapLogic converts it:** LeapLogic transforms `RANDOM` to Redshift's `RAND()` function.

**Teradata Input:**

```sql
SELECT customer_id, RANDOM() AS random_value
FROM customers
WHERE RANDOM() < 0.1;  -- Random 10% sample
```

**LeapLogic Converts to:**

```sql
SELECT customer_id, RAND() AS random_value
FROM customers
WHERE RAND() < 0.1;
```

### NULL Handling Functions

#### NULLIFZERO → NULLIF

**What the function does:** Converts zero values to NULL.

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

- [Amazon Redshift Documentation](https://docs.aws.amazon.com/redshift/)
- [Redshift SQL Reference](https://docs.aws.amazon.com/redshift/latest/dg/c_SQL_commands.html)
- [LeapLogic Documentation](https://www.leaplogic.io/)
- [Teradata to AWS Migration Guide](https://aws.amazon.com/solutions/implementations/teradata-migration/)

---

_Generated by LeapLogic - Automated Database Migration Platform_  
_Last Updated: November 26, 2025_
