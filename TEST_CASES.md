# Test Cases Reference

This document lists every test case the BigQuery Data Validation Framework can produce, grouped by validation layer, followed by the exact test cases executed in each existing test suite.

---

## Table of Contents

- [Test Case Catalog — All Supported Test Cases](#test-case-catalog--all-supported-test-cases)
- [Test Suite: test1 — Flat CSV (generic_file_load_test)](#test-suite-test1--flat-csv-generic_file_load_test)
- [Test Suite: test2\_nested — JSONL with STRUCT + ARRAY (orders\_with\_nested)](#test-suite-test2_nested--jsonl-with-struct--array-orders_with_nested)
- [Test Suite: test3\_complex\_csv — Complex CSV with JSON + BOOL + Partitions (customer\_transactions)](#test-suite-test3_complex_csv--complex-csv-with-json--bool--partitions-customer_transactions)
- [Test Name Patterns](#test-name-patterns)
- [Test Count Summary](#test-count-summary)

---

## Test Case Catalog — All Supported Test Cases

### Layer 1 — Schema Validation

| Test Name | Description | Config Flag |
|-----------|-------------|-------------|
| `schema_validation` | Compares the set of column names in the file against the BigQuery table schema. Reports missing and extra columns. Works for CSV and JSONL, including STRUCT/ARRAY columns. | `metadata_validation: true` |

**Fixed count:** 1 test per run.

---

### Layer 2 — Row Count Validation

| Test Name | Description | Config Flag |
|-----------|-------------|-------------|
| `row_count_validation` | Compares the total number of data rows in the source file against `COUNT(*)` in BigQuery. Reports absolute and percentage delta. | `row_count_validation: true` |

**Fixed count:** 1 test per run.

---

### Layer 3 — Primary Key Uniqueness

| Test Name | Description | Config Flag |
|-----------|-------------|-------------|
| `primary_key_uniqueness_file` | Detects duplicate primary key combinations in the source file. | `primary_key_uniqueness: true` |
| `primary_key_null_check_file` | Detects null or empty values in any primary key column in the source file. | `primary_key_uniqueness: true` |
| `primary_key_uniqueness_bq` | Detects duplicate primary key combinations in BigQuery using `GROUP BY … HAVING COUNT(*) > 1`. | `primary_key_uniqueness: true` |

**Fixed count:** 3 tests per run (or 0 if `primary_keys` is empty — layer is auto-skipped).

---

### Layer 4 — Aggregate Validation

One test is produced for **each `(column, function)` pair** listed in `aggregate_columns`.

| Test Name Pattern | Description | Supported Functions |
|-------------------|-------------|---------------------|
| `aggregate_validation_{column}_sum` | `SUM(column)` — file (pandas) vs BQ | Numeric columns |
| `aggregate_validation_{column}_min` | `MIN(column)` — file vs BQ | Numeric, Date, Timestamp, String |
| `aggregate_validation_{column}_max` | `MAX(column)` — file vs BQ | Numeric, Date, Timestamp, String |
| `aggregate_validation_{column}_avg` | `AVG(column)` — file vs BQ | Numeric columns |
| `aggregate_validation_{column}_distinct_count` | `COUNT(DISTINCT column)` — file vs BQ | Any scalar column |

**Variable count:** N tests = sum of all `functions` entries across all `aggregate_columns`.

---

### Layer 5 — Partition Validation

| Test Name | Description | Config Flag |
|-----------|-------------|-------------|
| `partition_validation` | Groups rows by partition column and compares per-partition row counts between file and BigQuery. | `partition_validation: true` AND `partition.enabled: true` |

**Fixed count:** 1 test per run (or `SKIPPED` if `partition.enabled: false`).

---

### Layer 6 — Hash Validation

| Test Name | Description | Config Flag |
|-----------|-------------|-------------|
| `hash_validation` | Computes an aggregate XOR hash on the file side (Python `hash()`) and on the BQ side (`BIT_XOR(FARM_FINGERPRINT(…))`). PASSes when both are non-zero (confirming data present on both sides). | `hash_validation: true` |

**Fixed count:** 1 test per run.

---

### Layer 7 — Random Sampling Validation

| Test Name | Description | Config Flag |
|-----------|-------------|-------------|
| `random_sampling_validation` | Randomly samples up to `random_sample_size` rows from the file, fetches matching rows from BigQuery using primary key(s), and performs field-level comparison. Supports STRUCT and ARRAY columns. | `random_sampling: true` |

**Fixed count:** 1 test per run (or `SKIPPED` if `primary_keys` is empty).

---

### Layer 8 — Column Distribution Validation

One test is produced for **each column** listed in `distribution_columns`.

| Test Name Pattern | Description | Suitable Columns |
|-------------------|-------------|-----------------|
| `column_distribution_{column}` | Computes and compares `min`, `max`, `avg`, `stddev_pop`, and `null_count` between file (pandas) and BigQuery (SQL). | Scalar NUMERIC columns only (INTEGER, FLOAT, NUMERIC) |

**Variable count:** 1 test per column in `distribution_columns`.

---

### Layer 9 — Null Value Validation

One test is produced for **each column** in `null_check_columns` (or every column in the file when the list is empty).

| Test Name Pattern | Description | Suitable Columns |
|-------------------|-------------|-----------------|
| `null_validation:{column}` | Counts null / missing values in the file (`None`, `NaN`, empty string) and in BigQuery (`IS NULL`), then compares the two counts. | Any column type: STRING, NUMERIC, DATE, TIMESTAMP, STRUCT, ARRAY |

**Variable count:** 1 test per checked column.

---

### Layer 10 — Column-Level Checksum Validation

One test is produced for **each column** in `column_checksum_columns` (or every column in the file when the list is empty).

| Test Name Pattern | Description | Suitable Columns |
|-------------------|-------------|-----------------|
| `column_checksum:{column}` | Computes a hash aggregate on the file side (XOR of `hash(cell_value)`) and on the BQ side (`BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(column)))`). PASSes when both are non-zero (confirming data loaded into the column). | Any column type: STRING, NUMERIC, DATE, TIMESTAMP, STRUCT, ARRAY |

**Variable count:** 1 test per checked column.

---

## Test Suite: test1 — Flat CSV (`generic_file_load_test`)

**Config:** `tests/test1/validation_config.yaml`  
**Source file:** `tests/test1/bq_generic_test_file.csv` — 5 rows, 13 columns (flat CSV)  
**BQ table:** `raw_file_loads.generic_file_load_test`  
**Primary key:** `id`  
**Last run:** 2026-03-13 · **overall_status: PASS** · 15/15 passed

| # | Test Name | Layer | Status | Notes |
|---|-----------|-------|--------|-------|
| 1 | `schema_validation` | 1 | PASS | 13 columns matched |
| 2 | `row_count_validation` | 2 | PASS | file=5, BQ=5 |
| 3 | `primary_key_uniqueness_file` | 3 | PASS | 0 duplicates |
| 4 | `primary_key_null_check_file` | 3 | PASS | 0 nulls |
| 5 | `primary_key_uniqueness_bq` | 3 | PASS | 0 duplicates in BQ |
| 6 | `aggregate_validation_amount_sum` | 4 | PASS | file=831.6, BQ=831.6 |
| 7 | `aggregate_validation_amount_min` | 4 | PASS | file=80.25, BQ=80.25 |
| 8 | `aggregate_validation_amount_max` | 4 | PASS | file=300.1, BQ=300.1 |
| 9 | `aggregate_validation_amount_avg` | 4 | PASS | file=166.32, BQ=166.32 |
| 10 | `aggregate_validation_quantity_sum` | 4 | PASS | file=12.0, BQ=12.0 |
| 11 | `aggregate_validation_quantity_avg` | 4 | PASS | file=2.4, BQ=2.4 |
| 12 | `aggregate_validation_email_distinct_count` | 4 | PASS | file=5, BQ=5 |
| 13 | `hash_validation` | 6 | PASS | Both sides non-zero |
| 14 | `random_sampling_validation` | 7 | PASS | 5/5 rows matched, 0 field mismatches |
| 15 | `column_distribution_quantity` | 8 | PASS | min=1, max=4, avg=2.4, stddev=1.140175 |

**Layers not run:** Layer 5 (partition disabled), Layers 9–10 (not yet added at time of run).

---

## Test Suite: test2\_nested — JSONL with STRUCT + ARRAY (`orders_with_nested`)

**Config:** `tests/test2_nested/validation_config.yaml`  
**Source file:** `tests/test2_nested/bq_json_test_file.jsonl` — 3 rows, 9 columns (JSONL)  
**BQ table:** `raw_file_loads.orders_with_nested`  
**Primary key:** `order_id`  
**Schema includes:** `customer_info STRUCT<country, email, name>` and `item_ids ARRAY<STRING>`

### Run 1 results (Layers 1–8, before Layers 9–10 were added)

**Last run:** 2026-03-13 · **overall_status: PASS** · 21/21 passed

| # | Test Name | Layer | Status | Notes |
|---|-----------|-------|--------|-------|
| 1 | `schema_validation` | 1 | PASS | 9 columns matched (incl. STRUCT + ARRAY) |
| 2 | `row_count_validation` | 2 | PASS | file=3, BQ=3 |
| 3 | `primary_key_uniqueness_file` | 3 | PASS | 0 duplicates |
| 4 | `primary_key_null_check_file` | 3 | PASS | 0 nulls |
| 5 | `primary_key_uniqueness_bq` | 3 | PASS | 0 duplicates in BQ |
| 6 | `aggregate_validation_order_amount_sum` | 4 | PASS | file=451.25, BQ=451.25 |
| 7 | `aggregate_validation_order_amount_min` | 4 | PASS | file=80.75, BQ=80.75 |
| 8 | `aggregate_validation_order_amount_max` | 4 | PASS | file=250.0, BQ=250.0 |
| 9 | `aggregate_validation_order_amount_avg` | 4 | PASS | file=150.42, BQ=150.42 |
| 10 | `aggregate_validation_order_id_min` | 4 | PASS | file=1001, BQ=1001 |
| 11 | `aggregate_validation_order_id_max` | 4 | PASS | file=1003, BQ=1003 |
| 12 | `aggregate_validation_order_id_distinct_count` | 4 | PASS | file=3, BQ=3 |
| 13 | `aggregate_validation_customer_id_sum` | 4 | PASS | file=1506, BQ=1506 |
| 14 | `aggregate_validation_customer_id_distinct_count` | 4 | PASS | file=3, BQ=3 |
| 15 | `aggregate_validation_order_status_distinct_count` | 4 | PASS | file=3, BQ=3 |
| 16 | `aggregate_validation_order_uuid_distinct_count` | 4 | PASS | file=3, BQ=3 |
| 17 | `hash_validation` | 6 | PASS | Both sides non-zero |
| 18 | `random_sampling_validation` | 7 | PASS | 3/3 rows matched (STRUCT + ARRAY fields verified) |
| 19 | `column_distribution_order_amount` | 8 | PASS | min=80.75, max=250.0, avg=150.42 |
| 20 | `column_distribution_order_id` | 8 | PASS | min=1001, max=1003 |
| 21 | `column_distribution_customer_id` | 8 | PASS | min=501, max=503 |

**Layer 5:** Skipped (partition disabled — `orders_with_nested` is not partitioned).

### Expected Run 2 results (Layers 1–10, after Layers 9–10 added)

When re-run with valid credentials, Layers 9 and 10 will produce **18 additional tests** (9 null checks + 9 column checksums — one per column):

| # | Test Name | Layer | Expected |
|---|-----------|-------|----------|
| 22 | `null_validation:order_timestamp` | 9 | PASS (0 nulls) |
| 23 | `null_validation:item_ids` | 9 | PASS (0 nulls) |
| 24 | `null_validation:customer_info` | 9 | PASS (0 nulls) |
| 25 | `null_validation:order_id` | 9 | PASS (0 nulls) |
| 26 | `null_validation:order_status` | 9 | PASS (0 nulls) |
| 27 | `null_validation:customer_id` | 9 | PASS (0 nulls) |
| 28 | `null_validation:order_uuid` | 9 | PASS (0 nulls) |
| 29 | `null_validation:order_date` | 9 | PASS (0 nulls) |
| 30 | `null_validation:order_amount` | 9 | PASS (0 nulls) |
| 31 | `column_checksum:order_timestamp` | 10 | PASS (both non-zero) |
| 32 | `column_checksum:item_ids` | 10 | PASS (both non-zero) |
| 33 | `column_checksum:customer_info` | 10 | PASS (both non-zero) |
| 34 | `column_checksum:order_id` | 10 | PASS (both non-zero) |
| 35 | `column_checksum:order_status` | 10 | PASS (both non-zero) |
| 36 | `column_checksum:customer_id` | 10 | PASS (both non-zero) |
| 37 | `column_checksum:order_uuid` | 10 | PASS (both non-zero) |
| 38 | `column_checksum:order_date` | 10 | PASS (both non-zero) |
| 39 | `column_checksum:order_amount` | 10 | PASS (both non-zero) |

**Expected total (Run 2):** 39 tests — all PASS.

---

## Test Suite: test3\_complex\_csv — Complex CSV with JSON + BOOL + Partitions (`customer_transactions`)

**Config:** `tests/test3_complex_csv/validation_config.yaml`  
**Source file:** `tests/test3_complex_csv/bq-customer-transactions.csv` — 8 rows, 17 columns (complex CSV)  
**BQ table:** `raw_file_loads.customer_transactions`  
**Primary key:** `transaction_id`  
**Partition column:** `transaction_date` (DATE — one distinct date per row)  
**Schema includes:** INT64, STRING, NUMERIC, FLOAT64, BOOL, DATE, TIMESTAMP, JSON  
**Last run:** 2026-03-13 · **overall_status: PASS** · 78/78 passed

### Layer breakdown

| Layer | Tests | Notes |
|-------|-------|-------|
| 1 — Schema | 1 | 17 columns matched |
| 2 — Row Count | 1 | file=8, BQ=8 |
| 3 — PK Uniqueness | 3 | `transaction_id` — 0 duplicates, 0 nulls |
| 4 — Aggregate | 32 | 16 columns × mixed functions (see below) |
| 5 — Partition | 1 | 8 partitions matched (2025-02-01 → 2025-02-08) |
| 6 — Hash | 1 | Both sides non-zero |
| 7 — Random Sampling | 1 | 8/8 rows matched; JSON column handled as dict |
| 8 — Distribution | 5 | 5 numeric columns: min/max/avg/stddev/null_count |
| 9 — Null Validation | 17 | All 17 columns — 0 nulls across the board |
| 10 — Column Checksum | 16 | 16 columns (is_first_purchase excluded — XOR collision) |
| **Total** | **78** | **all PASS** |

### Full test list

| # | Test Name | Layer | Status | Notes |
|---|-----------|-------|--------|-------|
| 1 | `schema_validation` | 1 | PASS | 17 columns matched |
| 2 | `row_count_validation` | 2 | PASS | file=8, BQ=8 |
| 3 | `primary_key_uniqueness_file` | 3 | PASS | 0 duplicates |
| 4 | `primary_key_null_check_file` | 3 | PASS | 0 nulls |
| 5 | `primary_key_uniqueness_bq` | 3 | PASS | 0 duplicates in BQ |
| 6 | `aggregate_validation_transaction_id_min` | 4 | PASS | 10001 |
| 7 | `aggregate_validation_transaction_id_max` | 4 | PASS | 10008 |
| 8 | `aggregate_validation_transaction_id_distinct_count` | 4 | PASS | 8 |
| 9 | `aggregate_validation_customer_id_min` | 4 | PASS | 501 |
| 10 | `aggregate_validation_customer_id_max` | 4 | PASS | 508 |
| 11 | `aggregate_validation_customer_id_distinct_count` | 4 | PASS | 8 |
| 12 | `aggregate_validation_transaction_amount_sum` | 4 | PASS | 1482.49 |
| 13 | `aggregate_validation_transaction_amount_min` | 4 | PASS | 60.25 (file float vs BQ string — type-tolerant) |
| 14 | `aggregate_validation_transaction_amount_max` | 4 | PASS | 420.90 (file float vs BQ string — type-tolerant) |
| 15 | `aggregate_validation_transaction_amount_avg` | 4 | PASS | 185.31125 |
| 16 | `aggregate_validation_tax_amount_sum` | 4 | PASS | 115.49 |
| 17 | `aggregate_validation_tax_amount_min` | 4 | PASS | 4.0 (file) vs "4" (BQ) — numeric tolerance |
| 18 | `aggregate_validation_tax_amount_max` | 4 | PASS | 35.0 (file) vs "35" (BQ) — numeric tolerance |
| 19 | `aggregate_validation_tax_amount_avg` | 4 | PASS | 14.43625 |
| 20 | `aggregate_validation_discount_percent_sum` | 4 | PASS | 48.0 |
| 21 | `aggregate_validation_discount_percent_min` | 4 | PASS | 0.0 |
| 22 | `aggregate_validation_discount_percent_max` | 4 | PASS | 15.0 |
| 23 | `aggregate_validation_discount_percent_avg` | 4 | PASS | 6.0 |
| 24 | `aggregate_validation_transaction_uuid_distinct_count` | 4 | PASS | 8 |
| 25 | `aggregate_validation_customer_email_distinct_count` | 4 | PASS | 8 |
| 26 | `aggregate_validation_country_code_distinct_count` | 4 | PASS | 7 (US appears twice) |
| 27 | `aggregate_validation_payment_method_distinct_count` | 4 | PASS | 4 |
| 28 | `aggregate_validation_transaction_status_distinct_count` | 4 | PASS | 4 |
| 29 | `aggregate_validation_product_ids_distinct_count` | 4 | PASS | 8 |
| 30 | `aggregate_validation_is_fraud_distinct_count` | 4 | PASS | 2 |
| 31 | `aggregate_validation_is_first_purchase_distinct_count` | 4 | PASS | 2 |
| 32 | `aggregate_validation_transaction_date_min` | 4 | PASS | 2025-02-01 |
| 33 | `aggregate_validation_transaction_date_max` | 4 | PASS | 2025-02-08 |
| 34 | `aggregate_validation_transaction_timestamp_min` | 4 | PASS | "2025-02-01 10:15:30" (file) vs "2025-02-01T10:15:30+00:00" (BQ) — normalised |
| 35 | `aggregate_validation_transaction_timestamp_max` | 4 | PASS | "2025-02-08 07:45:50" (file) vs "2025-02-08T07:45:50+00:00" (BQ) — normalised |
| 36 | `aggregate_validation_ingestion_timestamp_min` | 4 | PASS | "2025-02-01 10:16:00" (file) vs "2025-02-01T10:16:00+00:00" (BQ) — normalised |
| 37 | `aggregate_validation_ingestion_timestamp_max` | 4 | PASS | "2025-02-08 07:46:20" (file) vs "2025-02-08T07:46:20+00:00" (BQ) — normalised |
| 38 | `partition_validation` | 5 | PASS | 8 partitions matched (2025-02-01 through 2025-02-08) |
| 39 | `hash_validation` | 6 | PASS | Both sides non-zero |
| 40 | `random_sampling_validation` | 7 | PASS | 8/8 rows matched; `metadata_json` JSON string parsed as dict |
| 41 | `column_distribution_transaction_id` | 8 | PASS | min=10001, max=10008, avg=10004.5, stddev=2.44949 |
| 42 | `column_distribution_customer_id` | 8 | PASS | min=501, max=508, avg=504.5, stddev=2.44949 |
| 43 | `column_distribution_transaction_amount` | 8 | PASS | min=60.25, max=420.90, avg=185.31125, stddev=126.583468 |
| 44 | `column_distribution_tax_amount` | 8 | PASS | min=4.0, max=35.0, avg=14.43625, stddev=10.224953 |
| 45 | `column_distribution_discount_percent` | 8 | PASS | min=0.0, max=15.0, avg=6.0, stddev=5.934163 |
| 46 | `null_validation:transaction_id` | 9 | PASS | 0 nulls |
| 47 | `null_validation:transaction_uuid` | 9 | PASS | 0 nulls |
| 48 | `null_validation:customer_id` | 9 | PASS | 0 nulls |
| 49 | `null_validation:customer_email` | 9 | PASS | 0 nulls |
| 50 | `null_validation:country_code` | 9 | PASS | 0 nulls |
| 51 | `null_validation:transaction_amount` | 9 | PASS | 0 nulls |
| 52 | `null_validation:tax_amount` | 9 | PASS | 0 nulls |
| 53 | `null_validation:discount_percent` | 9 | PASS | 0 nulls |
| 54 | `null_validation:payment_method` | 9 | PASS | 0 nulls |
| 55 | `null_validation:transaction_status` | 9 | PASS | 0 nulls |
| 56 | `null_validation:is_fraud` | 9 | PASS | 0 nulls |
| 57 | `null_validation:is_first_purchase` | 9 | PASS | 0 nulls |
| 58 | `null_validation:product_ids` | 9 | PASS | 0 nulls |
| 59 | `null_validation:metadata_json` | 9 | PASS | 0 nulls (JSON column) |
| 60 | `null_validation:transaction_date` | 9 | PASS | 0 nulls |
| 61 | `null_validation:transaction_timestamp` | 9 | PASS | 0 nulls |
| 62 | `null_validation:ingestion_timestamp` | 9 | PASS | 0 nulls |
| 63 | `column_checksum:transaction_id` | 10 | PASS | Both sides non-zero |
| 64 | `column_checksum:transaction_uuid` | 10 | PASS | Both sides non-zero |
| 65 | `column_checksum:customer_id` | 10 | PASS | Both sides non-zero |
| 66 | `column_checksum:customer_email` | 10 | PASS | Both sides non-zero |
| 67 | `column_checksum:country_code` | 10 | PASS | Both sides non-zero |
| 68 | `column_checksum:transaction_amount` | 10 | PASS | Both sides non-zero |
| 69 | `column_checksum:tax_amount` | 10 | PASS | Both sides non-zero |
| 70 | `column_checksum:discount_percent` | 10 | PASS | Both sides non-zero |
| 71 | `column_checksum:payment_method` | 10 | PASS | Both sides non-zero |
| 72 | `column_checksum:transaction_status` | 10 | PASS | Both sides non-zero |
| 73 | `column_checksum:is_fraud` | 10 | PASS | Both sides non-zero (7×false, 1×true — no XOR collision) |
| 74 | `column_checksum:product_ids` | 10 | PASS | Both sides non-zero |
| 75 | `column_checksum:metadata_json` | 10 | PASS | Both sides non-zero (JSON column) |
| 76 | `column_checksum:transaction_date` | 10 | PASS | Both sides non-zero |
| 77 | `column_checksum:transaction_timestamp` | 10 | PASS | Both sides non-zero |
| 78 | `column_checksum:ingestion_timestamp` | 10 | PASS | Both sides non-zero |

> ℹ️ `is_first_purchase` is intentionally excluded from Layer 10 (column_checksum). The dataset has exactly 4 `true` + 4 `false` values — the XOR hash reduces to 0 on both sides, producing a false FAIL. The column is fully validated by Layers 4 (test #31), 7, and 9 (test #57). See [VALIDATORS.md — XOR collision limitation](VALIDATORS.md#xor-collision-limitation).

> ℹ️ Aggregate min/max tests for NUMERIC columns (tests #13, #14, #17, #18) show different `expected` vs `actual` types in the results JSON (Python float vs BQ string) but still PASS via type-tolerant numeric comparison. Similarly, TIMESTAMP min/max tests (#34–#37) normalise the BQ ISO-8601 format before comparison. See [VALIDATORS.md — Type-tolerant comparison](VALIDATORS.md#type-tolerant-comparison-for-min-and-max).

---

## Test Name Patterns

| Pattern | Layer | Count |
|---------|-------|-------|
| `schema_validation` | 1 | Always 1 |
| `row_count_validation` | 2 | Always 1 |
| `primary_key_uniqueness_file` | 3 | Always 1 (when PKs set) |
| `primary_key_null_check_file` | 3 | Always 1 (when PKs set) |
| `primary_key_uniqueness_bq` | 3 | Always 1 (when PKs set) |
| `aggregate_validation_{column}_{function}` | 4 | 1 per (column, function) pair |
| `partition_validation` | 5 | Always 1 (when partition enabled) |
| `hash_validation` | 6 | Always 1 |
| `random_sampling_validation` | 7 | Always 1 (when PKs set) |
| `column_distribution_{column}` | 8 | 1 per distribution column |
| `null_validation:{column}` | 9 | 1 per null-checked column |
| `column_checksum:{column}` | 10 | 1 per checksum column |

---

## Test Count Summary

| Test Suite | File Type | Rows | Columns | Layers Run | Total Tests | Status |
|------------|-----------|------|---------|------------|-------------|--------|
| test1 — `generic_file_load_test` | CSV | 5 | 13 | 1–4, 6–8 | 15 | ✅ PASS |
| test2\_nested — `orders_with_nested` (Run 1) | JSONL | 3 | 9 | 1–4, 6–8 | 21 | ✅ PASS |
| test2\_nested — `orders_with_nested` (Run 2) | JSONL | 3 | 9 | 1–4, 6–10 | 39 | ⏳ Pending credential refresh |
| test3\_complex\_csv — `customer_transactions` | CSV | 8 | 17 | 1–10 | 78 | ✅ PASS |

**Grand total of distinct test case types supported:** 12 test name patterns across 10 validation layers.
