# Validation Layers Reference

This document describes each of the 10 validation layers in the BigQuery Data Validation Framework — what it checks, how it works, what the output looks like, and how to handle failures.

---

## Table of Contents

- [Layer 1 — Schema (Metadata) Validation](#layer-1--schema-metadata-validation)
- [Layer 2 — Row Count Validation](#layer-2--row-count-validation)
- [Layer 3 — Primary Key Uniqueness](#layer-3--primary-key-uniqueness)
- [Layer 4 — Aggregate Validation](#layer-4--aggregate-validation)
- [Layer 5 — Partition Validation](#layer-5--partition-validation)
- [Layer 6 — Hash Validation](#layer-6--hash-validation)
- [Layer 7 — Random Sampling Validation](#layer-7--random-sampling-validation)
- [Layer 8 — Column Distribution Validation](#layer-8--column-distribution-validation)
- [Layer 9 — Null Value Validation](#layer-9--null-value-validation)
- [Layer 10 — Column-Level Checksum Validation](#layer-10--column-level-checksum-validation)
- [Status Reference](#status-reference)
- [Enabling and Disabling Layers](#enabling-and-disabling-layers)

---

## Layer 1 — Schema (Metadata) Validation

**Config flag:** `metadata_validation`  
**Test name:** `schema_validation`  
**Validator:** `validators/schema_validator.py`

### What it checks

Compares the set of column names in the source file against the column names in the BigQuery table schema. Works with both CSV and JSONL source files.

- Detects columns present in the file but **missing from BigQuery**
- Detects columns present in BigQuery but **missing from the file**
- Reports the BQ data type for each matched column (for reference only — types are not validated)
- For tables with `STRUCT` (`RECORD`) or `ARRAY` (`REPEATED`) columns, the top-level column name is compared (e.g. `customer_info`, `item_ids`) — nested sub-field names are not individually checked

### How it works

1. Reads column names from the source file (CSV header row or JSONL top-level keys via `FileReader`).
2. Fetches the BigQuery table schema via the BigQuery API.
3. Computes the symmetric difference of the two column name sets.
4. Reports `PASS` if all column names match exactly; `FAIL` otherwise.

### Config

```yaml
validation_layers:
  metadata_validation: true
```

### Output example — PASS

```json
{
  "test_name": "schema_validation",
  "status": "PASS",
  "expected": ["amount", "email", "id", "name"],
  "actual":   ["amount", "email", "id", "name"],
  "details": {
    "common_columns_count": 4,
    "missing_from_file": [],
    "extra_in_file": [],
    "bq_column_types": [
      "amount: BQ=NUMERIC",
      "email: BQ=STRING",
      "id: BQ=INTEGER",
      "name: BQ=STRING"
    ]
  }
}
```

### Output example — FAIL

```json
{
  "test_name": "schema_validation",
  "status": "FAIL",
  "details": {
    "missing_from_file": ["created_at"],
    "extra_in_file": ["legacy_field"]
  }
}
```

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| Column in BQ, missing from file | File was exported without all columns; schema mismatch |
| Column in file, missing from BQ | BQ table was created from an older schema; column renamed |

---

## Layer 2 — Row Count Validation

**Config flag:** `row_count_validation`  
**Test name:** `row_count_validation`  
**Validator:** `validators/row_count_validator.py`

### What it checks

Compares the total number of data rows in the source file (CSV or JSONL) against the total row count in the BigQuery table.

### How it works

1. Counts rows in the CSV file (excluding the header).
2. Executes `SELECT COUNT(*) FROM dataset.table` in BigQuery.
3. Reports `PASS` if counts are equal; `FAIL` otherwise.
4. Reports the absolute delta and percentage delta.

### Config

```yaml
validation_layers:
  row_count_validation: true
```

### Output example — PASS

```json
{
  "test_name": "row_count_validation",
  "status": "PASS",
  "expected": 5,
  "actual": 5,
  "details": {
    "file_row_count": 5,
    "bq_row_count": 5,
    "delta": 0,
    "delta_pct": 0.0
  }
}
```

### Output example — FAIL

```json
{
  "test_name": "row_count_validation",
  "status": "FAIL",
  "expected": 1000,
  "actual": 998,
  "details": {
    "file_row_count": 1000,
    "bq_row_count": 998,
    "delta": -2,
    "delta_pct": -0.2
  }
}
```

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| BQ count < file count | Rows were rejected or lost during load |
| BQ count > file count | Table was not truncated before reload; duplicate loads |
| BQ count = 0 | Load job failed silently; wrong table/dataset |

---

## Layer 3 — Primary Key Uniqueness

**Config flag:** `primary_key_uniqueness`  
**Test names:** `primary_key_uniqueness_file`, `primary_key_null_check_file`, `primary_key_uniqueness_bq`  
**Validator:** `validators/primary_key_validator.py`

### What it checks

Runs three separate sub-checks:

1. **File PK uniqueness** — no duplicate PK combinations in the CSV file
2. **File PK null check** — no null/empty values in any PK column in the file
3. **BQ PK uniqueness** — no duplicate PK combinations in the BigQuery table

Supports composite primary keys (multiple columns).

### How it works

1. Groups CSV rows by the primary key column(s); counts groups with more than 1 row.
2. Counts rows where any PK column is null or empty.
3. Executes a BigQuery `GROUP BY ... HAVING COUNT(*) > 1` query to detect BQ duplicates.

### Config

```yaml
primary_keys:
  - id
  # Add more columns for a composite key

validation_layers:
  primary_key_uniqueness: true
```

> If `primary_keys` is empty, this layer is skipped automatically.

### Output example — all PASS

```json
[
  {
    "test_name": "primary_key_uniqueness_file",
    "status": "PASS",
    "expected": 0,
    "actual": 0,
    "details": { "primary_keys": ["id"], "duplicate_rows": 0 }
  },
  {
    "test_name": "primary_key_null_check_file",
    "status": "PASS",
    "expected": 0,
    "actual": 0,
    "details": { "primary_keys": ["id"], "null_pk_rows": 0 }
  },
  {
    "test_name": "primary_key_uniqueness_bq",
    "status": "PASS",
    "expected": 0,
    "actual": 0,
    "details": { "primary_keys": ["id"], "duplicate_pk_groups_in_bq": 0 }
  }
]
```

### Common failure modes

| Sub-check | Failure | Likely cause |
|-----------|---------|-------------|
| File uniqueness | Duplicates in file | Upstream process generated duplicate rows |
| File null check | Null PKs in file | Source system sent rows without a key |
| BQ uniqueness | Duplicates in BQ | Table not truncated before reload; multiple load jobs |

---

## Layer 4 — Aggregate Validation

**Config flag:** `aggregate_validation`  
**Test names:** `aggregate_validation_{column}_{function}`  
**Validator:** `validators/aggregate_validator.py`

### What it checks

For each column and function listed under `aggregate_columns`, computes the aggregate value on the file side (using pandas) and on the BigQuery side (SQL query), then compares them.

### How it works

1. For each `(column, function)` pair in `aggregate_columns`:
   - Computes the value from the CSV using pandas (e.g. `df[column].sum()`).
   - Executes the equivalent SQL in BigQuery (e.g. `SELECT SUM(column) FROM ...`).
   - Compares the two values with floating-point tolerance.
2. Reports one result per `(column, function)` pair.

### Config

```yaml
validation_layers:
  aggregate_validation: true

aggregate_columns:
  - column: amount
    functions: [sum, min, max, avg]
  - column: email
    functions: [distinct_count]
```

### Supported functions

| Function | Config value | SQL equivalent | Suitable column types |
|----------|-------------|----------------|----------------------|
| Sum | `sum` | `SUM(column)` | INTEGER, FLOAT, NUMERIC |
| Minimum | `min` | `MIN(column)` | INTEGER, FLOAT, NUMERIC, DATE, TIMESTAMP, STRING |
| Maximum | `max` | `MAX(column)` | INTEGER, FLOAT, NUMERIC, DATE, TIMESTAMP, STRING |
| Average | `avg` | `AVG(column)` | INTEGER, FLOAT, NUMERIC |
| Distinct count | `distinct_count` | `COUNT(DISTINCT column)` | Any type |

### Output example — PASS

```json
{
  "test_name": "aggregate_validation_amount_sum",
  "status": "PASS",
  "expected": 831.6,
  "actual": 831.6,
  "details": {
    "column": "amount",
    "function": "sum",
    "file_value": 831.6,
    "bq_value": 831.6
  }
}
```

### Type-tolerant comparison for `min` and `max`

BigQuery returns `min` and `max` values for `NUMERIC` columns as strings (e.g. `"4"` instead of `4.0`), and for `TIMESTAMP` columns in ISO-8601 format with a `T` separator and UTC offset (e.g. `"2025-02-01T10:15:30+00:00"` instead of `"2025-02-01 10:15:30"`).

The validator handles these differences automatically:

| Column type | File side | BQ side | Normalisation |
|-------------|-----------|---------|---------------|
| INTEGER / FLOAT / NUMERIC | Python number | Python number | Compared with floating-point tolerance |
| NUMERIC (min/max) | Python float | String e.g. `"60.25"` | Both converted to `float` before comparison |
| TIMESTAMP (min/max) | String `"YYYY-MM-DD HH:MM:SS"` | ISO-8601 `"YYYY-MM-DDTHH:MM:SS+00:00"` | T-separator replaced with space; `+00:00` suffix stripped; compared as strings |
| DATE, STRING | String | String | Direct string comparison |

This means the `expected` and `actual` values in the JSON output for NUMERIC min/max and TIMESTAMP min/max **will look different** but the test still PASses:

```json
{
  "test_name": "aggregate_validation_tax_amount_min",
  "status": "PASS",
  "expected": 4.0,
  "actual": "4",
  ...
}
```

```json
{
  "test_name": "aggregate_validation_transaction_timestamp_min",
  "status": "PASS",
  "expected": "2025-02-01 10:15:30",
  "actual": "2025-02-01T10:15:30+00:00",
  ...
}
```

Both are by-design: the comparison logic correctly resolves the type and format difference before deciding PASS/FAIL.

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| Sum mismatch | Rows missing from BQ; numeric precision difference |
| Distinct count mismatch | Duplicate rows loaded; case-sensitivity difference |
| BigQuery error on avg/sum | Column is STRING type — use `distinct_count` instead |

---

## Layer 5 — Partition Validation

**Config flag:** `partition_validation` AND `partition.enabled: true`  
**Test name:** `partition_validation`  
**Validator:** `validators/partition_validator.py`

### What it checks

Groups rows by the partition column and compares per-partition row counts between the file and BigQuery. Detects:

- Partitions in the file but missing from BigQuery
- Partitions in BigQuery but missing from the file
- Partitions where row counts differ

### How it works

1. Groups the CSV by the partition column and counts rows per group.
2. Executes `SELECT partition_col, COUNT(*) FROM dataset.table GROUP BY partition_col` in BigQuery.
3. Compares partition-by-partition row counts.

### Config

```yaml
partition:
  enabled: true
  column: event_date
  type: DATE          # DATE or TIMESTAMP

validation_layers:
  partition_validation: true
```

Both `partition.enabled: true` and `partition_validation: true` must be set. If `partition.enabled` is `false`, the layer reports `SKIPPED` regardless of the flag.

### Output example — PASS

```json
{
  "test_name": "partition_validation",
  "status": "PASS",
  "expected": 30,
  "actual": 30,
  "details": {
    "partition_column": "event_date",
    "partition_type": "DATE",
    "total_partitions_checked": 30,
    "matching_partitions": 30,
    "mismatched_partitions": 0,
    "missing_in_bq": [],
    "missing_in_file": []
  }
}
```

### Output example — FAIL

```json
{
  "test_name": "partition_validation",
  "status": "FAIL",
  "details": {
    "mismatches": [
      {
        "partition": "2026-03-05",
        "file_count": 150,
        "bq_count": 148,
        "delta": -2,
        "issue": "Row count mismatch"
      },
      {
        "partition": "2026-03-06",
        "file_count": 200,
        "bq_count": null,
        "issue": "Partition exists in file but not in BQ"
      }
    ]
  }
}
```

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| Partition missing from BQ | Partial load; partition filter on load job |
| Row count mismatch per partition | Rows rejected in one partition only |
| Partition in BQ not in file | Stale data from a previous load still in BQ |

---

## Layer 6 — Hash Validation

**Config flag:** `hash_validation`  
**Test name:** `hash_validation`  
**Validator:** `validators/hash_validator.py`

### What it checks

Computes a single aggregate hash value over all rows in the file and all rows in BigQuery. The purpose is to confirm that **both sides contain a non-empty, consistent dataset** — not to perform a bit-exact comparison.

### How it works

- **File side:** Computes `XOR` of Python `hash(row_string)` for every row.
- **BQ side:** Executes `SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) FROM table t`.
- Both algorithms produce a non-zero result when data is present.
- The test **PASSes** if both hashes are non-zero (confirming data exists on both sides).

> ⚠️ **Note:** The two hash values will never be numerically equal because the algorithms differ (Python `hash()` vs Google `FARM_FINGERPRINT`). This is expected and documented in the output `details.note`. For bit-exact comparison, both sides would need to use the same algorithm.

### Config

```yaml
validation_layers:
  hash_validation: true
```

No additional config needed.

### Output example — PASS

```json
{
  "test_name": "hash_validation",
  "status": "PASS",
  "expected": "15337628627536868632",
  "actual": "-1770177170011187938",
  "details": {
    "file_hash_algorithm": "Python XOR(hash(row_string))",
    "bq_hash_algorithm": "BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(row)))",
    "file_hash": "15337628627536868632",
    "bq_hash": "-1770177170011187938",
    "file_row_count": 5,
    "note": "Both file and BQ produced non-zero hash aggregates. Hash values differ due to algorithm difference — this is expected."
  }
}
```

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| One hash is zero | Table is empty or file has no data rows |
| BigQuery error | Insufficient permissions; table does not exist |

---

## Layer 7 — Random Sampling Validation

**Config flag:** `random_sampling`  
**Test name:** `random_sampling_validation`  
**Validator:** `validators/random_sample_validator.py`

### What it checks

Randomly selects `random_sample_size` rows from the source file (CSV or JSONL), then fetches the corresponding rows from BigQuery using the primary key values. Performs a **field-level comparison** for each sampled row, including support for `STRUCT` (dict) and `ARRAY` (list) typed columns.

### How it works

1. Randomly samples up to `random_sample_size` rows from the source file.
2. Fetches the matching rows from BigQuery using a `WHERE (pk1, pk2, ...) IN (...)` query.
3. For each sampled row, compares every field value between file and BigQuery:
   - **Scalar fields** — normalised string comparison with numeric tolerance
   - **STRUCT fields** — deep dict equality, key-order independent (BigQuery returns keys alphabetically; JSONL preserves insertion order)
   - **ARRAY fields** — element-wise list comparison with nested-dict support
4. Reports mismatched fields per row.

### Config

```yaml
primary_keys:
  - id               # required — must be defined for this layer to run

random_sample_size: 100

validation_layers:
  random_sampling: true
```

> If `primary_keys` is empty, this layer is skipped automatically with a warning.

### Known limitation

> ⚠️ When composite primary keys include a **TIMESTAMP or non-STRING typed column**, the `IN` clause in the BQ query may fail with a type mismatch error (`STRUCT<INT64, TIMESTAMP>` vs `STRUCT<STRING, STRING>`). Workaround: set `random_sampling: false` in `validation_layers` for tables where a composite PK contains a TIMESTAMP column, or use a single STRING/INTEGER PK.

> ℹ️ **STRUCT and ARRAY columns are fully supported** in the field-level comparison. The validator handles dict key-order differences between JSONL source files and BigQuery automatically.

> ℹ️ **CSV JSON columns are fully supported.** When a CSV column contains a JSON payload (e.g. a `metadata_json` column stored as a JSON-formatted string in the CSV), the validator parses the string into a Python dict before comparing against the BigQuery JSON column, which is already returned as a dict by the BQ client. This prevents false mismatches caused by string-vs-dict type differences.

### Output example — PASS

```json
{
  "test_name": "random_sampling_validation",
  "status": "PASS",
  "details": {
    "sample_size": 5,
    "rows_fetched_from_bq": 5,
    "rows_with_mismatches": 0,
    "mismatches": []
  }
}
```

### Output example — FAIL

```json
{
  "test_name": "random_sampling_validation",
  "status": "FAIL",
  "details": {
    "sample_size": 5,
    "rows_fetched_from_bq": 5,
    "rows_with_mismatches": 1,
    "mismatches": [
      {
        "primary_key": { "id": 3 },
        "field": "amount",
        "file_value": "150.75",
        "bq_value": "150.00"
      }
    ]
  }
}
```

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| Field value mismatch | Data transformation applied during load; type coercion |
| Row not found in BQ | Row was filtered out or rejected during load |
| BQ query error (type mismatch) | Composite PK contains TIMESTAMP — disable this layer |

---

## Layer 8 — Column Distribution Validation

**Config flag:** `column_distribution`  
**Test names:** `column_distribution_{column}`  
**Validator:** `validators/distribution_validator.py`

### What it checks

For each column listed in `distribution_columns`, computes the following statistics on both the file (using pandas) and BigQuery (SQL), then compares them:

| Statistic | Description |
|-----------|-------------|
| `min_val` | Minimum value |
| `max_val` | Maximum value |
| `avg_val` | Arithmetic mean |
| `stddev_val` | Population standard deviation |
| `null_count` | Number of null / empty values |

Reports one result per column.

### How it works

1. For each column in `distribution_columns`:
   - Computes stats from the CSV using pandas.
   - Executes `SELECT MIN, MAX, AVG, STDDEV_POP, COUNTIF(IS NULL)` from BigQuery.
   - Compares each statistic with floating-point tolerance.
2. Reports `PASS` if all statistics match; `FAIL` if any differ.

### Config

```yaml
validation_layers:
  column_distribution: true

distribution_columns:
  - amount
  - quantity
  - score
```

### Known limitation

> ⚠️ **Only list scalar NUMERIC columns** (INTEGER, FLOAT, NUMERIC) in `distribution_columns`.  
> STRING, BOOLEAN, DATE, TIMESTAMP, STRUCT (`RECORD`), and ARRAY (`REPEATED`) columns will  
> cause a BigQuery query error (`Bad double value`) because the distribution SQL uses  
> `AVG()` and `STDDEV_POP()`.

### Output example — PASS

```json
{
  "test_name": "column_distribution_quantity",
  "status": "PASS",
  "expected": {
    "min_val": 1.0,
    "max_val": 4.0,
    "avg_val": 2.4,
    "stddev_val": 1.140175,
    "null_count": 0
  },
  "actual": {
    "min_val": 1.0,
    "max_val": 4.0,
    "avg_val": 2.4,
    "stddev_val": 1.140175,
    "null_count": 0
  },
  "details": {
    "column": "quantity",
    "statistics": {
      "min_val":    { "file": 1.0, "bq": 1.0, "match": true },
      "max_val":    { "file": 4.0, "bq": 4.0, "match": true },
      "avg_val":    { "file": 2.4, "bq": 2.4, "match": true },
      "stddev_val": { "file": 1.140175, "bq": 1.140175, "match": true },
      "null_count": { "file": 0, "bq": 0, "match": true }
    }
  }
}
```

### Output example — FAIL

```json
{
  "test_name": "column_distribution_amount",
  "status": "FAIL",
  "details": {
    "column": "amount",
    "statistics": {
      "min_val":    { "file": 80.25, "bq": 80.25, "match": true },
      "max_val":    { "file": 300.10, "bq": 300.10, "match": true },
      "avg_val":    { "file": 166.32, "bq": 164.00, "match": false },
      "stddev_val": { "file": 79.12, "bq": 77.90, "match": false },
      "null_count": { "file": 0, "bq": 0, "match": true }
    }
  }
}
```

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| avg/stddev mismatch | Rows missing from BQ; numeric rounding difference during load |
| null_count mismatch | BQ treats empty strings differently from nulls |
| BQ query error | Column is STRING or BOOLEAN — remove from `distribution_columns` |

---

---

## Layer 9 — Null Value Validation

**Config flag:** `null_validation`
**Test names:** `null_validation:{column}`
**Validator:** `validators/null_validator.py`

### What it checks

For every column listed in `null_check_columns` (or **all** columns when the list is empty), counts the number of null / missing values in both the source file and BigQuery, then verifies the counts match.

A value is treated as null when it is:
- Python `None` or pandas `NaN` on the file side
- An empty string `""` on the file side (treated as null to match BQ behaviour)
- `IS NULL` on the BigQuery side (applies to all column types: STRING, NUMERIC, DATE, TIMESTAMP, STRUCT, ARRAY)

### How it works

1. Iterates over each target column in the file DataFrame and counts nulls.
2. Executes a single BigQuery query with `COUNTIF(column IS NULL)` for each column.
3. Compares null counts; reports `PASS` if they match, `FAIL` otherwise.

### Config

```yaml
validation_layers:
  null_validation: true

# Optional — leave empty to check ALL columns from the source file
null_check_columns:
  - order_id
  - order_amount
  - customer_info   # STRUCT columns supported
  - item_ids        # ARRAY columns supported
```

### Output example — PASS

```json
{
  "test_name": "null_validation:order_id",
  "status": "PASS",
  "expected": 0,
  "actual": 0,
  "details": {
    "file_null_count": 0,
    "bq_null_count": 0,
    "diff": 0
  },
  "execution_time_ms": 120.5
}
```

### Output example — FAIL

```json
{
  "test_name": "null_validation:order_amount",
  "status": "FAIL",
  "expected": 0,
  "actual": 2,
  "details": {
    "file_null_count": 0,
    "bq_null_count": 2,
    "diff": 2
  },
  "execution_time_ms": 118.3
}
```

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| BQ null count > file null count | Rows were not loaded; BQ default NULL applied on missing fields |
| File null count > BQ null count | BQ coerced empty strings to a default value (e.g. `0` for integers) |
| WARNING: column not found in file | STRUCT sub-field listed directly — use the top-level column name |

---

## Layer 10 — Column-Level Checksum Validation

**Config flag:** `column_checksum`
**Test names:** `column_checksum:{column}`
**Validator:** `validators/column_checksum_validator.py`

### What it checks

For every column listed in `column_checksum_columns` (or **all** columns when the list is empty), computes a deterministic hash aggregate on both the file and BigQuery sides, then verifies that **both are non-zero** (confirming data was loaded into every column).

Works with all column types: STRING, NUMERIC, DATE, TIMESTAMP, STRUCT (`RECORD`), and ARRAY (`REPEATED`).

### How it works

- **File side:** XOR of `hash(json_str(cell_value))` for every row in the column. Dicts (STRUCT) and lists (ARRAY) are serialised to JSON before hashing.
- **BQ side:** `BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(column)))` per column, in a single query.
- **Pass condition:** Both file hash ≠ 0 AND BQ hash ≠ 0 (data present on both sides).
- The absolute hash values differ by design (Python `hash()` ≠ FARM_FINGERPRINT) — this is expected and noted in the output.

> ℹ️ For bit-exact column comparison, use the **Random Sampling** layer (Layer 7) which performs field-level row comparison.

### Config

```yaml
validation_layers:
  column_checksum: true

# Optional — leave empty to checksum ALL columns from the source file
column_checksum_columns:
  - order_id
  - order_amount
  - customer_info   # STRUCT supported
  - item_ids        # ARRAY supported
```

### Output example — PASS

```json
{
  "test_name": "column_checksum:order_id",
  "status": "PASS",
  "expected": -7823456123456789012,
  "actual": 5612378901234567890,
  "details": {
    "file_column_hash": -7823456123456789012,
    "bq_column_hash": 5612378901234567890,
    "file_has_data": true,
    "bq_has_data": true,
    "note": "Hash algorithms differ (Python XOR vs BQ FARM_FINGERPRINT). PASS confirms both sides are non-zero (data present). Use hash_validation for full row-level integrity."
  },
  "execution_time_ms": 135.7
}
```

### Output example — FAIL

```json
{
  "test_name": "column_checksum:customer_info",
  "status": "FAIL",
  "expected": -7823456123456789012,
  "actual": 0,
  "details": {
    "file_column_hash": -7823456123456789012,
    "bq_column_hash": 0,
    "file_has_data": true,
    "bq_has_data": false,
    "note": "Hash algorithms differ (Python XOR vs BQ FARM_FINGERPRINT). PASS confirms both sides are non-zero (data present). Use hash_validation for full row-level integrity."
  },
  "execution_time_ms": 133.1
}
```

### XOR collision limitation

> ⚠️ **BOOLEAN / low-cardinality columns can produce a false FAIL.**  
> The XOR-based hash reduces to **0** whenever the same value appears an **even** number of times in a column. For `BOOL` columns (or any column where every distinct value occurs an even number of times), both the file hash and the BQ hash will be 0, which the validator interprets as "no data" → **FAIL** — even though the column has data.  
>
> **Example:** a `BOOL` column with exactly 4 `true` + 4 `false` rows:
> - File side: `hash("true") XOR hash("true") XOR hash("true") XOR hash("true") = 0`
> - BQ side: `BIT_XOR(FARM_FINGERPRINT("true"))` over 4 rows `= 0`
>
> **Workaround:** exclude such columns from `column_checksum_columns` explicitly. The column remains fully validated by Layer 9 (null count), Layer 4 (distinct_count), and Layer 7 (field-level sampling). See [`CONFIGURATION.md`](CONFIGURATION.md#column_checksum_columns) for the full guidance.

### Common failure modes

| Failure | Likely cause |
|---------|-------------|
| BQ hash = 0 | Column is entirely NULL in BQ — data was not loaded |
| File hash = 0 | Column is entirely NULL in the source file |
| Both hashes = 0 on a BOOL column | XOR collision — even count of each distinct value; exclude column and use Layers 4, 7, 9 instead |
| WARNING: column not returned | Column name mismatch between config and BQ schema |

---

## Status Reference

Every individual test result carries one of the following status values:

| Status | Meaning | `overall_status` impact |
|--------|---------|------------------------|
| `PASS` | Check passed — expected equals actual | Contributes to PASS |
| `FAIL` | Check failed — mismatch detected | Elevates overall to FAIL |
| `WARN` | Within acceptable tolerance but noteworthy | Elevates overall to WARN (unless a FAIL exists) |
| `ERROR` | An exception was thrown during the check | Elevates overall to ERROR |
| `SKIPPED` | Layer was disabled in config or prerequisites not met | No impact on overall status |

### `overall_status` precedence

`FAIL` > `ERROR` > `WARN` > `PASS` > `SKIPPED`

If any test is `FAIL`, the overall is `FAIL`. If no `FAIL` but there is an `ERROR`, overall is `ERROR`, and so on.

---

## Enabling and Disabling Layers

All 10 layers are enabled by default. To disable a layer, set its flag to `false`:

```yaml
validation_layers:
  metadata_validation: true     # Layer 1
  row_count_validation: true    # Layer 2
  primary_key_uniqueness: true  # Layer 3
  aggregate_validation: true    # Layer 4
  partition_validation: false   # Layer 5 — also needs partition.enabled: true
  hash_validation: true         # Layer 6
  random_sampling: false        # Layer 7 — disabled when PK is TIMESTAMP
  column_distribution: true     # Layer 8
  null_validation: true         # Layer 9
  column_checksum: true         # Layer 10
```

### Special prerequisites

| Layer | Additional requirement |
|-------|----------------------|
| Layer 3 (PK Uniqueness) | `primary_keys` must be non-empty |
| Layer 5 (Partition) | `partition.enabled: true` must also be set |
| Layer 7 (Random Sampling) | `primary_keys` must be non-empty |
| Layer 4 (Aggregate) | `aggregate_columns` must be non-empty to produce results |
| Layer 8 (Distribution) | `distribution_columns` must list only NUMERIC columns |
| Layer 9 (Null) | `null_check_columns` may be empty (checks all columns) |
| Layer 10 (Column Checksum) | `column_checksum_columns` may be empty (checks all columns) |

### Recommended minimal config (fast smoke test)

For a quick smoke test with no BQ sampling cost:

```yaml
validation_layers:
  metadata_validation: true
  row_count_validation: true
  primary_key_uniqueness: true
  aggregate_validation: false
  partition_validation: false
  hash_validation: false
  random_sampling: false
  column_distribution: false
  null_validation: false
  column_checksum: false
```

### Recommended full config (thorough validation)

```yaml
validation_layers:
  metadata_validation: true
  row_count_validation: true
  primary_key_uniqueness: true
  aggregate_validation: true
  partition_validation: true    # requires partition.enabled: true
  hash_validation: true
  random_sampling: true         # requires STRING/INTEGER-only PK
  column_distribution: true     # requires NUMERIC-only distribution_columns
  null_validation: true
  column_checksum: true
```
