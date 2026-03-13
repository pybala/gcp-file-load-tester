# Configuration Reference

This document describes every field in the BigQuery Data Validation Framework YAML configuration file.

The annotated reference file is at [`config/validation_config_reference.yaml`](config/validation_config_reference.yaml).

---

## Table of Contents

- [Top-Level Fields](#top-level-fields)
- [primary\_keys](#primary_keys)
- [partition](#partition)
- [random\_sample\_size](#random_sample_size)
- [validation\_layers](#validation_layers)
- [aggregate\_columns](#aggregate_columns)
- [distribution\_columns](#distribution_columns)
- [null\_check\_columns](#null_check_columns)
- [column\_checksum\_columns](#column_checksum_columns)
- [Config Loading Modes](#config-loading-modes)
- [Full Example](#full-example)
- [Minimal Example](#minimal-example)

---

## Top-Level Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `project` | string | No | Inferred from credentials | GCP project ID that owns the BigQuery dataset. If omitted, the SDK infers the project from the service account key or the `GOOGLE_CLOUD_PROJECT` environment variable. |
| `dataset` | string | **Yes** | — | BigQuery dataset name. |
| `table` | string | **Yes** | — | BigQuery table name. |
| `file_path` | string | **Yes** | — | Path to the source data file. Accepts a local absolute path, a local relative path, or a GCS URI (`gs://`). Supported formats: `.csv` (comma-delimited) and `.jsonl` (newline-delimited JSON). Format is detected automatically from the file extension. |
| `primary_keys` | list\<string\> | No | `[]` | Column names that form the primary key. See [primary\_keys](#primary_keys). |
| `partition` | object | No | `{enabled: false}` | Partition configuration. See [partition](#partition). |
| `random_sample_size` | integer | No | `100` | Number of rows to sample for Layer 7. See [random\_sample\_size](#random_sample_size). |
| `validation_layers` | object | No | all `true` | Toggles for each of the 10 validation layers. See [validation\_layers](#validation_layers). |
| `aggregate_columns` | list\<object\> | No | `[]` | Column + function specs for Layer 4. See [aggregate\_columns](#aggregate_columns). |
| `distribution_columns` | list\<string\> | No | `[]` | Numeric column names for Layer 8. See [distribution\_columns](#distribution_columns). |
| `null_check_columns` | list\<string\> | No | `[]` | Columns to null-check in Layer 9. Empty = all columns. See [null\_check\_columns](#null_check_columns). |
| `column_checksum_columns` | list\<string\> | No | `[]` | Columns to checksum in Layer 10. Empty = all columns. See [column\_checksum\_columns](#column_checksum_columns). |

---

## `primary_keys`

```yaml
primary_keys:
  - id
  - event_timestamp   # composite key — list as many columns as needed
```

- **Type:** list of strings
- **Default:** `[]` (empty — no PK validation or random sampling)
- **Used by:** Layer 3 (Primary Key Uniqueness), Layer 7 (Random Sampling)

**Rules:**
- Single-column keys: list one column name.
- Composite keys: list multiple column names.
- If empty, Layer 3 is skipped and Layer 7 is also skipped automatically with a warning.
- Column names must exist in both the file (CSV header row or JSONL top-level keys) and the BigQuery schema.

**Example (single key):**
```yaml
primary_keys:
  - id
```

**Example (composite key):**
```yaml
primary_keys:
  - user_id
  - event_date
```

---

## `partition`

```yaml
partition:
  enabled: true
  column: event_date
  type: DATE
```

| Sub-field | Type | Required when enabled | Default | Description |
|-----------|------|-----------------------|---------|-------------|
| `enabled` | boolean | — | `false` | Set to `true` to activate partition validation (Layer 5). |
| `column` | string | Yes | — | Name of the partition column in both the file and BigQuery. |
| `type` | string | Yes | — | Data type of the partition column. Allowed values: `DATE`, `TIMESTAMP`. |

**Notes:**
- `partition_validation` in `validation_layers` must also be `true` for Layer 5 to run.
- If `enabled: false`, Layer 5 is reported as `SKIPPED` regardless of the `partition_validation` flag.
- `column` and `type` are **required** when `enabled: true` — omitting them causes a configuration validation error at startup.

---

## `random_sample_size`

```yaml
random_sample_size: 100
```

- **Type:** integer ≥ 1
- **Default:** `100`
- **Used by:** Layer 7 (Random Sampling)

The framework randomly selects up to this many rows from the source CSV file, then fetches the corresponding rows from BigQuery using the primary keys, and performs a field-by-field comparison.

**Notes:**
- If the file contains fewer rows than `random_sample_size`, all rows are sampled.
- Requires `primary_keys` to be defined; if `primary_keys` is empty, Layer 7 is skipped.
- For large tables, a value of 50–200 provides good coverage without excessive BQ query cost.

---

## `validation_layers`

```yaml
validation_layers:
  metadata_validation: true
  row_count_validation: true
  primary_key_uniqueness: true
  aggregate_validation: true
  partition_validation: false
  hash_validation: true
  random_sampling: true
  column_distribution: true
  null_validation: true
  column_checksum: true
```

Each flag is a boolean. Setting a flag to `false` skips that layer entirely — it appears in the output with `status: SKIPPED`.

| Flag | Default | Layer # | Description |
|------|---------|---------|-------------|
| `metadata_validation` | `true` | 1 | Schema/column name comparison |
| `row_count_validation` | `true` | 2 | Total row count comparison |
| `primary_key_uniqueness` | `true` | 3 | Duplicate and null PK detection |
| `aggregate_validation` | `true` | 4 | Per-column aggregate comparison |
| `partition_validation` | `true` | 5 | Per-partition row count comparison |
| `hash_validation` | `true` | 6 | Aggregate hash completeness check |
| `random_sampling` | `true` | 7 | Field-level row sampling comparison |
| `column_distribution` | `true` | 8 | Statistical distribution comparison |
| `null_validation` | `true` | 9 | Null count comparison per column |
| `column_checksum` | `true` | 10 | Per-column hash completeness check |

**Notes:**
- The entire `validation_layers` block is optional. If omitted, all 10 layers default to `true`.
- Individual flags can be omitted too — unspecified flags default to `true`.
- Layer 5 (`partition_validation`) also requires `partition.enabled: true` to produce results.
- Layer 9 (`null_validation`) checks all file columns when `null_check_columns` is empty.
- Layer 10 (`column_checksum`) checks all file columns when `column_checksum_columns` is empty.

---

## `aggregate_columns`

```yaml
aggregate_columns:
  - column: amount
    functions:
      - sum
      - min
      - max
      - avg
  - column: email
    functions:
      - distinct_count
  - column: event_date
    functions:
      - min
      - max
```

- **Type:** list of objects
- **Default:** `[]` (no aggregate checks run even if Layer 4 is enabled)
- **Used by:** Layer 4 (Aggregate Validation)

Each entry specifies a **column name** and a list of **aggregate functions** to compute and compare between file and BigQuery.

### Supported functions

| Function | Config value | SQL equivalent | Suitable column types |
|----------|-------------|----------------|----------------------|
| Sum | `sum` | `SUM(column)` | INTEGER, FLOAT, NUMERIC |
| Minimum | `min` | `MIN(column)` | INTEGER, FLOAT, NUMERIC, DATE, TIMESTAMP, STRING |
| Maximum | `max` | `MAX(column)` | INTEGER, FLOAT, NUMERIC, DATE, TIMESTAMP, STRING |
| Average | `avg` | `AVG(column)` | INTEGER, FLOAT, NUMERIC |
| Distinct count | `distinct_count` | `COUNT(DISTINCT column)` | Any type |

### Rules
- `column` must match the column name in the BigQuery schema exactly (case-sensitive).
- The `functions` list must contain at least one valid function value.
- Using `sum` or `avg` on a STRING column will cause a BigQuery query error.
- `min` and `max` work on dates, timestamps, and strings — useful for range checks.
- `distinct_count` works on any data type and is safe to use on string/categorical columns.
- **Do not list STRUCT (`RECORD`) or ARRAY (`REPEATED`) columns** — they cannot be aggregated with numeric functions and will cause a BigQuery query error. Use scalar columns only.

### Examples by use case

**Numeric column — full aggregate suite:**
```yaml
- column: revenue
  functions: [sum, min, max, avg]
```

**Date column — range check:**
```yaml
- column: event_date
  functions: [min, max]
```

**String/email column — uniqueness proxy:**
```yaml
- column: customer_email
  functions: [distinct_count]
```

---

## `distribution_columns`

```yaml
distribution_columns:
  - amount
  - quantity
  - score
```

- **Type:** list of strings
- **Default:** `[]` (no distribution checks run even if Layer 8 is enabled)
- **Used by:** Layer 8 (Column Distribution Validation)

For each column listed, the following statistics are computed on both the file and BigQuery, then compared:

| Statistic | Description |
|-----------|-------------|
| `min_val` | Minimum value |
| `max_val` | Maximum value |
| `avg_val` | Arithmetic mean |
| `stddev_val` | Population standard deviation |
| `null_count` | Number of null / empty values |

### Rules

> ⚠️ **Only list scalar NUMERIC columns here** (INTEGER, FLOAT, NUMERIC).  
> STRING, BOOLEAN, DATE, TIMESTAMP, STRUCT (`RECORD`), and ARRAY (`REPEATED`) columns  
> will cause a BigQuery query error (`Bad double value`) because the distribution query  
> uses `AVG()` and `STDDEV_POP()`.

- Column names must match the BigQuery schema exactly.
- If `distribution_columns` is empty (`[]`), Layer 8 produces no results but is still reported as run.

---

## `null_check_columns`

```yaml
null_check_columns:
  - order_id
  - order_amount
  - customer_info   # STRUCT (top-level field name)
  - item_ids        # ARRAY (top-level field name)
```

- **Type:** list of strings
- **Default:** `[]` (empty list — all columns from the source file are checked)
- **Used by:** Layer 9 (Null Value Validation)

For each listed column, the validator counts null / missing values in the source file and in BigQuery, then compares the two counts.

### Rules

- Column names must match the BigQuery schema exactly (case-sensitive).
- Leave the list **empty** to automatically check every column present in the source file.
- Provide an explicit list to limit checks to the most important columns (faster, less noise).
- Both scalar (STRING, INTEGER, FLOAT, DATE, TIMESTAMP) and nested (STRUCT, ARRAY) column types are supported — use the **top-level** column name only.
- Do **not** list individual STRUCT sub-fields (e.g. `customer_info.name`) — use `customer_info` as the column name.

### Examples

**Check specific columns only:**
```yaml
null_check_columns:
  - order_id
  - order_amount
  - order_status
```

**Check all columns (leave empty):**
```yaml
null_check_columns: []
```

---

## `column_checksum_columns`

```yaml
column_checksum_columns:
  - order_id
  - order_amount
  - customer_info   # STRUCT supported
  - item_ids        # ARRAY supported
```

- **Type:** list of strings
- **Default:** `[]` (empty list — all columns from the source file are checksummed)
- **Used by:** Layer 10 (Column-Level Checksum Validation)

For each listed column, the validator computes a hash aggregate on the file side and on BigQuery, then checks that **both are non-zero** (confirming data was loaded into every column).

### Rules

- Column names must match the BigQuery schema exactly (case-sensitive).
- Leave the list **empty** to automatically checksum every column present in the source file.
- Both scalar and nested (STRUCT, ARRAY) column types are supported.
- The file-side and BQ-side hash algorithms differ (Python `hash()` vs `FARM_FINGERPRINT`), so the absolute values will never match — the check verifies **data presence** (non-zero), not equality.
- For bit-exact field comparison, use Layer 7 (Random Sampling) instead.

> ⚠️ **XOR collision limitation — BOOLEAN and low-cardinality columns**  
> The XOR-based hash reduces to **0** whenever the same value appears an **even** number of times in a column. For BOOLEAN columns (or any column where every distinct value occurs an even number of times), both the file hash and the BQ hash will be 0, and the validator will report FAIL even though the column has data.  
>
> **Example:** a `BOOL` column with exactly 4 `true` + 4 `false` rows:
> - File side: `hash("true") XOR hash("true") XOR hash("true") XOR hash("true") = 0`
> - BQ side: `BIT_XOR(FARM_FINGERPRINT("true"))` over 4 rows `= 0`  
> Both sides return 0 — this is a false positive failure, not a real load error.  
>
> **Workaround:** exclude such columns from `column_checksum_columns` explicitly. These columns remain validated by:
> - **Layer 9 (Null Validation)** — null count comparison confirms data is present
> - **Layer 4 (Aggregate)** — `distinct_count` confirms the expected number of unique values
> - **Layer 7 (Random Sampling)** — field-level row-by-row comparison
>
> **Rule of thumb:** avoid listing `BOOL` columns, or any column where all distinct values appear an even number of times in the specific dataset being tested.

### Examples

**Checksum specific columns only:**
```yaml
column_checksum_columns:
  - order_id
  - order_amount
  - customer_info
  - item_ids
```

**Checksum all columns (leave empty):**
```yaml
column_checksum_columns: []
```

---

## Config Loading Modes

The same YAML schema is supported in all three loading modes:

### 1. Local CLI — file path

```bash
python main.py --config /path/to/validation_config.yaml
python main.py --config tests/test1/validation_config.yaml
```

### 2. Local CLI — GCS URI

```bash
python main.py --config gs://my-bucket/configs/validation_config.yaml
```

Requires `google-cloud-storage` and valid GCP credentials.

### 3. Cloud Function — GCS config path

```json
{ "config_path": "gs://my-bucket/configs/validation_config.yaml" }
```

### 4. Cloud Function — inline config dict

```json
{
  "config": {
    "dataset": "my_dataset",
    "table": "my_table",
    "file_path": "gs://my-bucket/data/myfile.csv",
    "primary_keys": ["id"],
    "validation_layers": {
      "metadata_validation": true,
      "row_count_validation": true,
      "primary_key_uniqueness": true,
      "aggregate_validation": false,
      "partition_validation": false,
      "hash_validation": true,
      "random_sampling": false,
      "column_distribution": false
    }
  }
}
```

---

## Full Example

```yaml
# GCP target
project: my-gcp-project-id
dataset: raw_file_loads
table: sales_transactions

# Source file
file_path: /data/sales_transactions_2026_03.csv

# Primary key (composite)
primary_keys:
  - transaction_id
  - transaction_date

# Partition (enabled)
partition:
  enabled: true
  column: transaction_date
  type: DATE

# Random sampling
random_sample_size: 200

# Validation layers — all on except partition and random sampling
validation_layers:
  metadata_validation: true
  row_count_validation: true
  primary_key_uniqueness: true
  aggregate_validation: true
  partition_validation: true
  hash_validation: true
  random_sampling: false        # disabled: composite PK includes DATE column
  column_distribution: true
  null_validation: true
  column_checksum: true

# Aggregate checks
aggregate_columns:
  - column: amount
    functions: [sum, min, max, avg]
  - column: quantity
    functions: [sum, avg]
  - column: customer_email
    functions: [distinct_count]
  - column: transaction_date
    functions: [min, max]

# Distribution checks (numeric only)
distribution_columns:
  - amount
  - quantity

# Null checks — explicit list (leave empty to check all columns)
null_check_columns:
  - transaction_id
  - amount
  - quantity
  - customer_email

# Column checksum — all columns (leave empty to check all)
column_checksum_columns: []
```

---

## Minimal Example

```yaml
dataset: my_dataset
table: my_table
file_path: /path/to/data.csv
```

All validation layers default to `true` but no aggregate, distribution, null-check, or checksum columns are configured, so Layers 4, 8, 9, and 10 produce no sub-results (or check all columns for Layers 9 and 10 when `null_check_columns` and `column_checksum_columns` are omitted). Layers 3 and 7 are silently skipped because `primary_keys` is empty.
