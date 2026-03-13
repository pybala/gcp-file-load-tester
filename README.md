# BigQuery Data Validation Framework

A lightweight, configuration-driven framework for validating **CSV and JSONL** file data against Google BigQuery tables — including tables with nested `STRUCT` and repeated `ARRAY` columns. Runs as a **local CLI tool** or as a **Google Cloud Function** (HTTP trigger).

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Validation Layers](#validation-layers)
- [CLI Usage](#cli-usage)
- [Cloud Function Usage](#cloud-function-usage)
- [Output Format](#output-format)
- [Exit Codes](#exit-codes)
- [Environment Variables](#environment-variables)
- [Known Limitations](#known-limitations)

---

## Overview

The framework compares a source CSV or JSONL file against a BigQuery table across up to **10 independent validation layers**:

| # | Layer | What it checks |
|---|-------|---------------|
| 1 | Schema (Metadata) | Column names match between file and BQ |
| 2 | Row Count | File row count equals BQ row count |
| 3 | Primary Key Uniqueness | No duplicate or null PKs in file or BQ |
| 4 | Aggregate Validation | Per-column sum / min / max / avg / distinct_count |
| 5 | Partition Validation | Per-partition row counts match |
| 6 | Hash Validation | Aggregate hash confirms data completeness |
| 7 | Random Sampling | Field-level comparison of N sampled rows (STRUCT/ARRAY aware) |
| 8 | Column Distribution | min / max / avg / stddev / null_count per column |
| 9 | Null Validation | Null count per column matches between file and BQ |
| 10 | Column Checksum | Per-column hash confirms data loaded into every column |

Each layer can be independently **enabled or disabled** via the YAML config. No code changes required to add a new test — only a new YAML config file.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      main.py                            │
│  ┌──────────────┐          ┌─────────────────────────┐  │
│  │  CLI Mode    │          │  Cloud Function Mode    │  │
│  │  (argparse)  │          │  (bq_validate / Flask)  │  │
│  └──────┬───────┘          └────────────┬────────────┘  │
│         └──────────────┬────────────────┘               │
│                        ▼                                 │
│              core/config_loader.py                       │
│                 (Pydantic v2 schema)                     │
│                        │                                 │
│                        ▼                                 │
│           engine/validation_runner.py                    │
│           (orchestrates all 10 layers)                   │
│                        │                                 │
│           ┌────────────┼────────────┐                    │
│           ▼            ▼            ▼                    │
│    core/file_reader  validators/  core/bigquery_client   │
│                        │                                 │
│                        ▼                                 │
│           engine/result_formatter.py                     │
│              (JSON output builder)                       │
└─────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
gcp-file-load-tester/
├── main.py                          # CLI entry point + Cloud Function handler
├── Makefile                         # Developer shortcuts
├── requirements.txt                 # Python dependencies
├── setup_venv.sh                    # One-shot venv setup script
├── .env                             # Local environment variables (not committed)
├── .env.example                     # Template for .env
│
├── core/
│   ├── config_loader.py             # YAML → Pydantic ValidationConfig
│   ├── bigquery_client.py           # All BigQuery query helpers
│   └── file_reader.py               # CSV / JSONL loading + aggregation helpers
│
├── engine/
│   ├── validation_runner.py         # Orchestrates all 10 validation layers
│   └── result_formatter.py          # Builds the final JSON output
│
├── validators/
│   ├── schema_validator.py          # Layer 1: column name comparison
│   ├── row_count_validator.py       # Layer 2: total row count
│   ├── primary_key_validator.py     # Layer 3: PK uniqueness / nulls
│   ├── aggregate_validator.py       # Layer 4: sum/min/max/avg/distinct_count
│   ├── partition_validator.py       # Layer 5: per-partition row counts
│   ├── hash_validator.py            # Layer 6: aggregate hash comparison
│   ├── random_sample_validator.py   # Layer 7: field-level row sampling (STRUCT/ARRAY aware)
│   ├── distribution_validator.py    # Layer 8: min/max/avg/stddev/null_count
│   ├── null_validator.py            # Layer 9: null count comparison per column
│   └── column_checksum_validator.py # Layer 10: per-column hash presence check
│
├── config/
│   ├── validation_config.yaml           # Default working config
│   └── validation_config_reference.yaml # Full reference config with all options
│
└── tests/
    ├── test1/
    │   ├── bq_generic_test_file.csv         # Flat CSV test data (13 columns)
    │   ├── validation_config.yaml           # Config for test1 (generic_file_load_test)
    │   └── results.json                     # Last run results
    ├── test2_nested/
    │   ├── bq_json_test_file.jsonl          # JSONL test data with STRUCT + ARRAY columns
    │   ├── validation_config.yaml           # Config for test2 (orders_with_nested) — all 10 layers
    │   └── results.json                     # Last run results
    └── test3_complex_csv/
        ├── bq-customer-transactions.csv     # Complex CSV: 8 rows, 17 columns (INT64, STRING,
        │                                    #   NUMERIC, FLOAT64, BOOL, DATE, TIMESTAMP, JSON)
        ├── validation_config.yaml           # Config for test3 — all 10 layers incl. partition
        └── results.json                     # Last run results (78/78 PASS)
```

---

## Quick Start

### 1. Set up the virtual environment

```bash
cd repo/gcp-file-load-tester
make setup
# or manually:
bash setup_venv.sh
```

### 2. Configure GCP credentials

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

Or set `GOOGLE_APPLICATION_CREDENTIALS` in your `.env` file.

### 3. Create a validation config

Copy the reference config and fill in your values:

```bash
cp config/validation_config_reference.yaml config/my_validation.yaml
# Edit my_validation.yaml with your project, dataset, table, file_path, etc.
```

### 4. Run a validation

```bash
# Using make
make run CONFIG=config/my_validation.yaml

# Using Python directly (bypasses .env override)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json \
  .venv/bin/python main.py --config config/my_validation.yaml

# Save results to a JSON file
.venv/bin/python main.py \
  --config config/my_validation.yaml \
  --output results/my_test_results.json
```

---

## Configuration

All test behaviour is controlled by a single YAML configuration file. See [`config/validation_config_reference.yaml`](config/validation_config_reference.yaml) for the full annotated reference.

### Minimal config

```yaml
dataset: my_dataset
table: my_table
file_path: /path/to/data.csv     # or /path/to/data.jsonl — format detected automatically
```

### Full config skeleton

```yaml
project: my-gcp-project-id
dataset: my_dataset
table: my_table
file_path: /path/to/data.csv

primary_keys:
  - id

partition:
  enabled: false
  column: event_date
  type: DATE

random_sample_size: 100

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

aggregate_columns:
  - column: amount
    functions: [sum, min, max, avg]
  - column: email
    functions: [distinct_count]

distribution_columns:
  - amount
  - quantity

# Null checks — leave empty to check ALL columns
null_check_columns:
  - id
  - amount
  - email

# Column checksum — leave empty to checksum ALL columns
column_checksum_columns: []
```

For the complete reference with all fields, types, defaults, and examples see **[CONFIGURATION.md](CONFIGURATION.md)**.

---

## Validation Layers

For detailed documentation on each validation layer including what it checks, expected output, and common failure modes, see **[VALIDATORS.md](VALIDATORS.md)**.

---

## CLI Usage

```
usage: bq-data-validator [-h] --config PATH [--output PATH]
                         [--log-level {DEBUG,INFO,WARNING,ERROR}]
                         [--indent INDENT]

Options:
  --config PATH         Local path or GCS URI to the YAML validation config.
  --output PATH         Optional path to write the JSON results file.
  --log-level LEVEL     Logging verbosity: DEBUG | INFO | WARNING | ERROR
                        (default: INFO)
  --indent N            JSON output indentation spaces (default: 2)
```

### Examples

**Without Makefile — source `.env` then call Python directly:**

```bash
# Run test3 — results to stdout
cd repo/gcp-file-load-tester && \
  set -a && source .env && set +a && \
  .venv/bin/python main.py --config tests/test3_complex_csv/validation_config.yaml

# Run test3 — save results to JSON file
cd repo/gcp-file-load-tester && \
  set -a && source .env && set +a && \
  .venv/bin/python main.py \
    --config tests/test3_complex_csv/validation_config.yaml \
    --output tests/test3_complex_csv/results.json

# Run any test suite — swap only the --config path
.venv/bin/python main.py --config tests/test1/validation_config.yaml            --output tests/test1/results.json
.venv/bin/python main.py --config tests/test2_nested/validation_config.yaml     --output tests/test2_nested/results.json
.venv/bin/python main.py --config tests/test3_complex_csv/validation_config.yaml --output tests/test3_complex_csv/results.json

# Verbose logging
cd repo/gcp-file-load-tester && \
  set -a && source .env && set +a && \
  .venv/bin/python main.py \
    --config tests/test3_complex_csv/validation_config.yaml \
    --output tests/test3_complex_csv/results.json \
    --log-level DEBUG

# If credentials are already exported in your shell, omit the source step
cd repo/gcp-file-load-tester && \
  .venv/bin/python main.py --config tests/test3_complex_csv/validation_config.yaml
```

> `set -a && source .env && set +a` exports all variables from `.env` into the shell
> (sets `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_CLOUD_PROJECT`, `LOG_LEVEL`) —
> equivalent to what the Makefile's `-include .env` + `export` does.

**Using make (respects `.env` automatically):**

```bash
make run CONFIG=config/validation_config.yaml
make run CONFIG=tests/test3_complex_csv/validation_config.yaml
make run CONFIG=tests/test3_complex_csv/validation_config.yaml OUTPUT=tests/test3_complex_csv/results.json
```

---

## Cloud Function Usage

### Deploy

```bash
make deploy GCP_PROJECT=my-gcp-project REGION=us-central1
```

Or manually:

```bash
gcloud functions deploy bq-data-validator \
  --gen2 \
  --runtime python311 \
  --region us-central1 \
  --source . \
  --entry-point bq_validate \
  --trigger-http \
  --allow-unauthenticated \
  --memory 512MB \
  --timeout 540s
```

### Invoke

**Using a GCS config path:**
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"config_path": "gs://my-bucket/configs/validation_config.yaml"}' \
  https://<FUNCTION_URL>
```

**Using an inline config dict:**
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "dataset": "my_dataset",
      "table": "my_table",
      "file_path": "gs://my-bucket/data/myfile.csv",
      "validation_layers": {
        "metadata_validation": true,
        "row_count_validation": true
      }
    }
  }' \
  https://<FUNCTION_URL>
```

### Local dev server

```bash
make serve          # Starts functions-framework on port 8080
make invoke         # POSTs a test request to localhost:8080
```

---

## Output Format

All results are returned as a JSON document (stdout for CLI, HTTP body for Cloud Function).

```json
{
  "run_id": "uuid-v4",
  "dataset": "my_dataset",
  "table": "my_table",
  "file_path": "/path/to/data.csv",
  "timestamp": "2026-03-08T12:00:00+00:00",
  "overall_status": "PASS",
  "total_execution_time_ms": 3500.0,
  "summary": {
    "total": 16,
    "passed": 16,
    "failed": 0,
    "errors": 0,
    "warned": 0,
    "skipped": 0
  },
  "results": [
    {
      "test_name": "schema_validation",
      "status": "PASS",
      "expected": ["col_a", "col_b"],
      "actual": ["col_a", "col_b"],
      "details": { "...": "..." },
      "execution_time_ms": 0.2
    }
  ]
}
```

### `overall_status` values

| Status | Meaning |
|--------|---------|
| `PASS` | All enabled checks passed |
| `FAIL` | One or more checks returned FAIL |
| `WARN` | One or more checks returned WARN, none FAIL |
| `ERROR` | One or more checks threw an exception |
| `SKIPPED` | All checks were skipped |

### Individual test `status` values

| Status | Meaning |
|--------|---------|
| `PASS` | Check passed — expected equals actual |
| `FAIL` | Check failed — mismatch detected |
| `WARN` | Within tolerance but noteworthy |
| `ERROR` | Exception occurred during the check |
| `SKIPPED` | Layer disabled in config |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Run completed — overall_status is PASS, WARN, or SKIPPED |
| `1` | Validation failures — overall_status is FAIL or ERROR |
| `2` | Framework error — bad config, file not found, BQ auth error |

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes (local) | Path to GCP service account JSON key file. Not needed on GCP infrastructure. |
| `GOOGLE_CLOUD_PROJECT` | No | GCP project ID fallback. Overridden by `project` in YAML config. |
| `LOG_LEVEL` | No | Logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default: `INFO`. |
| `FUNCTION_PORT` | No | Port for local Cloud Function dev server (`make serve`). Default: `8080`. |
| `FUNCTION_TARGET` | No | Cloud Function entry point name. Default: `bq_validate`. |

---

## Known Limitations

| Issue | Affected Layer | Notes |
|-------|---------------|-------|
| Composite PK type casting | Layer 7 (Random Sampling) | When a composite primary key includes a TIMESTAMP or non-STRING column, the `IN` clause query may fail with a type mismatch error. Workaround: disable `random_sampling` for tables with TIMESTAMP PKs. |
| String columns in distribution | Layer 8 (Distribution) | Only NUMERIC columns (INTEGER, FLOAT, NUMERIC) should be listed under `distribution_columns`. STRING or BOOLEAN columns will cause a BigQuery query error. |
| Hash algorithm difference | Layers 6 & 10 | File side uses Python's `hash()` XOR; BQ side uses `FARM_FINGERPRINT`. The two values will never be equal — both checks only confirm both sides are non-zero (data present). |
| XOR collision on BOOL / low-cardinality columns | Layer 10 (Column Checksum) | A column where every distinct value appears an even number of times produces a hash of 0 on both sides, which the validator treats as "no data" → false FAIL. Workaround: exclude such columns from `column_checksum_columns` and validate them via Layers 4 (`distinct_count`), 7 (field-level sampling), and 9 (null count) instead. |
| NUMERIC min/max returned as string by BQ | Layer 4 (Aggregate) | BigQuery returns `MIN`/`MAX` of `NUMERIC` columns as strings (e.g. `"4"` not `4.0`). The validator normalises both sides to `float` before comparison — PASS is still reported correctly even when `expected` and `actual` look different in the JSON output. |
| TIMESTAMP min/max format difference | Layer 4 (Aggregate) | BigQuery returns `MIN`/`MAX` of `TIMESTAMP` columns in ISO-8601 format with `T` separator and `+00:00` suffix. The validator strips the suffix and replaces `T` with a space before comparing — PASS is still reported correctly. |
| CSV JSON column as string | Layer 7 (Random Sampling) | A `JSON`-typed BQ column sourced from a CSV stores the payload as a string in the file but BQ returns it as a parsed dict. The validator automatically calls `json.loads()` on string values before dict comparison. |
| File format | All | Supported formats: comma-delimited CSV (`.csv`) and newline-delimited JSON (`.jsonl`). File format is detected automatically from the file extension. |
| STRUCT / ARRAY in aggregates | Layer 4 (Aggregate) | `aggregate_columns` must only list scalar columns. STRUCT (`RECORD`) and ARRAY (`REPEATED`) columns cannot be aggregated with `sum`/`avg`/`min`/`max` — use `distinct_count` with caution. |
| STRUCT / ARRAY in distribution | Layer 8 (Distribution) | `distribution_columns` must only list scalar NUMERIC columns. STRUCT and ARRAY columns will cause a BigQuery query error. |
| STRUCT sub-fields in null check | Layer 9 (Null Validation) | `null_check_columns` must reference top-level column names only (e.g. `customer_info`, not `customer_info.name`). |
| Column checksum not bit-exact | Layer 10 (Column Checksum) | Hash algorithms differ between file and BQ sides. The check confirms data presence (non-zero), not value equality. Use Layer 7 (Random Sampling) for field-level comparison. |
| BQ table must pre-exist | All | The framework validates data already loaded into BigQuery; it does not load data itself. |
