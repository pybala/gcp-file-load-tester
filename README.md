# GCP File Load Tester — BigQuery Data Validation Framework

Validates data loaded into BigQuery against source files, running up to 10 validation layers.

---

## Quick Start

```bash
cd repo/gcp-file-load-tester

# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env — set GOOGLE_APPLICATION_CREDENTIALS and other values

# 4. Start the API server
python api_server.py --port 8000
```

---

## BigQuery Metadata Tables Setup (REQUIRED)

The API server writes validation configs and run results to three BigQuery tables.  
**These tables must be created before running the server with BigQuery writes enabled.**

### Option A — BigQuery DDL (recommended for first-time setup)

```bash
# Authenticate first
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/terraform-key.json

# Run DDL
bq query --project_id=data-test-automation-489413 \
         --use_legacy_sql=false \
         < bq_setup.sql
```

### Option B — Terraform

```bash
cd repo/gcp-test-automation/terraform
terraform init
terraform apply
```

The Terraform now creates both the `test_results` dataset AND the `validation_ds` dataset with all three tables.

---

## Authentication

The API server needs GCP credentials to write to BigQuery.

### Using a Service Account Key File

```bash
# Set the env var to point at the key file
export GOOGLE_APPLICATION_CREDENTIALS=/Users/balakumar/Work/apps/myapps/my-git-docker/terraform-key.json

# Or add it to .env:
echo 'GOOGLE_APPLICATION_CREDENTIALS=/Users/balakumar/Work/apps/myapps/my-git-docker/terraform-key.json' >> .env
```

### Skip BigQuery Writes (local dev without GCP)

```bash
python api_server.py --no-metadata
```

This skips all BigQuery writes — validations still run and results are saved locally in `saved_configs/runs_cache.json`.

---

## Starting the API Server

```bash
# Recommended: load .env, then start
cd repo/gcp-file-load-tester
set -a && source .env && set +a
python api_server.py --port 8000
```

Server starts at `http://localhost:8000`.

---

## Debugging Notes

### Issue: Configs and validation results not saved to BigQuery

**Root Causes Found (2026-03-19):**

#### 1. BigQuery tables did not exist
The Terraform only created the `test_results` dataset — it never created the three metadata tables (`validation_configs`, `validation_runs`, `validation_tests`) in the `validation_ds` dataset.

**Fix:** Added table definitions to `repo/gcp-test-automation/terraform/modules/bigquery/main.tf` and created `bq_setup.sql` as a standalone DDL alternative.

#### 2. Dataset name mismatch
`metadata_writer.py` defaults to `METADATA_DATASET=validation_ds`, but Terraform was only creating the `test_results` dataset. These are different datasets.

**Fix:** Terraform now creates `validation_ds` with the three tables. The `metadata_writer.py` default `validation_ds` is correct and unchanged.

#### 3. `run_id` inconsistency
`api_server.py` generated its own short `run_id` (`run-{12hex}`) while `validation_runner.py` independently generated a full UUID as `run_id` inside the result output. The two IDs diverged:
- Local cache stored `run-abc123...`
- BigQuery stored the full UUID from `validation_runner`
- The UI would look up `run-abc123` in BQ and find nothing

**Fix:** `api_server.py` now uses the `run_id` from `validation_runner`'s output as the single source of truth, storing the same ID in both the local cache and BigQuery.

#### 4. `write_config` not called before `write_run` on validation runs
When `/run-validation` was called without a prior `/configs` save, no row existed in `validation_configs` for that config. The `_try_write_run_bq()` helper only called `write_run` and `write_tests`, skipping `write_config`.

**Fix:** `_try_write_run_bq()` now calls `writer.write_config()` first (MERGE upsert — safe to call repeatedly), then `write_run` and `write_tests`.

#### 5. GCP credentials not configured
`GOOGLE_APPLICATION_CREDENTIALS` was not set, causing the BigQuery client to fail. All failures were caught silently (best-effort pattern) — no errors shown to the user.

**Fix:** Document the credential setup (above). Use `--no-metadata` flag during local dev without GCP access.

---

## BigQuery Tables Schema

| Table | Purpose |
|---|---|
| `validation_ds.validation_configs` | One row per unique config (upserted via MERGE) |
| `validation_ds.validation_runs` | One row per validation execution |
| `validation_ds.validation_tests` | One row per individual test result |

See `bq_setup.sql` for full column definitions.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/configs` | List all saved configs |
| GET | `/configs/<id>` | Get a config by ID |
| POST | `/configs` | Save a new config |
| PUT | `/configs/<id>` | Update an existing config |
| DELETE | `/configs/<id>` | Delete a config |
| POST | `/run-validation` | Run validation and get results |
| GET | `/runs` | List all past runs |
| GET | `/runs/<run_id>` | Get a specific run result |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to GCP service account JSON key |
| `GOOGLE_CLOUD_PROJECT` | — | GCP project ID |
| `METADATA_PROJECT` | `data-test-automation-489413` | Project hosting metadata tables |
| `METADATA_DATASET` | `validation_ds` | Dataset hosting metadata tables |
| `LOG_LEVEL` | `INFO` | Logging verbosity |