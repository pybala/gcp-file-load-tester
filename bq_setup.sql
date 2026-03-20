-- =============================================================================
-- bq_setup.sql
-- ------------
-- DDL to create the three validation metadata tables in BigQuery.
--
-- Run with bq CLI:
--   bq query --project_id=data-test-automation-489413 \
--            --use_legacy_sql=false \
--            < bq_setup.sql
--
-- Or run each statement individually in the BigQuery console.
-- =============================================================================

-- Create the metadata dataset (idempotent)
CREATE SCHEMA IF NOT EXISTS `data-test-automation-489413.validation_ds`
  OPTIONS (location = 'US');

-- =============================================================================
-- validation_configs
-- One row per unique validation config (upserted via MERGE on every save/run).
-- =============================================================================
CREATE TABLE IF NOT EXISTS `data-test-automation-489413.validation_ds.validation_configs` (
  config_id               STRING    NOT NULL,  -- SHA-256 of config_path or random UUID
  config_name             STRING,              -- human-readable name / file path
  project_id              STRING,              -- GCP project of the target BQ table
  dataset                 STRING    NOT NULL,  -- target BigQuery dataset
  table_name              STRING    NOT NULL,  -- target BigQuery table
  file_path               STRING,              -- source file path or GCS URI
  primary_keys            ARRAY<STRING>,       -- list of PK column names
  partition_enabled       BOOL,
  partition_column        STRING,
  partition_type          STRING,              -- DATE | TIMESTAMP
  random_sample_size      INT64,
  validation_layers       JSON,                -- {metadata_validation: true, ...}
  aggregate_columns       JSON,                -- [{column, functions}, ...]
  distribution_columns    ARRAY<STRING>,
  null_check_columns      ARRAY<STRING>,
  column_checksum_columns ARRAY<STRING>,
  config_yaml             STRING,              -- raw YAML text
  is_active               BOOL,
  created_at              TIMESTAMP,
  updated_at              TIMESTAMP
);

-- =============================================================================
-- validation_runs
-- One row per execution of a validation config.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `data-test-automation-489413.validation_ds.validation_runs` (
  run_id                  STRING    NOT NULL,  -- UUID from validation_runner
  config_id               STRING,              -- FK → validation_configs.config_id
  config_name             STRING,
  project_id              STRING,
  dataset                 STRING,
  table_name              STRING,
  file_path               STRING,
  gcs_result_path         STRING,              -- optional GCS URI of full result JSON
  overall_status          STRING,              -- PASS | FAIL | ERROR
  total_tests             INT64,
  passed_tests            INT64,
  failed_tests            INT64,
  error_tests             INT64,
  warned_tests            INT64,
  skipped_tests           INT64,
  total_execution_time_ms FLOAT64,
  run_timestamp           TIMESTAMP,           -- when the run started
  created_at              TIMESTAMP            -- when this row was written
);

-- =============================================================================
-- validation_tests
-- One row per individual test result within a run.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `data-test-automation-489413.validation_ds.validation_tests` (
  run_id            STRING,        -- FK → validation_runs.run_id
  test_name         STRING,        -- e.g. row_count, schema_match, pk_uniqueness
  status            STRING,        -- PASS | FAIL | ERROR | WARN | SKIPPED
  expected          STRING,        -- expected value (stringified)
  actual            STRING,        -- actual value (stringified)
  execution_time_ms FLOAT64,
  details           STRING,        -- JSON blob with extended details
  dataset           STRING,
  table_name        STRING,
  run_timestamp     TIMESTAMP,
  created_at        TIMESTAMP
);