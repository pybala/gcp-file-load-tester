"""
metadata_writer.py
------------------
Writes validation metadata to the BigQuery tracking tables:

  validation_ds.validation_configs  -- one row per unique config (upsert via MERGE)
  validation_ds.validation_runs     -- one row per validation execution
  validation_ds.validation_tests    -- one row per individual test result

Tables are expected to already exist in BigQuery (DDL supplied separately).

All write operations are best-effort: failures are logged as warnings but
never raise exceptions so they never block or fail a validation run.
"""

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Defaults -- override via env vars or constructor arguments
_DEFAULT_METADATA_PROJECT = "data-test-automation-489413"
_DEFAULT_METADATA_DATASET = "validation_ds"

_TABLE_CONFIGS = "validation_configs"
_TABLE_RUNS = "validation_runs"
_TABLE_TESTS = "validation_tests"


def _now_iso() -> str:
    """Current UTC timestamp as ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _stable_config_id(config_path: Optional[str]) -> str:
    """
    Derive a deterministic UUID-style config_id from the config path.

    Using a stable ID means the same config file always maps to the same
    row in validation_configs, making upsert behaviour predictable.

    If config_path is None (inline dict), a new random UUID is generated.

    Args:
        config_path: File path or GCS URI used to load the config.

    Returns:
        A UUID4-format string (32 hex chars with hyphens).
    """
    if not config_path:
        return str(uuid.uuid4())
    digest = hashlib.sha256(config_path.encode()).hexdigest()
    # Format as UUID (8-4-4-4-12)
    return f"{digest[0:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


class MetadataWriter:
    """
    Writes validation run metadata to the three BigQuery tracking tables.

    Usage::

        writer = MetadataWriter()
        writer.write_all(config, output, config_path="gs://bucket/cfg.yaml")

    Individual methods (write_config, write_run, write_tests) are also
    available if finer control is needed.
    """

    def __init__(
        self,
        metadata_project: Optional[str] = None,
        metadata_dataset: Optional[str] = None,
    ):
        self.metadata_project = (
            metadata_project
            or os.environ.get("METADATA_PROJECT", _DEFAULT_METADATA_PROJECT)
        )
        self.metadata_dataset = (
            metadata_dataset
            or os.environ.get("METADATA_DATASET", _DEFAULT_METADATA_DATASET)
        )
        self._client: Optional[bigquery.Client] = None
        logger.info(
            "MetadataWriter initialised -- project=%s  dataset=%s",
            self.metadata_project,
            self.metadata_dataset,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def write_all(
        self,
        config: Any,
        output: Dict[str, Any],
        *,
        config_path: Optional[str] = None,
        gcs_result_path: Optional[str] = None,
    ) -> None:
        """Convenience wrapper: upsert config, insert run and all test rows."""
        config_id = self.write_config(config, config_path=config_path)
        self.write_run(config, output, config_id=config_id, gcs_result_path=gcs_result_path)
        self.write_tests(config, output)

    def write_config(
        self,
        config: Any,
        *,
        config_path: Optional[str] = None,
        config_name: Optional[str] = None,
    ) -> str:
        """
        Upsert a row in validation_configs using a BigQuery MERGE statement.

        The config_id is derived deterministically from config_path so repeated
        runs against the same config file update the existing row rather than
        creating duplicates.

        Args:
            config:      ValidationConfig instance.
            config_path: File path or GCS URI used to load the config (used to
                         derive a stable config_id).
            config_name: Human-readable name from the UI.  Falls back to
                         config_path, then "dataset.table" if not provided.

        Returns:
            The config_id string used for this config row.
        """
        config_id = _stable_config_id(config_path)
        friendly_name = config_name or config_path or f"{config.dataset}.{config.table}"
        now = _now_iso()

        # Build validation_layers JSON
        vl = config.validation_layers
        if hasattr(vl, "model_dump"):
            layers_dict = vl.model_dump()
        elif hasattr(vl, "__dict__"):
            layers_dict = vars(vl)
        else:
            layers_dict = dict(vl)

        # Build aggregate_columns JSON
        agg_cols = []
        for ac in (config.aggregate_columns or []):
            if hasattr(ac, "model_dump"):
                agg_cols.append(ac.model_dump())
            elif isinstance(ac, dict):
                agg_cols.append(ac)
            else:
                agg_cols.append({"column": str(ac)})

        # Partition fields
        partition = getattr(config, "partition", None)
        if partition is not None:
            partition_enabled = getattr(partition, "enabled", False)
            partition_column = getattr(partition, "column", None)
            partition_type = getattr(partition, "type", None)
        else:
            partition_enabled = getattr(config, "partition_enabled", False)
            partition_column = getattr(config, "partition_column", None)
            partition_type = getattr(config, "partition_type", None)

        config_yaml_text = getattr(config, "config_yaml", None)

        primary_keys_json = json.dumps(list(config.primary_keys or []))
        distribution_cols_json = json.dumps(list(config.distribution_columns or []))
        null_check_cols_json = json.dumps(list(config.null_check_columns or []))
        checksum_cols_json = json.dumps(list(config.column_checksum_columns or []))
        layers_json = json.dumps(layers_dict)
        agg_cols_json = json.dumps(agg_cols)

        table_id = self._table_id(_TABLE_CONFIGS)

        merge_sql = (
            "MERGE `" + table_id + "` T "
            "USING ( "
            "  SELECT "
            "    @config_id AS config_id, "
            "    @config_name AS config_name, "
            "    @project_id AS project_id, "
            "    @dataset AS dataset, "
            "    @table_name AS table_name, "
            "    @file_path AS file_path, "
            "    JSON_VALUE_ARRAY(@primary_keys) AS primary_keys, "
            "    @partition_enabled AS partition_enabled, "
            "    @partition_column AS partition_column, "
            "    @partition_type AS partition_type, "
            "    @random_sample_size AS random_sample_size, "
            "    PARSE_JSON(@validation_layers) AS validation_layers, "
            "    PARSE_JSON(@aggregate_columns) AS aggregate_columns, "
            "    JSON_VALUE_ARRAY(@distribution_columns) AS distribution_columns, "
            "    JSON_VALUE_ARRAY(@null_check_columns) AS null_check_columns, "
            "    JSON_VALUE_ARRAY(@checksum_columns) AS column_checksum_columns, "
            "    @config_yaml AS config_yaml, "
            "    TRUE AS is_active, "
            "    CAST(@now AS TIMESTAMP) AS updated_at "
            ") S "
            "ON T.config_id = S.config_id "
            "WHEN MATCHED THEN "
            "  UPDATE SET "
            "    config_name = S.config_name, "
            "    project_id = S.project_id, "
            "    dataset = S.dataset, "
            "    table_name = S.table_name, "
            "    file_path = S.file_path, "
            "    primary_keys = S.primary_keys, "
            "    partition_enabled = S.partition_enabled, "
            "    partition_column = S.partition_column, "
            "    partition_type = S.partition_type, "
            "    random_sample_size = S.random_sample_size, "
            "    validation_layers = S.validation_layers, "
            "    aggregate_columns = S.aggregate_columns, "
            "    distribution_columns = S.distribution_columns, "
            "    null_check_columns = S.null_check_columns, "
            "    column_checksum_columns = S.column_checksum_columns, "
            "    config_yaml = S.config_yaml, "
            "    is_active = S.is_active, "
            "    updated_at = S.updated_at "
            "WHEN NOT MATCHED THEN "
            "  INSERT ( "
            "    config_id, config_name, project_id, dataset, table_name, "
            "    file_path, primary_keys, partition_enabled, partition_column, "
            "    partition_type, random_sample_size, validation_layers, "
            "    aggregate_columns, distribution_columns, null_check_columns, "
            "    column_checksum_columns, config_yaml, is_active, "
            "    created_at, updated_at "
            "  ) VALUES ( "
            "    S.config_id, S.config_name, S.project_id, S.dataset, S.table_name, "
            "    S.file_path, S.primary_keys, S.partition_enabled, S.partition_column, "
            "    S.partition_type, S.random_sample_size, S.validation_layers, "
            "    S.aggregate_columns, S.distribution_columns, S.null_check_columns, "
            "    S.column_checksum_columns, S.config_yaml, S.is_active, "
            "    CAST(@now AS TIMESTAMP), CAST(@now AS TIMESTAMP) "
            "  )"
        )

        query_params = [
            bigquery.ScalarQueryParameter("config_id", "STRING", config_id),
            bigquery.ScalarQueryParameter("config_name", "STRING", friendly_name),
            bigquery.ScalarQueryParameter(
                "project_id", "STRING", getattr(config, "project", None)
            ),
            bigquery.ScalarQueryParameter("dataset", "STRING", config.dataset),
            bigquery.ScalarQueryParameter("table_name", "STRING", config.table),
            bigquery.ScalarQueryParameter("file_path", "STRING", config.file_path),
            bigquery.ScalarQueryParameter("primary_keys", "STRING", primary_keys_json),
            bigquery.ScalarQueryParameter("partition_enabled", "BOOL", partition_enabled),
            bigquery.ScalarQueryParameter("partition_column", "STRING", partition_column),
            bigquery.ScalarQueryParameter("partition_type", "STRING", partition_type),
            bigquery.ScalarQueryParameter(
                "random_sample_size", "INT64",
                getattr(config, "random_sample_size", None),
            ),
            bigquery.ScalarQueryParameter("validation_layers", "STRING", layers_json),
            bigquery.ScalarQueryParameter("aggregate_columns", "STRING", agg_cols_json),
            bigquery.ScalarQueryParameter(
                "distribution_columns", "STRING", distribution_cols_json
            ),
            bigquery.ScalarQueryParameter(
                "null_check_columns", "STRING", null_check_cols_json
            ),
            bigquery.ScalarQueryParameter("checksum_columns", "STRING", checksum_cols_json),
            bigquery.ScalarQueryParameter("config_yaml", "STRING", config_yaml_text),
            bigquery.ScalarQueryParameter("now", "STRING", now),
        ]

        self._run_dml(merge_sql, query_params)
        logger.info("validation_configs row upserted -- config_id=%s", config_id)
        return config_id

    def write_run(
        self,
        config: Any,
        output: Dict[str, Any],
        *,
        config_id: Optional[str] = None,
        config_name: Optional[str] = None,
        gcs_result_path: Optional[str] = None,
    ) -> None:
        """Insert a row into validation_runs."""
        summary = output.get("summary", {})
        config_path = getattr(config, "config_path", None)
        config_name = config_name or config_path or f"{config.dataset}.{config.table}"

        row = {
            "run_id": output.get("run_id"),
            "config_id": config_id,
            "config_name": config_name,
            "project_id": getattr(config, "project", None),
            "dataset": output.get("dataset") or config.dataset,
            "table_name": output.get("table") or config.table,
            "file_path": output.get("file_path") or config.file_path,
            "gcs_result_path": gcs_result_path,
            "overall_status": output.get("overall_status"),
            "total_tests": summary.get("total", 0),
            "passed_tests": summary.get("passed", 0),
            "failed_tests": summary.get("failed", 0),
            "error_tests": summary.get("errors", 0),
            "warned_tests": summary.get("warned", 0),
            "skipped_tests": summary.get("skipped", 0),
            "total_execution_time_ms": output.get("total_execution_time_ms", 0.0),
            "run_timestamp": output.get("timestamp"),
            "created_at": _now_iso(),
        }

        self._insert_rows(_TABLE_RUNS, [row])
        logger.info(
            "validation_runs row written -- run_id=%s  status=%s",
            row["run_id"],
            row["overall_status"],
        )

    def write_tests(
        self,
        config: Any,
        output: Dict[str, Any],
    ) -> None:
        """Insert one row per individual test result into validation_tests."""
        run_id = output.get("run_id")
        run_timestamp = output.get("timestamp")
        dataset = output.get("dataset") or config.dataset
        table_name = output.get("table") or config.table
        now = _now_iso()

        test_results: List[Dict[str, Any]] = output.get("results", [])
        if not test_results:
            logger.warning(
                "No test results found in output -- validation_tests will be empty"
            )
            return

        rows = []
        for result in test_results:
            details = result.get("details")
            if details is not None and not isinstance(details, str):
                details_json = json.dumps(details, default=str)
            else:
                details_json = details

            expected = result.get("expected")
            actual = result.get("actual")

            rows.append({
                "run_id": run_id,
                "test_name": result.get("test_name"),
                "status": result.get("status"),
                "expected": str(expected) if expected is not None else None,
                "actual": str(actual) if actual is not None else None,
                "execution_time_ms": result.get("execution_time_ms", 0.0),
                "details": details_json,
                "dataset": dataset,
                "table_name": table_name,
                "run_timestamp": run_timestamp,
                "created_at": now,
            })

        # Stream in batches of 500 (BigQuery streaming insert limit per request)
        batch_size = 500
        total_written = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            self._insert_rows(_TABLE_TESTS, batch)
            total_written += len(batch)

        logger.info(
            "validation_tests rows written -- run_id=%s  count=%d",
            run_id,
            total_written,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_client(self) -> bigquery.Client:
        """Lazy-initialise and return the BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(project=self.metadata_project)
        return self._client

    def _table_id(self, table_name: str) -> str:
        """Return the fully-qualified table ID string."""
        return f"{self.metadata_project}.{self.metadata_dataset}.{table_name}"

    def _insert_rows(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        """
        Stream-insert rows into a metadata table.

        Errors are logged as warnings -- they are never re-raised so the
        validation run itself is not affected by metadata write failures.
        """
        if not rows:
            return
        table_id = self._table_id(table_name)
        try:
            client = self._get_client()
            errors = client.insert_rows_json(table_id, rows)
            if errors:
                logger.warning(
                    "BigQuery streaming insert reported errors for %s: %s",
                    table_id,
                    errors,
                )
            else:
                logger.debug(
                    "Successfully inserted %d row(s) into %s", len(rows), table_id
                )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Failed to insert %d row(s) into %s: %s",
                len(rows),
                table_id,
                exc,
                exc_info=True,
            )

    def _run_dml(self, sql: str, query_params: list) -> None:
        """
        Execute a DML statement (MERGE) via a BigQuery query job.

        Used by write_config to perform a true upsert on validation_configs,
        ensuring repeated runs against the same config never create duplicates.
        """
        try:
            client = self._get_client()
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            job = client.query(sql, job_config=job_config)
            job.result()  # Wait for the job to complete
            logger.debug(
                "DML executed successfully: affected_rows=%s",
                job.num_dml_affected_rows,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Failed to execute DML: %s",
                exc,
                exc_info=True,
            )
