"""
primary_key_validator.py
------------------------
Validates primary key integrity across both the file and BigQuery table.

Checks:
  1. File PK uniqueness   — duplicate PK combinations in the CSV.
  2. File PK completeness — null/missing PK values in the CSV.
  3. BQ PK uniqueness     — duplicate PK combinations in BigQuery.

Supports composite primary keys (multiple columns).
"""

import logging
import time
from typing import Any, Dict, List

from core.bigquery_client import BigQueryClient
from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def validate(
    file_reader: FileReader,
    bq_client: BigQueryClient,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Run primary key validation tests against file and BigQuery.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig — uses config.primary_keys,
                     config.dataset, config.table.

    Returns:
        List of result dicts (one per sub-check), each with keys:
        test_name, status, expected, actual, details, execution_time_ms.
    """
    primary_keys: List[str] = config.primary_keys
    results: List[Dict[str, Any]] = []

    if not primary_keys:
        logger.warning("No primary_keys defined — skipping primary key validation.")
        return [
            {
                "test_name": "primary_key_validation",
                "status": "SKIPPED",
                "expected": None,
                "actual": None,
                "details": {"reason": "No primary_keys defined in config"},
                "execution_time_ms": 0.0,
            }
        ]

    # ------------------------------------------------------------------
    # Sub-check 1: File PK uniqueness
    # ------------------------------------------------------------------
    results.append(_check_file_pk_uniqueness(file_reader, primary_keys))

    # ------------------------------------------------------------------
    # Sub-check 2: File PK completeness (no nulls)
    # ------------------------------------------------------------------
    results.append(_check_file_pk_completeness(file_reader, primary_keys))

    # ------------------------------------------------------------------
    # Sub-check 3: BigQuery PK uniqueness
    # ------------------------------------------------------------------
    results.append(_check_bq_pk_uniqueness(bq_client, config, primary_keys))

    return results


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _check_file_pk_uniqueness(
    file_reader: FileReader,
    primary_keys: List[str],
) -> Dict[str, Any]:
    """Check that the CSV has no duplicate primary key combinations."""
    start = time.perf_counter()
    test_name = "primary_key_uniqueness_file"
    try:
        dup_count = file_reader.get_duplicate_pk_count(primary_keys)
        status = "PASS" if dup_count == 0 else "FAIL"

        if status == "PASS":
            logger.info("File PK uniqueness PASSED — no duplicates found")
        else:
            logger.warning("File PK uniqueness FAILED — %d duplicate rows found", dup_count)

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": 0,
            "actual": dup_count,
            "details": {
                "primary_keys": primary_keys,
                "duplicate_rows": dup_count,
            },
            "execution_time_ms": round(elapsed_ms, 3),
        }
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error("%s error: %s", test_name, exc, exc_info=True)
        return {
            "test_name": test_name,
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "details": {"error": str(exc)},
            "execution_time_ms": round(elapsed_ms, 3),
        }


def _check_file_pk_completeness(
    file_reader: FileReader,
    primary_keys: List[str],
) -> Dict[str, Any]:
    """Check that no primary key column contains null values in the CSV."""
    start = time.perf_counter()
    test_name = "primary_key_null_check_file"
    try:
        null_count = file_reader.get_null_pk_count(primary_keys)
        status = "PASS" if null_count == 0 else "FAIL"

        if status == "PASS":
            logger.info("File PK null check PASSED — no null PK values")
        else:
            logger.warning("File PK null check FAILED — %d rows with null PK", null_count)

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": 0,
            "actual": null_count,
            "details": {
                "primary_keys": primary_keys,
                "null_pk_rows": null_count,
            },
            "execution_time_ms": round(elapsed_ms, 3),
        }
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error("%s error: %s", test_name, exc, exc_info=True)
        return {
            "test_name": test_name,
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "details": {"error": str(exc)},
            "execution_time_ms": round(elapsed_ms, 3),
        }


def _check_bq_pk_uniqueness(
    bq_client: BigQueryClient,
    config: Any,
    primary_keys: List[str],
) -> Dict[str, Any]:
    """Check that BigQuery has no duplicate primary key combinations."""
    start = time.perf_counter()
    test_name = "primary_key_uniqueness_bq"
    try:
        dup_count = bq_client.get_duplicate_pk_count(
            config.dataset, config.table, primary_keys
        )
        status = "PASS" if dup_count == 0 else "FAIL"

        if status == "PASS":
            logger.info("BQ PK uniqueness PASSED — no duplicates found")
        else:
            logger.warning("BQ PK uniqueness FAILED — %d duplicate PK groups", dup_count)

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": 0,
            "actual": dup_count,
            "details": {
                "primary_keys": primary_keys,
                "duplicate_pk_groups_in_bq": dup_count,
            },
            "execution_time_ms": round(elapsed_ms, 3),
        }
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error("%s error: %s", test_name, exc, exc_info=True)
        return {
            "test_name": test_name,
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "details": {"error": str(exc)},
            "execution_time_ms": round(elapsed_ms, 3),
        }