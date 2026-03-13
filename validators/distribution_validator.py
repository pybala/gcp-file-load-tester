"""
distribution_validator.py
-------------------------
Computes and compares column distribution statistics between the source CSV
file and the BigQuery table.

Statistics computed per column:
  - min_val     — minimum value
  - max_val     — maximum value
  - avg_val     — mean value
  - stddev_val  — standard deviation
  - null_count  — number of null/NaN values
  - total_count — total row count

If distribution_columns is not specified in the config, the validator
auto-detects all numeric columns in the file.
"""

import logging
import math
import time
from typing import Any, Dict, List, Optional

from core.bigquery_client import BigQueryClient
from core.file_reader import FileReader

logger = logging.getLogger(__name__)

# Relative tolerance for numeric stat comparisons (1%)
_FLOAT_TOLERANCE = 0.01


def validate(
    file_reader: FileReader,
    bq_client: BigQueryClient,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Compute and compare column distribution statistics for configured columns.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig — uses config.distribution_columns,
                     config.dataset, config.table.

    Returns:
        List of result dicts, one per column.
    """
    results: List[Dict[str, Any]] = []

    # Determine which columns to analyse
    columns = _resolve_columns(file_reader, config)

    if not columns:
        logger.info("No distribution_columns to validate.")
        return [
            {
                "test_name": "column_distribution_validation",
                "status": "SKIPPED",
                "expected": None,
                "actual": None,
                "details": {"reason": "No numeric columns found for distribution analysis"},
                "execution_time_ms": 0.0,
            }
        ]

    for column in columns:
        logger.info("Computing distribution stats for column '%s'", column)
        results.append(_validate_column(file_reader, bq_client, config, column))

    return results


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _resolve_columns(file_reader: FileReader, config: Any) -> List[str]:
    """
    Return the list of columns to analyse.

    If config.distribution_columns is non-empty, use it directly.
    Otherwise auto-detect numeric columns from the file.

    Args:
        file_reader: FileReader instance.
        config:      ValidationConfig.

    Returns:
        List of column names.
    """
    if config.distribution_columns:
        # Validate that specified columns exist in the file
        valid = []
        for col in config.distribution_columns:
            if col in file_reader.columns:
                valid.append(col)
            else:
                logger.warning(
                    "distribution_columns: '%s' not found in file — skipping.", col
                )
        return valid

    # Auto-detect numeric columns
    import pandas as pd
    numeric_cols = file_reader.dataframe.select_dtypes(include=["number"]).columns.tolist()
    logger.info(
        "Auto-detected %d numeric columns for distribution analysis: %s",
        len(numeric_cols),
        numeric_cols,
    )
    return numeric_cols


def _validate_column(
    file_reader: FileReader,
    bq_client: BigQueryClient,
    config: Any,
    column: str,
) -> Dict[str, Any]:
    """
    Compare distribution statistics for a single column.

    Args:
        file_reader: FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig.
        column:      Column name.

    Returns:
        Result dict.
    """
    start = time.perf_counter()
    test_name = f"column_distribution_{column}"

    try:
        # File-side statistics
        file_stats = file_reader.compute_column_distribution(column)

        # BQ-side statistics
        bq_stats = bq_client.get_column_distribution(
            config.dataset, config.table, column
        )

        # Compare each statistic
        stat_results: Dict[str, Any] = {}
        all_pass = True

        for stat_key in ("min_val", "max_val", "avg_val", "stddev_val", "null_count"):
            file_val = file_stats.get(stat_key)
            bq_val = bq_stats.get(stat_key)
            match = _stat_matches(file_val, bq_val, stat_key)
            if not match:
                all_pass = False
            stat_results[stat_key] = {
                "file": _safe_float(file_val),
                "bq": _safe_float(bq_val),
                "match": match,
            }

        # null_count is an exact integer check
        file_null = file_stats.get("null_count", 0)
        bq_null = bq_stats.get("null_count", 0)
        null_match = int(file_null or 0) == int(bq_null or 0)
        if not null_match:
            all_pass = False
        stat_results["null_count"] = {
            "file": int(file_null or 0),
            "bq": int(bq_null or 0),
            "match": null_match,
        }

        status = "PASS" if all_pass else "FAIL"

        details: Dict[str, Any] = {
            "column": column,
            "statistics": stat_results,
            "file_total_count": file_stats.get("total_count"),
            "bq_total_count": bq_stats.get("total_count"),
        }

        if status == "PASS":
            logger.info("Distribution PASSED for column '%s'", column)
        else:
            failing = [k for k, v in stat_results.items() if not v.get("match")]
            logger.warning(
                "Distribution FAILED for column '%s' — mismatched stats: %s",
                column, failing,
            )

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": {k: v["file"] for k, v in stat_results.items()},
            "actual": {k: v["bq"] for k, v in stat_results.items()},
            "details": details,
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


def _stat_matches(
    file_val: Optional[float],
    bq_val: Optional[float],
    stat_key: str,
) -> bool:
    """
    Compare two statistic values with appropriate tolerance.

    null_count is compared exactly (integer).
    All others use relative floating-point tolerance.

    Args:
        file_val: File-side statistic value.
        bq_val:   BQ-side statistic value.
        stat_key: Name of the statistic (e.g. 'min_val', 'null_count').

    Returns:
        True if values are considered equal.
    """
    if file_val is None and bq_val is None:
        return True
    if file_val is None or bq_val is None:
        return False

    if stat_key == "null_count":
        return int(file_val) == int(bq_val)

    try:
        f = float(file_val)
        b = float(bq_val)
        if math.isnan(f) and math.isnan(b):
            return True
        if math.isnan(f) or math.isnan(b):
            return False
        if f == 0 and b == 0:
            return True
        if f == 0:
            return abs(b) < 1e-9
        return abs(f - b) / abs(f) <= _FLOAT_TOLERANCE
    except (TypeError, ValueError):
        return str(file_val) == str(bq_val)


def _safe_float(value: Any) -> Optional[float]:
    """Convert a value to float, returning None for null/NaN."""
    if value is None:
        return None
    try:
        f = float(value)
        return None if math.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None