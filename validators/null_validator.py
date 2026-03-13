"""
null_validator.py
-----------------
Validates that null value counts per column match between the source file
and the BigQuery table.

Checks every column listed in ``null_check_columns`` (or **all** file
columns when that list is empty) using:

  File side  — pandas ``None`` / ``NaN`` / empty-string detection.
  BQ side    — ``COUNTIF(column IS NULL)`` for every selected column.

Works for all BigQuery column types including STRING, NUMERIC, DATE,
TIMESTAMP, STRUCT (RECORD), and ARRAY (REPEATED).
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
    Check null counts per column on both file and BigQuery sides.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig (provides dataset, table,
                     null_check_columns).

    Returns:
        List of result dicts — one per column checked.
    """
    # ------------------------------------------------------------------
    # Resolve the list of columns to check
    # ------------------------------------------------------------------
    columns: List[str] = list(getattr(config, "null_check_columns", []))
    if not columns:
        columns = list(file_reader.columns)

    if not columns:
        return [
            _result(
                column="(none)",
                status="SKIPPED",
                file_nulls=None,
                bq_nulls=None,
                details={"reason": "No columns available to check"},
                elapsed_ms=0.0,
            )
        ]

    logger.debug("Null validation — checking %d column(s): %s", len(columns), columns)

    # ------------------------------------------------------------------
    # File-side null counts
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    file_null_counts: Dict[str, int] = file_reader.get_column_null_counts(columns)
    file_elapsed_ms = (time.perf_counter() - t0) * 1000
    logger.debug("File null counts computed in %.1f ms", file_elapsed_ms)

    # ------------------------------------------------------------------
    # BQ-side null counts (single query for all columns)
    # ------------------------------------------------------------------
    t1 = time.perf_counter()
    bq_null_counts: Dict[str, int] = bq_client.get_column_null_counts(
        dataset=config.dataset,
        table=config.table,
        columns=columns,
    )
    bq_elapsed_ms = (time.perf_counter() - t1) * 1000
    logger.debug("BQ null counts computed in %.1f ms", bq_elapsed_ms)

    total_elapsed_ms = file_elapsed_ms + bq_elapsed_ms

    # ------------------------------------------------------------------
    # Build one result dict per column
    # ------------------------------------------------------------------
    results: List[Dict[str, Any]] = []
    for col in columns:
        file_nulls = file_null_counts.get(col, -1)
        bq_nulls = bq_null_counts.get(col, -1)

        if file_nulls == -1:
            status = "WARNING"
            details: Dict[str, Any] = {
                "reason": f"Column '{col}' not found in source file",
            }
        elif bq_nulls == -1:
            status = "WARNING"
            details = {
                "reason": f"Column '{col}' not found in BigQuery result",
            }
        else:
            passed = file_nulls == bq_nulls
            status = "PASS" if passed else "FAIL"
            details = {
                "file_null_count": file_nulls,
                "bq_null_count": bq_nulls,
                "diff": bq_nulls - file_nulls,
            }

        results.append(
            _result(
                column=col,
                status=status,
                file_nulls=file_nulls if file_nulls != -1 else None,
                bq_nulls=bq_nulls if bq_nulls != -1 else None,
                details=details,
                elapsed_ms=round(total_elapsed_ms / len(columns), 2),
            )
        )
        logger.info(
            "  Null check [%s]: file=%s, bq=%s → %s",
            col,
            file_nulls,
            bq_nulls,
            status,
        )

    return results


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _result(
    column: str,
    status: str,
    file_nulls: Any,
    bq_nulls: Any,
    details: Dict[str, Any],
    elapsed_ms: float,
) -> Dict[str, Any]:
    return {
        "test_name": f"null_validation:{column}",
        "status": status,
        "expected": file_nulls,
        "actual": bq_nulls,
        "details": details,
        "execution_time_ms": elapsed_ms,
    }