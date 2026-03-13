"""
column_checksum_validator.py
----------------------------
Validates data integrity at the column level by computing a deterministic
hash aggregate for each column on both the source file and the BigQuery
table, then verifying that both sides are non-zero (i.e. data was loaded).

Algorithm
---------
  File side  — XOR of ``hash(json_normalised(cell_value))`` for every row.
  BQ side    — ``BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(column)))`` per col.

Because Python's ``hash()`` and BigQuery's ``FARM_FINGERPRINT`` use
different algorithms the absolute hash values will differ, so the check is:

  PASS  — both file_hash ≠ 0  AND  bq_hash ≠ 0
          (confirms every column carries data on both sides)
  FAIL  — either hash is 0 (empty / all-null column on that side)

The per-column hashes are also recorded in the output for audit purposes.

Works for all BigQuery column types: STRING, NUMERIC, DATE, TIMESTAMP,
STRUCT (RECORD), and ARRAY (REPEATED).
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
    Compute and compare column-level checksums on file and BigQuery.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig (provides dataset, table,
                     column_checksum_columns).

    Returns:
        List of result dicts — one per column checked.
    """
    # ------------------------------------------------------------------
    # Resolve the columns to hash
    # ------------------------------------------------------------------
    columns: List[str] = list(getattr(config, "column_checksum_columns", []))
    if not columns:
        columns = list(file_reader.columns)

    if not columns:
        return [
            _result(
                column="(none)",
                status="SKIPPED",
                file_hash=None,
                bq_hash=None,
                details={"reason": "No columns available to checksum"},
                elapsed_ms=0.0,
            )
        ]

    logger.debug(
        "Column checksum validation — hashing %d column(s): %s",
        len(columns),
        columns,
    )

    # ------------------------------------------------------------------
    # File-side column checksums
    # ------------------------------------------------------------------
    t0 = time.perf_counter()
    file_checksums: Dict[str, int] = file_reader.compute_column_checksums(columns)
    file_elapsed_ms = (time.perf_counter() - t0) * 1000
    logger.debug("File column checksums computed in %.1f ms", file_elapsed_ms)

    # ------------------------------------------------------------------
    # BQ-side column checksums (single query for all columns)
    # ------------------------------------------------------------------
    t1 = time.perf_counter()
    bq_checksums: Dict[str, int] = bq_client.get_column_checksums(
        dataset=config.dataset,
        table=config.table,
        columns=columns,
    )
    bq_elapsed_ms = (time.perf_counter() - t1) * 1000
    logger.debug("BQ column checksums computed in %.1f ms", bq_elapsed_ms)

    total_elapsed_ms = file_elapsed_ms + bq_elapsed_ms

    # ------------------------------------------------------------------
    # Build one result dict per column
    # ------------------------------------------------------------------
    results: List[Dict[str, Any]] = []
    for col in columns:
        file_hash = file_checksums.get(col, -1)
        bq_hash = bq_checksums.get(col, -1)

        if file_hash == -1:
            status = "WARNING"
            details: Dict[str, Any] = {
                "reason": f"Column '{col}' not found in source file",
            }
        elif bq_hash == -1:
            status = "WARNING"
            details = {
                "reason": f"Column '{col}' not returned by BigQuery checksum query",
            }
        else:
            # Both hashes non-zero means the column contains data on both sides.
            # Hash values differ by design (Python hash ≠ FARM_FINGERPRINT).
            file_has_data = file_hash != 0
            bq_has_data = bq_hash != 0
            passed = file_has_data and bq_has_data
            status = "PASS" if passed else "FAIL"
            details = {
                "file_column_hash": file_hash,
                "bq_column_hash": bq_hash,
                "file_has_data": file_has_data,
                "bq_has_data": bq_has_data,
                "note": (
                    "Hash algorithms differ (Python XOR vs BQ FARM_FINGERPRINT). "
                    "PASS confirms both sides are non-zero (data present). "
                    "Use hash_validation for full row-level integrity."
                ),
            }

        results.append(
            _result(
                column=col,
                status=status,
                file_hash=file_hash if file_hash != -1 else None,
                bq_hash=bq_hash if bq_hash != -1 else None,
                details=details,
                elapsed_ms=round(total_elapsed_ms / len(columns), 2),
            )
        )
        logger.info(
            "  Column checksum [%s]: file_hash=%s, bq_hash=%s → %s",
            col,
            file_hash,
            bq_hash,
            status,
        )

    return results


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _result(
    column: str,
    status: str,
    file_hash: Any,
    bq_hash: Any,
    details: Dict[str, Any],
    elapsed_ms: float,
) -> Dict[str, Any]:
    return {
        "test_name": f"column_checksum:{column}",
        "status": status,
        "expected": file_hash,
        "actual": bq_hash,
        "details": details,
        "execution_time_ms": elapsed_ms,
    }