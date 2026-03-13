"""
row_count_validator.py
----------------------
Validates that the number of rows in the source CSV file matches the
number of rows loaded into the BigQuery table.
"""

import logging
import time
from typing import Any, Dict

from core.bigquery_client import BigQueryClient
from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def validate(
    file_reader: FileReader,
    bq_client: BigQueryClient,
    config: Any,
) -> Dict[str, Any]:
    """
    Compare the row count of the file against the BigQuery table row count.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig with dataset and table attributes.

    Returns:
        Result dict with keys: test_name, status, expected, actual,
        details, execution_time_ms.
    """
    start = time.perf_counter()
    test_name = "row_count_validation"

    try:
        file_count = file_reader.row_count
        logger.info("File row count: %d", file_count)

        bq_count = bq_client.get_row_count(config.dataset, config.table)
        logger.info("BigQuery row count: %d", bq_count)

        status = "PASS" if file_count == bq_count else "FAIL"
        delta = bq_count - file_count
        delta_pct = (abs(delta) / file_count * 100) if file_count > 0 else None

        details: Dict[str, Any] = {
            "file_row_count": file_count,
            "bq_row_count": bq_count,
            "delta": delta,
            "delta_pct": round(delta_pct, 4) if delta_pct is not None else None,
        }

        if status == "PASS":
            logger.info("Row count validation PASSED — %d rows", file_count)
        else:
            logger.warning(
                "Row count mismatch — file=%d, BQ=%d, delta=%d (%.2f%%)",
                file_count,
                bq_count,
                delta,
                delta_pct or 0,
            )

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": file_count,
            "actual": bq_count,
            "details": details,
            "execution_time_ms": round(elapsed_ms, 3),
        }

    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error("Row count validation error: %s", exc, exc_info=True)
        return {
            "test_name": test_name,
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "details": {"error": str(exc)},
            "execution_time_ms": round(elapsed_ms, 3),
        }