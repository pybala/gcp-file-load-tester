"""
schema_validator.py
-------------------
Validates the schema (column names) of the source CSV file against the
BigQuery table schema.

Checks:
  - Missing columns: columns in BQ but absent from the file.
  - Extra columns:   columns in the file but absent from BQ.
  - Column type mismatches are reported as informational details.
"""

import logging
import time
from typing import Any, Dict, List

from google.cloud.bigquery import SchemaField

from core.file_reader import FileReader

logger = logging.getLogger(__name__)

# BigQuery type → broad category mapping for loose type comparison
_BQ_TYPE_CATEGORY = {
    "STRING": "string",
    "BYTES": "string",
    "INTEGER": "numeric",
    "INT64": "numeric",
    "FLOAT": "numeric",
    "FLOAT64": "numeric",
    "NUMERIC": "numeric",
    "BIGNUMERIC": "numeric",
    "BOOLEAN": "boolean",
    "BOOL": "boolean",
    "TIMESTAMP": "datetime",
    "DATE": "datetime",
    "TIME": "datetime",
    "DATETIME": "datetime",
    "RECORD": "struct",
    "STRUCT": "struct",
    "GEOGRAPHY": "other",
    "JSON": "other",
}


def validate(
    file_reader: FileReader,
    bq_schema: List[SchemaField],
    config: Any,
) -> Dict[str, Any]:
    """
    Compare file columns against the BigQuery table schema.

    Args:
        file_reader: Loaded FileReader instance.
        bq_schema:   List of BigQuery SchemaField objects.
        config:      ValidationConfig (unused here, kept for interface uniformity).

    Returns:
        Result dict with keys: test_name, status, expected, actual,
        details, execution_time_ms.
    """
    start = time.perf_counter()
    test_name = "schema_validation"

    try:
        file_cols = set(file_reader.columns)
        bq_cols = {field.name for field in bq_schema}

        missing_from_file = sorted(bq_cols - file_cols)      # In BQ, not in file
        extra_in_file = sorted(file_cols - bq_cols)          # In file, not in BQ
        common_cols = sorted(file_cols & bq_cols)

        # Build a type comparison for common columns
        bq_type_map = {field.name: field.field_type for field in bq_schema}
        type_details: List[str] = []
        for col in common_cols:
            bq_type = bq_type_map.get(col, "UNKNOWN").upper()
            type_details.append(f"{col}: BQ={bq_type}")

        status = "PASS" if not missing_from_file and not extra_in_file else "FAIL"

        details: Dict[str, Any] = {
            "common_columns_count": len(common_cols),
            "missing_from_file": missing_from_file,
            "extra_in_file": extra_in_file,
            "bq_column_types": type_details,
        }

        if missing_from_file:
            logger.warning(
                "Schema mismatch — columns in BQ but missing from file: %s",
                missing_from_file,
            )
        if extra_in_file:
            logger.warning(
                "Schema mismatch — columns in file but not in BQ: %s",
                extra_in_file,
            )
        if status == "PASS":
            logger.info("Schema validation PASSED — %d columns matched", len(common_cols))

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": sorted(bq_cols),
            "actual": sorted(file_cols),
            "details": details,
            "execution_time_ms": round(elapsed_ms, 3),
        }

    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error("Schema validation error: %s", exc, exc_info=True)
        return {
            "test_name": test_name,
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "details": {"error": str(exc)},
            "execution_time_ms": round(elapsed_ms, 3),
        }