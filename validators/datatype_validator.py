"""
datatype_validator.py
---------------------
Validates that each specified column in the source file contains only values
that conform to the declared data type.

File-side only — no BigQuery query is required.

Supported expected_type values:
  integer   — whole numbers (int or integer-parsable string)
  float     — any numeric value (int or float)
  string    — any non-null value is accepted (always passes)
  boolean   — true/false/1/0/yes/no (case-insensitive)
  date      — YYYY-MM-DD format
  timestamp — YYYY-MM-DD HH:MM:SS or ISO-8601
"""

import logging
import re
import time
from datetime import date, datetime
from typing import Any, Dict, List

import pandas as pd

from core.file_reader import FileReader

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Type-check helpers
# ------------------------------------------------------------------

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?")
_BOOL_VALID = {"true", "false", "1", "0", "yes", "no", "t", "f", "y", "n"}


def _is_integer(v) -> bool:
    if isinstance(v, bool):
        return False
    if isinstance(v, int):
        return True
    try:
        int(str(v).strip())
        return True
    except (ValueError, TypeError):
        return False


def _is_float(v) -> bool:
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return True
    try:
        float(str(v).strip())
        return True
    except (ValueError, TypeError):
        return False


def _is_boolean(v) -> bool:
    if isinstance(v, bool):
        return True
    return str(v).strip().lower() in _BOOL_VALID


def _is_date(v) -> bool:
    if isinstance(v, date) and not isinstance(v, datetime):
        return True
    return bool(_DATE_RE.match(str(v).strip()))


def _is_timestamp(v) -> bool:
    if isinstance(v, datetime):
        return True
    return bool(_TS_RE.match(str(v).strip()))


_TYPE_CHECKERS = {
    "integer": _is_integer,
    "float": _is_float,
    "string": lambda v: True,
    "boolean": _is_boolean,
    "date": _is_date,
    "timestamp": _is_timestamp,
}


# ------------------------------------------------------------------
# Public validator entry point
# ------------------------------------------------------------------

def validate(
    file_reader: FileReader,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Validate data types for each column listed in ``datatype_columns``.

    Args:
        file_reader: Loaded FileReader instance.
        config:      ValidationConfig with ``datatype_columns`` list.

    Returns:
        List of result dicts — one per column checked.
    """
    datatype_columns = list(getattr(config, "datatype_columns", []))
    if not datatype_columns:
        return []

    df = file_reader.dataframe
    results: List[Dict[str, Any]] = []

    for col_cfg in datatype_columns:
        t0 = time.perf_counter()
        column = col_cfg.column
        expected_type = col_cfg.expected_type.lower()

        if column not in df.columns:
            results.append({
                "test_name": f"datatype_validation:{column}",
                "status": "WARNING",
                "details": {"reason": f"Column '{column}' not found in source file"},
                "execution_time_ms": 0.0,
            })
            continue

        checker = _TYPE_CHECKERS.get(expected_type)
        if checker is None:
            results.append({
                "test_name": f"datatype_validation:{column}",
                "status": "ERROR",
                "details": {"reason": f"Unknown expected_type '{expected_type}'"},
                "execution_time_ms": 0.0,
            })
            continue

        col_series = df[column]
        invalid_rows: List[Dict[str, Any]] = []
        for idx, val in col_series.items():
            # Nulls are skipped here — null validation is handled by Layer 9.
            if pd.isna(val) or val == "":
                continue
            if not checker(val):
                invalid_rows.append({"row_index": int(idx), "value": str(val)})

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        status = "PASS" if not invalid_rows else "FAIL"

        results.append({
            "test_name": f"datatype_validation:{column}",
            "status": status,
            "expected": expected_type,
            "actual": expected_type if not invalid_rows else f"{len(invalid_rows)} value(s) failed type check",
            "details": {
                "column": column,
                "expected_type": expected_type,
                "invalid_count": len(invalid_rows),
                "invalid_samples": invalid_rows[:10],
            },
            "execution_time_ms": elapsed_ms,
        })
        logger.info(
            "  Datatype check [%s] expected=%s: %d invalid → %s",
            column, expected_type, len(invalid_rows), status,
        )

    return results