"""
aggregate_validator.py
----------------------
Validates aggregate statistics (sum, min, max, avg, distinct_count) for
configured columns, comparing file-computed values against BigQuery results.

Each column × function pair produces an individual result entry so failures
can be pinpointed precisely.
"""

import logging
import math
import time
from typing import Any, Dict, List

from core.bigquery_client import BigQueryClient
from core.file_reader import FileReader

logger = logging.getLogger(__name__)

# Relative tolerance for floating-point comparisons (0.01 = 1%)
_FLOAT_TOLERANCE = 0.01


def validate(
    file_reader: FileReader,
    bq_client: BigQueryClient,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Run aggregate validation for all columns specified in config.aggregate_columns.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig — uses config.aggregate_columns,
                     config.dataset, config.table.

    Returns:
        List of result dicts, one per column × function combination.
    """
    results: List[Dict[str, Any]] = []

    if not config.aggregate_columns:
        logger.info("No aggregate_columns defined — skipping aggregate validation.")
        return [
            {
                "test_name": "aggregate_validation",
                "status": "SKIPPED",
                "expected": None,
                "actual": None,
                "details": {"reason": "No aggregate_columns defined in config"},
                "execution_time_ms": 0.0,
            }
        ]

    for col_config in config.aggregate_columns:
        column = col_config.column
        functions = col_config.functions

        logger.info("Running aggregate validation for column '%s' — functions: %s", column, functions)

        # Fetch BQ aggregates for this column in a single query
        bq_aggs = _safe_bq_aggregate(bq_client, config, column, functions)

        for fn in functions:
            results.append(
                _validate_single_aggregate(file_reader, column, fn, bq_aggs)
            )

    return results


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _safe_bq_aggregate(
    bq_client: BigQueryClient,
    config: Any,
    column: str,
    functions: List[str],
) -> Dict[str, Any]:
    """
    Fetch BQ aggregates, returning an empty dict on error (error will surface
    per-function when the key is missing).
    """
    try:
        return bq_client.get_aggregate_stats(
            config.dataset, config.table, column, list(functions)
        )
    except Exception as exc:
        logger.error(
            "Failed to fetch BQ aggregates for column '%s': %s", column, exc, exc_info=True
        )
        return {"__error__": str(exc)}


def _validate_single_aggregate(
    file_reader: FileReader,
    column: str,
    fn: str,
    bq_aggs: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compare a single file-side aggregate value against its BQ counterpart.

    Args:
        file_reader: FileReader instance.
        column:      Column name.
        fn:          Aggregate function name.
        bq_aggs:     Dict of BQ aggregate results for this column.

    Returns:
        Result dict.
    """
    start = time.perf_counter()
    test_name = f"aggregate_validation_{column}_{fn}"

    try:
        # Check if BQ fetch had an error
        if "__error__" in bq_aggs:
            raise RuntimeError(f"BQ aggregate error: {bq_aggs['__error__']}")

        # Compute file-side value
        file_value = file_reader.compute_aggregate(column, fn)

        # Retrieve BQ value
        bq_value = bq_aggs.get(fn)

        # Compare values with tolerance for floats
        status, details = _compare_values(fn, file_value, bq_value, column)

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": _serialisable(file_value),
            "actual": _serialisable(bq_value),
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


def _compare_values(
    fn: str,
    file_value: Any,
    bq_value: Any,
    column: str,
) -> tuple:
    """
    Compare file and BQ aggregate values with appropriate tolerance.

    Returns:
        Tuple of (status_str, details_dict).
    """
    if file_value is None and bq_value is None:
        return "PASS", {"note": "Both values are null"}

    if file_value is None or bq_value is None:
        return "FAIL", {
            "column": column,
            "function": fn,
            "file_value": _serialisable(file_value),
            "bq_value": _serialisable(bq_value),
            "note": "One value is null while the other is not",
        }

    # For numeric functions, allow a small relative tolerance
    if fn in ("sum", "avg"):
        try:
            f_val = float(file_value)
            b_val = float(bq_value)
            if f_val == 0 and b_val == 0:
                match = True
            elif f_val == 0:
                match = abs(b_val) < 1e-9
            else:
                match = abs(f_val - b_val) / abs(f_val) <= _FLOAT_TOLERANCE
        except (TypeError, ValueError):
            match = str(file_value) == str(bq_value)
    elif fn == "distinct_count":
        match = int(file_value) == int(bq_value)
    else:
        # min / max: try numeric comparison first, then timestamp-aware
        # normalisation, then fall back to string equality.
        # Numeric: handles NUMERIC/FLOAT columns where BQ drops trailing
        # decimal zeros (e.g. file=4.0 vs bq="4").
        # Timestamp: handles TIMESTAMP columns where BQ returns ISO-8601
        # with 'T' separator and '+00:00' suffix vs the plain CSV format.
        match = _compare_min_max(file_value, bq_value)

    status = "PASS" if match else "FAIL"
    details = {
        "column": column,
        "function": fn,
        "file_value": _serialisable(file_value),
        "bq_value": _serialisable(bq_value),
    }
    if not match and fn in ("sum", "avg"):
        try:
            delta = float(bq_value) - float(file_value)
            details["delta"] = round(delta, 6)
        except (TypeError, ValueError):
            pass

    if status == "PASS":
        logger.info("Aggregate PASSED — %s(%s)", fn, column)
    else:
        logger.warning(
            "Aggregate FAILED — %s(%s): file=%s, bq=%s",
            fn, column, file_value, bq_value,
        )

    return status, details


def _compare_min_max(file_value: Any, bq_value: Any) -> bool:
    """
    Compare min/max aggregate values with numeric and timestamp awareness.

    Strategy (in order):
    1. Try numeric comparison with a small relative tolerance — handles
       NUMERIC/FLOAT columns where BigQuery drops trailing zeros
       (e.g. file=4.0 vs bq="4").
    2. Normalise both sides as timestamp strings and compare — handles
       TIMESTAMP columns where BigQuery returns ISO-8601 with 'T' separator
       and '+00:00' UTC suffix (e.g. "2025-02-01T10:15:30+00:00") while the
       CSV side has a plain space separator ("2025-02-01 10:15:30").
    3. Fall back to exact string comparison.
    """
    # 1. Numeric comparison
    try:
        f = float(file_value)
        b = float(bq_value)
        if f == 0 and b == 0:
            return True
        if f == 0:
            return abs(b) < 1e-9
        return abs(f - b) / abs(f) <= _FLOAT_TOLERANCE
    except (TypeError, ValueError):
        pass

    # 2. Timestamp-aware normalisation
    f_str = _normalise_ts(str(file_value))
    b_str = _normalise_ts(str(bq_value))
    if f_str == b_str:
        return True

    # 3. Exact string fallback
    return str(file_value) == str(bq_value)


def _normalise_ts(value: str) -> str:
    """
    Normalise a timestamp/datetime string so that format differences between
    the CSV source and BigQuery do not cause false mismatches.

    Transformations applied:
    - Strip trailing '+00:00' UTC offset   ("…+00:00" → "…")
    - Strip trailing 'Z' UTC designator    ("…Z"       → "…")
    - Replace 'T' date/time separator with a space  ("…T…" → "… …")
    - Strip any remaining leading/trailing whitespace
    """
    s = value.strip()
    if s.endswith("+00:00"):
        s = s[: -len("+00:00")].strip()
    elif s.endswith("Z"):
        s = s[:-1].strip()
    s = s.replace("T", " ")
    return s


def _serialisable(value: Any) -> Any:
    """Convert non-JSON-serialisable types to their string/float equivalents."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):          # numpy scalar
        return value.item()
    if hasattr(value, "isoformat"):     # date / datetime
        return value.isoformat()
    return value
