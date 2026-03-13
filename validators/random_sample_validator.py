"""
random_sample_validator.py
--------------------------
Validates data correctness by randomly sampling rows from the source CSV file,
fetching the corresponding rows from BigQuery (via primary key lookup), and
performing field-level value comparison.

This is the most fine-grained validator — it catches value-level discrepancies
(wrong values, type mismatches, encoding issues) that aggregate checks miss.

Requires primary_keys to be defined in the config.
"""

import logging
import time
from typing import Any, Dict, List, Optional

from core.bigquery_client import BigQueryClient
from core.file_reader import FileReader

logger = logging.getLogger(__name__)

# Relative tolerance for numeric comparisons (0.0001 = 0.01%)
_NUMERIC_TOLERANCE = 0.0001


def validate(
    file_reader: FileReader,
    bq_client: BigQueryClient,
    config: Any,
) -> Dict[str, Any]:
    """
    Randomly sample rows from the file and compare them field-by-field with BQ.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig — uses config.primary_keys,
                     config.random_sample_size, config.dataset, config.table.

    Returns:
        Result dict with keys: test_name, status, expected, actual,
        details, execution_time_ms.
    """
    start = time.perf_counter()
    test_name = "random_sampling_validation"

    # Guard: primary keys required for BQ lookup
    if not config.primary_keys:
        logger.warning("random_sampling skipped — no primary_keys defined.")
        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": "SKIPPED",
            "expected": None,
            "actual": None,
            "details": {"reason": "No primary_keys defined in config"},
            "execution_time_ms": round(elapsed_ms, 3),
        }

    try:
        primary_keys = config.primary_keys
        sample_size = config.random_sample_size

        # ---------------------------------------------------------------
        # 1. Sample rows from the file
        # ---------------------------------------------------------------
        file_sample = file_reader.get_random_sample(
            n=sample_size,
            primary_keys=primary_keys,
        )
        actual_sample_size = len(file_sample)
        logger.info("Sampled %d rows from file for comparison", actual_sample_size)

        if actual_sample_size == 0:
            elapsed_ms = (time.perf_counter() - start) * 1000
            return {
                "test_name": test_name,
                "status": "SKIPPED",
                "expected": None,
                "actual": None,
                "details": {"reason": "File is empty — no rows to sample"},
                "execution_time_ms": round(elapsed_ms, 3),
            }

        # ---------------------------------------------------------------
        # 2. Fetch corresponding rows from BigQuery
        # ---------------------------------------------------------------
        bq_rows = bq_client.get_rows_by_primary_keys(
            dataset=config.dataset,
            table=config.table,
            primary_keys=primary_keys,
            key_values=file_sample,
        )
        logger.info("Fetched %d matching rows from BigQuery", len(bq_rows))

        # Index BQ rows by composite PK for fast lookup
        bq_index: Dict[str, Dict[str, Any]] = {}
        for row in bq_rows:
            pk_key = _make_pk_key(row, primary_keys)
            bq_index[pk_key] = row

        # ---------------------------------------------------------------
        # 3. Field-level comparison
        # ---------------------------------------------------------------
        row_results: List[Dict[str, Any]] = []
        missing_in_bq: List[str] = []
        rows_with_mismatches = 0

        for file_row in file_sample:
            pk_key = _make_pk_key(file_row, primary_keys)
            bq_row = bq_index.get(pk_key)

            if bq_row is None:
                missing_in_bq.append(pk_key)
                row_results.append({
                    "pk": pk_key,
                    "status": "MISSING_IN_BQ",
                    "mismatches": [],
                })
                rows_with_mismatches += 1
                continue

            mismatches = _compare_row(file_row, bq_row)
            row_status = "PASS" if not mismatches else "FAIL"
            if mismatches:
                rows_with_mismatches += 1

            row_results.append({
                "pk": pk_key,
                "status": row_status,
                "mismatches": mismatches,
            })

        # ---------------------------------------------------------------
        # 4. Summarise
        # ---------------------------------------------------------------
        passing_rows = actual_sample_size - rows_with_mismatches
        status = "PASS" if rows_with_mismatches == 0 else "FAIL"

        details: Dict[str, Any] = {
            "sample_size_requested": sample_size,
            "sample_size_actual": actual_sample_size,
            "rows_matched_in_bq": len(bq_rows),
            "rows_missing_in_bq": len(missing_in_bq),
            "rows_with_field_mismatches": rows_with_mismatches,
            "rows_passing": passing_rows,
            "missing_in_bq_pks": missing_in_bq,
            # Include only failing rows in details to keep output concise
            "failing_rows": [r for r in row_results if r["status"] != "PASS"],
        }

        if status == "PASS":
            logger.info(
                "Random sampling PASSED — all %d sampled rows matched", actual_sample_size
            )
        else:
            logger.warning(
                "Random sampling FAILED — %d/%d rows had discrepancies",
                rows_with_mismatches,
                actual_sample_size,
            )

        elapsed_ms = (time.perf_counter() - start) * 1000
        return {
            "test_name": test_name,
            "status": status,
            "expected": actual_sample_size,
            "actual": passing_rows,
            "details": details,
            "execution_time_ms": round(elapsed_ms, 3),
        }

    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error("Random sampling validation error: %s", exc, exc_info=True)
        return {
            "test_name": test_name,
            "status": "ERROR",
            "expected": None,
            "actual": None,
            "details": {"error": str(exc)},
            "execution_time_ms": round(elapsed_ms, 3),
        }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _make_pk_key(row: Dict[str, Any], primary_keys: List[str]) -> str:
    """
    Build a string composite key from a row dict and list of PK column names.

    Args:
        row:          Row dict (file or BQ).
        primary_keys: List of PK column names.

    Returns:
        A string like "val1::val2" suitable for use as a dict key.
    """
    return "::".join(str(row.get(k, "")) for k in primary_keys)


def _compare_row(
    file_row: Dict[str, Any],
    bq_row: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Compare all common fields between a file row and its BQ counterpart.

    Handles nested types (STRUCT → dict, ARRAY → list) by comparing them
    directly as Python objects rather than converting to strings first.
    This avoids false mismatches caused by dict-key ordering differences
    between the source JSONL (insertion order) and BigQuery (alphabetical).

    Args:
        file_row: Row dict from the file.
        bq_row:   Row dict from BigQuery.

    Returns:
        List of mismatch dicts (empty if all fields match).
    """
    mismatches = []
    common_columns = set(file_row.keys()) & set(bq_row.keys())

    for col in sorted(common_columns):
        file_val = file_row[col]
        bq_val = bq_row[col]

        # ------------------------------------------------------------------
        # STRUCT (dict) comparison — order-independent deep equality.
        # BigQuery returns STRUCT fields in alphabetical key order; the file
        # preserves JSONL insertion order.  Comparing as dicts is correct.
        # ------------------------------------------------------------------
        if isinstance(file_val, dict) or isinstance(bq_val, dict):
            match = _dicts_equal(file_val, bq_val)
            if not match:
                mismatches.append({
                    "column": col,
                    "file_value": str(file_val),
                    "bq_value": str(bq_val),
                })
            continue

        # ------------------------------------------------------------------
        # ARRAY (list) comparison — element-wise, order-preserving.
        # ------------------------------------------------------------------
        if isinstance(file_val, list) or isinstance(bq_val, list):
            match = _lists_equal(file_val, bq_val)
            if not match:
                mismatches.append({
                    "column": col,
                    "file_value": str(file_val),
                    "bq_value": str(bq_val),
                })
            continue

        # ------------------------------------------------------------------
        # Scalar comparison — normalise to string then use numeric tolerance.
        # ------------------------------------------------------------------
        file_str = _normalise(file_val)
        bq_str = _normalise(bq_val)

        if not _values_match(file_str, bq_str):
            mismatches.append({
                "column": col,
                "file_value": file_str,
                "bq_value": bq_str,
            })

    return mismatches


def _dicts_equal(a: Any, b: Any) -> bool:
    """
    Recursively compare two dicts for equality, ignoring key order.

    Handles the common case where one side is a JSON string and the other
    is a parsed dict — CSV files store JSON columns as raw strings, while
    BigQuery JSON columns are returned as Python dicts by the BQ client.
    Both sides are parsed as JSON when possible before comparison.

    Args:
        a: First value (may be a dict or a JSON string).
        b: Second value (may be a dict or a JSON string).

    Returns:
        True if a and b are deeply equal regardless of key order.
    """
    import json as _json

    if a is None and b is None:
        return True

    # Attempt to parse JSON strings so that a CSV JSON string and a BQ dict
    # are compared as equivalent Python objects.
    if isinstance(a, str):
        try:
            a = _json.loads(a)
        except (ValueError, TypeError):
            pass
    if isinstance(b, str):
        try:
            b = _json.loads(b)
        except (ValueError, TypeError):
            pass

    if not isinstance(a, dict) or not isinstance(b, dict):
        return a == b
    if set(a.keys()) != set(b.keys()):
        return False
    return all(_dicts_equal(a[k], b[k]) for k in a)


def _lists_equal(a: Any, b: Any) -> bool:
    """
    Compare two lists element-wise, supporting nested dicts.

    Args:
        a: First value (expected to be a list).
        b: Second value (expected to be a list).

    Returns:
        True if lists have the same length and all elements are equal.
    """
    if a is None and b is None:
        return True
    if not isinstance(a, list) or not isinstance(b, list):
        return a == b
    if len(a) != len(b):
        return False
    return all(
        _dicts_equal(x, y) if isinstance(x, dict) else x == y
        for x, y in zip(a, b)
    )


def _normalise(value: Any) -> Optional[str]:
    """
    Convert a value to a normalised string for comparison.

    Normalisation rules applied in order:
    1. None / NaN              → None
    2. datetime with tzinfo    → strip timezone suffix, keep naive representation
       e.g. datetime(2025,1,1,10,0,0,tzinfo=UTC) → "2025-01-01 10:00:00"
    3. datetime without tzinfo → format without microseconds
    4. date                    → ISO format string "YYYY-MM-DD"
    5. float with no fraction  → format as integer string (e.g. "2.0" → "2")
    6. Everything else         → str(value).strip()

    The timezone stripping is intentional: CSV files store naive timestamps
    (no timezone), while BigQuery always returns TIMESTAMP columns as
    UTC-aware datetime objects.  Since the load process preserves the wall-
    clock value as UTC, comparing the naive representation is correct.
    """
    import math
    import datetime as _dt

    if value is None:
        return None

    # pandas / numpy NaN
    if isinstance(value, float) and math.isnan(value):
        return None

    # datetime (must check before date — datetime is a subclass of date)
    if isinstance(value, _dt.datetime):
        # Strip timezone — both file (naive) and BQ (UTC-aware) represent
        # the same wall-clock instant; compare only the local representation.
        naive = value.replace(tzinfo=None)
        # Drop microseconds if zero to match typical CSV precision
        if naive.microsecond == 0:
            return naive.strftime("%Y-%m-%d %H:%M:%S")
        return naive.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0")

    # date (not datetime)
    if isinstance(value, _dt.date):
        return value.isoformat()

    # float that is a whole number → strip the ".0" so "2.0" == "2"
    if isinstance(value, float) and value == int(value):
        return str(int(value))

    result = str(value).strip()

    # String representation of a timezone-aware timestamp
    # e.g. "2025-01-01 10:00:00+00:00" or "2025-01-01T10:00:00Z"
    if "+" in result and "T" not in result:
        # "YYYY-MM-DD HH:MM:SS+HH:MM" → strip offset
        result = result.split("+")[0].strip()
    elif result.endswith("+00:00"):
        result = result[: -len("+00:00")].strip()
    elif result.endswith("Z") and len(result) > 1:
        result = result[:-1].replace("T", " ").strip()

    return result


def _values_match(file_val: Optional[str], bq_val: Optional[str]) -> bool:
    """
    Compare two normalised string values, using numeric tolerance for floats.

    Args:
        file_val: Normalised file value.
        bq_val:   Normalised BQ value.

    Returns:
        True if values are considered equal.
    """
    if file_val is None and bq_val is None:
        return True
    if file_val is None or bq_val is None:
        return False
    if file_val == bq_val:
        return True

    # Try numeric comparison with tolerance
    try:
        f = float(file_val)
        b = float(bq_val)
        if f == 0 and b == 0:
            return True
        if f == 0:
            return abs(b) < 1e-9
        return abs(f - b) / abs(f) <= _NUMERIC_TOLERANCE
    except (ValueError, ZeroDivisionError):
        pass

    return False