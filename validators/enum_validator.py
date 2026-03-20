"""
enum_validator.py
-----------------
Validates that each specified column in the source file contains only values
from the declared allowed set (enum / categorical validation).

Catches bad categorical values that would silently pass other layers such as
row count or aggregate validation.

File-side only — no BigQuery query is required.
"""

import logging
import time
from typing import Any, Dict, List, Set

import pandas as pd

from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def validate(
    file_reader: FileReader,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Check that every non-null value in each column is in the declared
    allowed set.

    Args:
        file_reader: Loaded FileReader instance.
        config:      ValidationConfig with ``enum_columns`` list.

    Returns:
        List of result dicts — one per column checked.
    """
    enum_columns = list(getattr(config, "enum_columns", []))
    if not enum_columns:
        return []

    df = file_reader.dataframe
    results: List[Dict[str, Any]] = []

    for col_cfg in enum_columns:
        t0 = time.perf_counter()
        column = col_cfg.column
        allowed: Set[str] = {str(v) for v in col_cfg.allowed_values}

        if column not in df.columns:
            results.append({
                "test_name": f"enum_validation:{column}",
                "status": "WARNING",
                "details": {"reason": f"Column '{column}' not found in source file"},
                "execution_time_ms": 0.0,
            })
            continue

        # Drop nulls — null check is handled by Layer 9
        non_null = df[column].dropna()
        non_null = non_null[non_null != ""]

        actual_values: Set[str] = set(non_null.astype(str).unique())
        invalid_values: Set[str] = actual_values - allowed

        # Count occurrences of each invalid value for diagnostic output
        invalid_counts: Dict[str, int] = {}
        if invalid_values:
            vc = non_null.astype(str).value_counts()
            invalid_counts = {v: int(vc.get(v, 0)) for v in invalid_values}

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        status = "PASS" if not invalid_values else "FAIL"

        results.append({
            "test_name": f"enum_validation:{column}",
            "status": status,
            "expected": sorted(allowed),
            "actual": sorted(actual_values),
            "details": {
                "column": column,
                "allowed_values": sorted(allowed),
                "invalid_values": sorted(invalid_values),
                "invalid_value_counts": invalid_counts,
                "invalid_row_count": sum(invalid_counts.values()),
            },
            "execution_time_ms": elapsed_ms,
        })
        logger.info(
            "  Enum check [%s]: %d invalid value(s) → %s",
            column, len(invalid_values), status,
        )

    return results