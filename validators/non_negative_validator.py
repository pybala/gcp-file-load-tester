"""
non_negative_validator.py
-------------------------
Validates that numeric column values in the source file are greater than
or equal to zero (non-negative).

This is a focused companion to ``range_validator`` for the common case
where a column must simply never go negative — e.g. amounts, quantities,
counts, durations, prices.

File-side only — no BigQuery query is required.

Config shape:
  non_negative_columns : list[str]  — column names to check

  Leave as an empty list [] to skip the layer; there is no "check all"
  behaviour for this validator because only numeric columns are meaningful.
"""

import logging
import time
from typing import Any, Dict, List

import pandas as pd

from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def validate(
    file_reader: FileReader,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Check that every non-null value in each declared column is >= 0.

    Args:
        file_reader: Loaded FileReader instance.
        config:      ValidationConfig with ``non_negative_columns`` list.

    Returns:
        List of result dicts — one per column checked.
    """
    non_negative_columns: List[str] = list(getattr(config, "non_negative_columns", []))
    if not non_negative_columns:
        return []

    df = file_reader.dataframe
    results: List[Dict[str, Any]] = []

    for column in non_negative_columns:
        t0 = time.perf_counter()

        if column not in df.columns:
            results.append({
                "test_name": f"non_negative_validation:{column}",
                "status": "WARNING",
                "details": {"reason": f"Column '{column}' not found in source file"},
                "execution_time_ms": 0.0,
            })
            continue

        # Coerce to numeric; non-parsable values become NaN (skipped)
        numeric_series = pd.to_numeric(df[column], errors="coerce")
        non_null = numeric_series.dropna()

        if non_null.empty:
            results.append({
                "test_name": f"non_negative_validation:{column}",
                "status": "WARNING",
                "details": {
                    "reason": f"Column '{column}' has no numeric values to check",
                },
                "execution_time_ms": round((time.perf_counter() - t0) * 1000, 2),
            })
            continue

        negative_series = numeric_series[numeric_series < 0].dropna()
        negative_count = int(len(negative_series))

        # Collect sample violations for diagnostics (up to 10)
        violation_samples: List[Dict[str, Any]] = [
            {"row_index": int(idx), "value": float(val)}
            for idx, val in list(negative_series.items())[:10]
        ]

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        status = "PASS" if negative_count == 0 else "FAIL"

        results.append({
            "test_name": f"non_negative_validation:{column}",
            "status": status,
            "expected": 0,
            "actual": negative_count,
            "details": {
                "column": column,
                "negative_value_count": negative_count,
                "actual_min": float(non_null.min()),
                "violation_samples": violation_samples,
            },
            "execution_time_ms": elapsed_ms,
        })
        logger.info(
            "  Non-negative check [%s]: %d negative value(s) → %s",
            column, negative_count, status,
        )

    return results