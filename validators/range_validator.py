"""
range_validator.py
------------------
Validates that numeric column values in the source file fall within a
declared minimum and/or maximum boundary.

Prevents bad values (e.g. negative ages, impossible percentages > 100,
future dates in a historical dataset) from passing silently.

File-side only — no BigQuery query is required.

Config shape (per column):
  column : str           — column name
  min    : float | null  — inclusive lower bound  (omit to skip lower check)
  max    : float | null  — inclusive upper bound  (omit to skip upper check)
"""

import logging
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def validate(
    file_reader: FileReader,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Check that each column's values are within the declared [min, max] range.

    Args:
        file_reader: Loaded FileReader instance.
        config:      ValidationConfig with ``range_columns`` list.

    Returns:
        List of result dicts — one per column checked.
    """
    range_columns = list(getattr(config, "range_columns", []))
    if not range_columns:
        return []

    df = file_reader.dataframe
    results: List[Dict[str, Any]] = []

    for col_cfg in range_columns:
        t0 = time.perf_counter()
        column = col_cfg.column
        bound_min: Optional[float] = col_cfg.min
        bound_max: Optional[float] = col_cfg.max

        if column not in df.columns:
            results.append({
                "test_name": f"range_validation:{column}",
                "status": "WARNING",
                "details": {"reason": f"Column '{column}' not found in source file"},
                "execution_time_ms": 0.0,
            })
            continue

        # Convert column to numeric; coerce non-numeric to NaN
        numeric_series = pd.to_numeric(df[column], errors="coerce")
        non_null = numeric_series.dropna()

        if non_null.empty:
            results.append({
                "test_name": f"range_validation:{column}",
                "status": "WARNING",
                "details": {
                    "reason": f"Column '{column}' has no numeric values to check",
                },
                "execution_time_ms": round((time.perf_counter() - t0) * 1000, 2),
            })
            continue

        actual_min = float(non_null.min())
        actual_max = float(non_null.max())

        violations: List[Dict[str, Any]] = []

        # Below-minimum violations
        if bound_min is not None:
            below = numeric_series[numeric_series < bound_min].dropna()
            for idx, val in below.items():
                violations.append({
                    "row_index": int(idx),
                    "value": float(val),
                    "violation": f"< min ({bound_min})",
                })

        # Above-maximum violations
        if bound_max is not None:
            above = numeric_series[numeric_series > bound_max].dropna()
            for idx, val in above.items():
                violations.append({
                    "row_index": int(idx),
                    "value": float(val),
                    "violation": f"> max ({bound_max})",
                })

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        status = "PASS" if not violations else "FAIL"

        results.append({
            "test_name": f"range_validation:{column}",
            "status": status,
            "expected": {
                "min": bound_min,
                "max": bound_max,
            },
            "actual": {
                "min": actual_min,
                "max": actual_max,
            },
            "details": {
                "column": column,
                "declared_min": bound_min,
                "declared_max": bound_max,
                "actual_min": actual_min,
                "actual_max": actual_max,
                "violation_count": len(violations),
                "violation_samples": violations[:10],
            },
            "execution_time_ms": elapsed_ms,
        })
        logger.info(
            "  Range check [%s] [%s, %s]: actual=[%s, %s], violations=%d → %s",
            column, bound_min, bound_max, actual_min, actual_max,
            len(violations), status,
        )

    return results