"""
regex_validator.py
------------------
Validates that string column values in the source file match a declared
regular expression pattern.

Useful for enforcing format constraints such as:
  • Email addresses
  • Phone numbers
  • ISO date strings
  • UUID / transaction IDs
  • Country codes (e.g. two-letter ISO 3166)
  • Postal / ZIP codes

File-side only — no BigQuery query is required.

Config shape (per column):
  column  : str  — column name
  pattern : str  — Python ``re``-compatible regular expression
"""

import logging
import re
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
    Check that each non-null column value fully matches the declared regex.

    Uses ``re.fullmatch`` — the pattern must match the *entire* value, not
    just a substring.  Wrap the pattern in ``.*`` on either side if a
    substring match is intended.

    Args:
        file_reader: Loaded FileReader instance.
        config:      ValidationConfig with ``regex_columns`` list.

    Returns:
        List of result dicts — one per column checked.
    """
    regex_columns = list(getattr(config, "regex_columns", []))
    if not regex_columns:
        return []

    df = file_reader.dataframe
    results: List[Dict[str, Any]] = []

    for col_cfg in regex_columns:
        t0 = time.perf_counter()
        column = col_cfg.column
        pattern_str = col_cfg.pattern

        if column not in df.columns:
            results.append({
                "test_name": f"regex_validation:{column}",
                "status": "WARNING",
                "details": {"reason": f"Column '{column}' not found in source file"},
                "execution_time_ms": 0.0,
            })
            continue

        # Compile pattern early to catch syntax errors
        try:
            compiled = re.compile(pattern_str)
        except re.error as exc:
            results.append({
                "test_name": f"regex_validation:{column}",
                "status": "ERROR",
                "details": {
                    "reason": f"Invalid regex pattern '{pattern_str}': {exc}",
                },
                "execution_time_ms": 0.0,
            })
            continue

        col_series = df[column]
        invalid_rows: List[Dict[str, Any]] = []

        for idx, val in col_series.items():
            # Skip nulls — null check is handled by Layer 9
            if pd.isna(val) or val == "":
                continue
            str_val = str(val)
            if not compiled.fullmatch(str_val):
                invalid_rows.append({"row_index": int(idx), "value": str_val})

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        status = "PASS" if not invalid_rows else "FAIL"

        results.append({
            "test_name": f"regex_validation:{column}",
            "status": status,
            "expected": f"All values match pattern: {pattern_str}",
            "actual": (
                "All values matched"
                if not invalid_rows
                else f"{len(invalid_rows)} value(s) did not match"
            ),
            "details": {
                "column": column,
                "pattern": pattern_str,
                "invalid_count": len(invalid_rows),
                "invalid_samples": invalid_rows[:10],
            },
            "execution_time_ms": elapsed_ms,
        })
        logger.info(
            "  Regex check [%s] pattern='%s': %d invalid → %s",
            column, pattern_str, len(invalid_rows), status,
        )

    return results