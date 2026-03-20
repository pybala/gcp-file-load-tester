"""
json_schema_validator.py
------------------------
Validates that JSON / STRUCT columns in the source file contain the
required keys declared in the config.

Useful for catching incomplete or malformed JSON payloads before they
reach BigQuery.  For example, a ``customer_info`` column that should
always carry ``name`` and ``age`` keys.

Supports two value shapes per cell:
  • Python dict   — already parsed (e.g. JSONL files loaded via FileReader)
  • JSON string   — a column stored as a JSON-formatted string in CSV

File-side only — no BigQuery query is required.

Config shape (per column):
  column        : str        — column name
  required_keys : list[str]  — keys that must be present in every row's value
"""

import json
import logging
import time
from typing import Any, Dict, List

import pandas as pd

from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def _parse_cell(val: Any) -> Any:
    """
    Attempt to parse a cell value as JSON.

    Returns the parsed object (dict / list) if successful, or the original
    value unchanged.  Returns None for null / empty cells.
    """
    if pd.isna(val) or val == "":
        return None
    if isinstance(val, dict):
        return val
    try:
        return json.loads(str(val))
    except (json.JSONDecodeError, TypeError, ValueError):
        return val  # return as-is; the caller will report it as invalid


def validate(
    file_reader: FileReader,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Check that each row's JSON/STRUCT column value contains all required keys.

    Args:
        file_reader: Loaded FileReader instance.
        config:      ValidationConfig with ``json_schema_columns`` list.

    Returns:
        List of result dicts — one per column checked.
    """
    json_schema_columns = list(getattr(config, "json_schema_columns", []))
    if not json_schema_columns:
        return []

    df = file_reader.dataframe
    results: List[Dict[str, Any]] = []

    for col_cfg in json_schema_columns:
        t0 = time.perf_counter()
        column = col_cfg.column
        required_keys: List[str] = list(col_cfg.required_keys)

        if column not in df.columns:
            results.append({
                "test_name": f"json_schema_validation:{column}",
                "status": "WARNING",
                "details": {"reason": f"Column '{column}' not found in source file"},
                "execution_time_ms": 0.0,
            })
            continue

        violations: List[Dict[str, Any]] = []

        for idx, val in df[column].items():
            parsed = _parse_cell(val)

            # Skip nulls — null check is handled by Layer 9
            if parsed is None:
                continue

            if not isinstance(parsed, dict):
                violations.append({
                    "row_index": int(idx),
                    "issue": "Value is not a JSON object (dict)",
                    "value": str(val)[:120],
                })
                continue

            missing = [k for k in required_keys if k not in parsed]
            if missing:
                violations.append({
                    "row_index": int(idx),
                    "missing_keys": missing,
                    "present_keys": sorted(parsed.keys()),
                })

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        status = "PASS" if not violations else "FAIL"

        results.append({
            "test_name": f"json_schema_validation:{column}",
            "status": status,
            "expected": f"All rows contain keys: {required_keys}",
            "actual": (
                "All rows have required keys"
                if not violations
                else f"{len(violations)} row(s) missing required keys"
            ),
            "details": {
                "column": column,
                "required_keys": required_keys,
                "violation_count": len(violations),
                "violation_samples": violations[:10],
            },
            "execution_time_ms": elapsed_ms,
        })
        logger.info(
            "  JSON schema check [%s] required=%s: %d violations → %s",
            column, required_keys, len(violations), status,
        )

    return results