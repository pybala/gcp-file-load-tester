"""
result_formatter.py
-------------------
Formats the raw list of validator result dicts into the final JSON output
structure required by the framework.

Output structure:
{
  "run_id": "<uuid4>",
  "dataset": "<dataset>",
  "table": "<table>",
  "timestamp": "<ISO-8601 UTC>",
  "overall_status": "PASS | FAIL | ERROR",
  "summary": { "total": N, "passed": N, "failed": N, "errors": N, "skipped": N },
  "results": [ { test_name, status, expected, actual, details, execution_time_ms }, ... ]
}
"""

import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Status precedence for determining overall_status
_STATUS_RANK = {"ERROR": 4, "FAIL": 3, "WARN": 2, "PASS": 1, "SKIPPED": 0}


def format_output(
    run_id: str,
    config: Any,
    results: List[Dict[str, Any]],
    run_start_iso: str,
) -> Dict[str, Any]:
    """
    Assemble the final output dictionary.

    Args:
        run_id:        UUID4 string identifying this run.
        config:        ValidationConfig instance.
        results:       Flat list of result dicts from all validators.
        run_start_iso: ISO-8601 timestamp when the run started.

    Returns:
        Fully assembled output dict (JSON-serialisable).
    """
    summary = _build_summary(results)
    overall_status = _compute_overall_status(results)
    total_ms = sum(r.get("execution_time_ms", 0) for r in results)

    output = {
        "run_id": run_id,
        "dataset": config.dataset,
        "table": config.table,
        "file_path": config.file_path,
        "timestamp": run_start_iso,
        "overall_status": overall_status,
        "total_execution_time_ms": round(total_ms, 3),
        "summary": summary,
        "results": [_sanitise_result(r) for r in results],
    }

    logger.info(
        "Run %s completed — overall_status=%s, tests=%d, passed=%d, failed=%d, errors=%d",
        run_id,
        overall_status,
        summary["total"],
        summary["passed"],
        summary["failed"],
        summary["errors"],
    )

    return output


def to_json(output: Dict[str, Any], indent: int = 2) -> str:
    """
    Serialise the output dict to a JSON string.

    Uses a custom encoder to handle non-standard types (dates, numpy scalars).

    Args:
        output: Assembled output dict.
        indent: JSON indentation level.

    Returns:
        JSON string.
    """
    return json.dumps(output, indent=indent, default=_json_default, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_summary(results: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count results by status category."""
    counts = {"total": 0, "passed": 0, "failed": 0, "errors": 0, "warned": 0, "skipped": 0}
    for r in results:
        counts["total"] += 1
        status = r.get("status", "").upper()
        if status == "PASS":
            counts["passed"] += 1
        elif status == "FAIL":
            counts["failed"] += 1
        elif status == "ERROR":
            counts["errors"] += 1
        elif status == "WARN":
            counts["warned"] += 1
        elif status == "SKIPPED":
            counts["skipped"] += 1
    return counts


def _compute_overall_status(results: List[Dict[str, Any]]) -> str:
    """
    Determine the overall run status based on the highest-rank individual status.

    Rank: ERROR > FAIL > WARN > PASS > SKIPPED
    """
    if not results:
        return "PASS"
    max_rank = max(
        _STATUS_RANK.get(r.get("status", "").upper(), 0) for r in results
    )
    for status, rank in _STATUS_RANK.items():
        if rank == max_rank:
            return status
    return "PASS"


def _sanitise_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure required keys are present and values are JSON-safe.

    Args:
        result: Raw result dict from a validator.

    Returns:
        Sanitised result dict with all required keys.
    """
    return {
        "test_name": result.get("test_name", "unknown"),
        "status": result.get("status", "UNKNOWN"),
        "expected": _make_serialisable(result.get("expected")),
        "actual": _make_serialisable(result.get("actual")),
        "details": _make_serialisable(result.get("details", {})),
        "execution_time_ms": result.get("execution_time_ms", 0.0),
    }


def _make_serialisable(value: Any) -> Any:
    """
    Recursively convert a value to a JSON-serialisable type.

    Handles: None, bool, int, float, str, list, dict, datetime, numpy scalars.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {k: _make_serialisable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_make_serialisable(v) for v in value]
    if hasattr(value, "isoformat"):          # datetime / date
        return value.isoformat()
    if hasattr(value, "item"):               # numpy scalar
        return _make_serialisable(value.item())
    return str(value)


def _json_default(obj: Any) -> Any:
    """Fallback JSON encoder for types not handled by _make_serialisable."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "item"):
        return obj.item()
    return str(obj)


def current_utc_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()