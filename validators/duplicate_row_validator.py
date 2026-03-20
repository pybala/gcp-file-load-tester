"""
duplicate_row_validator.py
--------------------------
Validates that the source file contains no fully duplicate rows — i.e. rows
where every column value is identical to another row.

This is distinct from Layer 3 (primary_key_uniqueness), which only checks
duplicate combinations of the declared PK columns.  This layer checks all
columns simultaneously, catching cases where:
  • The same record was accidentally included twice in the extract.
  • A fan-out join produced duplicate rows that share the same PK.

File-side only — no BigQuery query is required.
"""

import json
import logging
import time
from typing import Any, Dict, List

import pandas as pd

from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def validate(
    file_reader: FileReader,
    config: Any,
) -> Dict[str, Any]:
    """
    Detect fully duplicate rows in the source file.

    A row is considered a duplicate if every column value matches another
    row exactly.  Null values are treated as equal for grouping purposes
    (pandas default behaviour for ``duplicated()``).

    Args:
        file_reader: Loaded FileReader instance.
        config:      ValidationConfig (no extra fields required).

    Returns:
        A single result dict.
    """
    t0 = time.perf_counter()
    df = file_reader.dataframe

    total_rows = len(df)

    # pandas duplicated() cannot hash unhashable column types (dict, list)
    # which arise from STRUCT/ARRAY columns in JSONL files.
    # Serialise any such columns to a stable JSON string before comparison.
    def _make_hashable(series: pd.Series) -> pd.Series:
        """Return a copy of the series with unhashable values JSON-serialised."""
        try:
            # Quick probe — will raise TypeError if any value is unhashable
            series.apply(hash)
            return series
        except TypeError:
            return series.apply(
                lambda v: json.dumps(v, sort_keys=True, default=str)
                if isinstance(v, (dict, list))
                else v
            )

    comparable_df = df.apply(_make_hashable)
    duplicate_mask = comparable_df.duplicated(keep=False)
    duplicate_count = int(duplicate_mask.sum())

    # Collect sample duplicate rows for diagnostics (up to 10)
    sample_duplicates: List[Dict[str, Any]] = []
    if duplicate_count > 0:
        dup_df = df[duplicate_mask].head(10)
        for idx, row in dup_df.iterrows():
            sample_duplicates.append({
                "row_index": int(idx),
                "values": {col: str(val) for col, val in row.items()},
            })

    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    status = "PASS" if duplicate_count == 0 else "FAIL"

    logger.info(
        "  Duplicate row check: %d/%d rows are duplicates → %s",
        duplicate_count, total_rows, status,
    )

    return {
        "test_name": "duplicate_row_validation",
        "status": status,
        "expected": 0,
        "actual": duplicate_count,
        "details": {
            "total_rows": total_rows,
            "duplicate_row_count": duplicate_count,
            "duplicate_sample": sample_duplicates,
        },
        "execution_time_ms": elapsed_ms,
    }