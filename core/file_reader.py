"""
file_reader.py
--------------
Reads source data files (CSV, JSONL, JSON) into a pandas DataFrame.

Supports two file location modes:
  - Local filesystem path:  /path/to/file.csv  or  /path/to/file.jsonl
  - Google Cloud Storage:   gs://bucket-name/path/to/file.csv

When a GCS URI is provided the file is downloaded to a temporary local
directory before being loaded into pandas.  The temp file is cleaned up
automatically after loading.

Provides helper methods reused across multiple validators to avoid
re-reading the file on every validation step.

Supported file formats:
  .csv              — comma-separated values (header row required)
  .jsonl / .ndjson  — newline-delimited JSON (one JSON object per line)
  .json             — either a JSON array or newline-delimited JSON
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GCS helper
# ---------------------------------------------------------------------------

def _is_gcs_path(path: str) -> bool:
    """Return True if the path is a GCS URI (gs://...)."""
    return path.startswith("gs://")


def _download_from_gcs(gcs_uri: str, dest_dir: str) -> str:
    """
    Download a file from GCS to a local temp directory.

    Args:
        gcs_uri:  GCS URI in the form gs://bucket/object/path.
        dest_dir: Local directory to download into.

    Returns:
        Local file path of the downloaded file.

    Raises:
        ImportError: If google-cloud-storage is not installed.
        Exception:   On GCS download failure.
    """
    try:
        from google.cloud import storage as gcs
    except ImportError as exc:
        raise ImportError(
            "google-cloud-storage is required for GCS file support. "
            "Install it with: pip install google-cloud-storage"
        ) from exc

    # Parse gs://bucket/object
    without_prefix = gcs_uri[len("gs://"):]
    bucket_name, _, blob_name = without_prefix.partition("/")

    local_filename = os.path.basename(blob_name) or "downloaded_file.csv"
    local_path = os.path.join(dest_dir, local_filename)

    logger.info("Downloading GCS file: %s → %s", gcs_uri, local_path)
    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)
    logger.info("GCS download complete: %s (%d bytes)", local_path, os.path.getsize(local_path))

    return local_path


def _is_jsonlines(local_path: str) -> bool:
    """
    Heuristic: check if a .json file is actually newline-delimited JSON.

    Reads the first non-empty line; if it is a standalone JSON object
    (starts with '{'), treat the file as JSONL.

    Args:
        local_path: Local file path.

    Returns:
        True if the file appears to be JSONL, False otherwise.
    """
    try:
        with open(local_path, "r", encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if stripped:
                    return stripped.startswith("{")
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# FileReader
# ---------------------------------------------------------------------------

class FileReader:
    """
    Reads and caches a source data file as a pandas DataFrame.

    Supports CSV, JSONL (newline-delimited JSON), and plain JSON formats,
    as well as both local paths and GCS URIs (gs://bucket/path).

    The DataFrame is loaded once and reused across all validation steps,
    avoiding redundant I/O for large files.

    For JSONL files with nested columns (STRUCT / ARRAY):
      - STRUCT fields are represented as Python dicts in the DataFrame.
      - ARRAY fields are represented as Python lists in the DataFrame.
      - Scalar validators (aggregate, distribution) should only target
        scalar columns; nested columns are detected and skipped gracefully.
    """

    def __init__(self, file_path: str):
        """
        Initialise the reader and load the file into memory.

        Args:
            file_path: Local path or GCS URI (gs://...) to the source file.

        Raises:
            FileNotFoundError: If a local file does not exist.
            ValueError: If the file cannot be parsed.
        """
        self._original_path = file_path
        self._tmp_dir: Optional[tempfile.TemporaryDirectory] = None

        # Resolve to a local path (downloading from GCS if necessary)
        local_path = self._resolve_local_path(file_path)

        logger.info("Loading file: %s", local_path)
        try:
            ext = Path(local_path).suffix.lower()
            if ext in (".jsonl", ".ndjson"):
                # Newline-delimited JSON — each line is one JSON object
                self._df: pd.DataFrame = pd.read_json(
                    local_path,
                    lines=True,
                )
                logger.info("Detected format: JSONL (newline-delimited JSON)")
            elif ext == ".json":
                if _is_jsonlines(local_path):
                    self._df = pd.read_json(local_path, lines=True)
                    logger.info("Detected format: JSONL (newline-delimited JSON, .json ext)")
                else:
                    self._df = pd.read_json(local_path)
                    logger.info("Detected format: JSON array")
            else:
                # Default: CSV
                self._df = pd.read_csv(
                    local_path,
                    low_memory=False,     # Avoid mixed-type inference warnings
                    keep_default_na=True,
                )
                logger.info("Detected format: CSV")
        except Exception as exc:
            raise ValueError(
                f"Failed to parse file '{local_path}': {exc}"
            ) from exc

        logger.info(
            "File loaded — rows=%d, columns=%d", len(self._df), len(self._df.columns)
        )

    # ------------------------------------------------------------------
    # Private: path resolution
    # ------------------------------------------------------------------

    def _resolve_local_path(self, file_path: str) -> str:
        """
        Return a guaranteed-local path, downloading from GCS if needed.

        Args:
            file_path: Original path or GCS URI.

        Returns:
            Absolute local filesystem path.
        """
        if _is_gcs_path(file_path):
            # Create a managed temp directory; cleaned up on __del__
            self._tmp_dir = tempfile.TemporaryDirectory(prefix="bq_validator_")
            return _download_from_gcs(file_path, self._tmp_dir.name)

        # Local path validation
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return str(path.resolve())

    def __del__(self):
        """Clean up any temporary directory created for GCS downloads."""
        if self._tmp_dir is not None:
            try:
                self._tmp_dir.cleanup()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Basic properties
    # ------------------------------------------------------------------

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return the full DataFrame (read-only view)."""
        return self._df

    @property
    def columns(self) -> List[str]:
        """Return the list of column names from the file."""
        return list(self._df.columns)

    @property
    def row_count(self) -> int:
        """Return the number of data rows in the file."""
        return len(self._df)

    def is_scalar_column(self, column: str) -> bool:
        """
        Return True if a column contains only scalar (non-nested) values.

        Columns backed by dicts (STRUCT) or lists (ARRAY) from JSONL files
        are considered non-scalar and are excluded from numeric validators.

        Args:
            column: Column name.

        Returns:
            True if all non-null values are scalar (int, float, str, bool).
        """
        if column not in self._df.columns:
            return False
        non_null = self._df[column].dropna()
        if non_null.empty:
            return True
        first_val = non_null.iloc[0]
        return not isinstance(first_val, (dict, list))

    # ------------------------------------------------------------------
    # Aggregate helpers (file-side)
    # ------------------------------------------------------------------

    def compute_aggregate(self, column: str, function: str) -> Optional[float]:
        """
        Compute a single aggregate function on a file column.

        Supported functions: sum, min, max, avg, distinct_count.

        Args:
            column:   Column name.
            function: Aggregate function name.

        Returns:
            Computed value as a float, or None if the column is empty/all-null.

        Raises:
            KeyError:   If the column does not exist in the file.
            ValueError: If the function is not supported.
        """
        if column not in self._df.columns:
            raise KeyError(f"Column '{column}' not found in file.")

        series = (
            pd.to_numeric(self._df[column], errors="coerce")
            if function in ("sum", "avg")
            else self._df[column]
        )

        if function == "sum":
            return float(series.sum())
        elif function == "min":
            return series.min()
        elif function == "max":
            return series.max()
        elif function == "avg":
            return float(series.mean()) if not series.isna().all() else None
        elif function == "distinct_count":
            return int(series.nunique())
        else:
            raise ValueError(f"Unsupported aggregate function: {function}")

    # ------------------------------------------------------------------
    # Primary key helpers
    # ------------------------------------------------------------------

    def get_duplicate_pk_count(self, primary_keys: List[str]) -> int:
        """
        Count rows where the primary key combination is duplicated.

        Args:
            primary_keys: List of column names forming the primary key.

        Returns:
            Number of duplicate rows (rows beyond the first occurrence).
        """
        missing = [k for k in primary_keys if k not in self._df.columns]
        if missing:
            raise KeyError(f"Primary key column(s) not found in file: {missing}")

        duplicated = self._df.duplicated(subset=primary_keys, keep="first")
        return int(duplicated.sum())

    def get_null_pk_count(self, primary_keys: List[str]) -> int:
        """
        Count rows where any primary key column is null/NaN.

        Args:
            primary_keys: List of PK column names.

        Returns:
            Number of rows with at least one null PK value.
        """
        missing = [k for k in primary_keys if k not in self._df.columns]
        if missing:
            raise KeyError(f"Primary key column(s) not found in file: {missing}")

        mask = self._df[primary_keys].isnull().any(axis=1)
        return int(mask.sum())

    # ------------------------------------------------------------------
    # Partition helpers
    # ------------------------------------------------------------------

    def get_partition_row_counts(self, partition_column: str) -> Dict[str, int]:
        """
        Return a dict mapping each distinct partition value to its row count.

        Args:
            partition_column: Column name used for partitioning.

        Returns:
            Dict: {partition_value_str → row_count}.
        """
        if partition_column not in self._df.columns:
            raise KeyError(
                f"Partition column '{partition_column}' not found in file."
            )

        counts = self._df.groupby(partition_column).size()
        return {str(k): int(v) for k, v in counts.items()}

    # ------------------------------------------------------------------
    # Hash helpers
    # ------------------------------------------------------------------

    def compute_row_hash(self, columns: Optional[List[str]] = None) -> int:
        """
        Compute a simple XOR-based hash across all rows using pandas.

        For JSONL files with nested columns (dict/list values), each cell
        is serialised to a string before hashing.

        Args:
            columns: Ordered list of columns to include.  Defaults to all.

        Returns:
            Integer XOR hash aggregate.
        """
        cols = columns if columns else list(self._df.columns)
        subset = self._df[cols].astype(str)

        row_hashes = subset.apply(
            lambda row: hash("|".join(row.values)), axis=1
        )
        result = 0
        for h in row_hashes:
            result ^= int(h) & 0xFFFFFFFFFFFFFFFF  # keep 64-bit unsigned
        return result

    # ------------------------------------------------------------------
    # Random sampling
    # ------------------------------------------------------------------

    def get_random_sample(
        self,
        n: int,
        primary_keys: List[str],
        random_state: int = 42,
    ) -> List[Dict[str, Any]]:
        """
        Randomly select up to n rows from the file.

        Args:
            n:            Number of rows to sample.
            primary_keys: PK column names (must be present for BQ lookup).
            random_state: Seed for reproducibility.

        Returns:
            List of row dicts (all columns).
        """
        sample_size = min(n, len(self._df))
        sample_df = self._df.sample(n=sample_size, random_state=random_state)
        sample_df = sample_df.where(pd.notna(sample_df), other=None)
        return sample_df.to_dict(orient="records")

    # ------------------------------------------------------------------
    # Distribution helpers
    # ------------------------------------------------------------------

    def compute_column_distribution(self, column: str) -> Dict[str, Any]:
        """
        Compute distribution statistics for a file column.

        Args:
            column: Column name.

        Returns:
            Dict with keys: min_val, max_val, avg_val, stddev_val,
                            null_count, total_count.
        """
        if column not in self._df.columns:
            raise KeyError(f"Column '{column}' not found in file.")

        numeric = pd.to_numeric(self._df[column], errors="coerce")
        return {
            "min_val":     float(numeric.min()) if not numeric.isna().all() else None,
            "max_val":     float(numeric.max()) if not numeric.isna().all() else None,
            "avg_val":     float(numeric.mean()) if not numeric.isna().all() else None,
            "stddev_val":  float(numeric.std()) if not numeric.isna().all() else None,
            "null_count":  int(self._df[column].isnull().sum()),
            "total_count": len(self._df),
        }

    # ------------------------------------------------------------------
    # Null count helpers
    # ------------------------------------------------------------------

    def get_column_null_counts(
        self, columns: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Count null / missing values per column in the file.

        A value is treated as null if it is Python ``None``, pandas ``NaN``
        (``float('nan')``), or an empty string ``""``.

        Handles scalar, STRUCT (dict), and ARRAY (list) column types from
        JSONL files without special-casing the type.

        Args:
            columns: Column names to inspect.
                     If ``None`` (default), all file columns are checked.

        Returns:
            Dict mapping column_name → null count.
            Returns -1 for any column not found in the file.
        """
        df = self._df
        cols: List[str] = columns if columns is not None else list(df.columns)
        result: Dict[str, int] = {}
        for col in cols:
            if col not in df.columns:
                logger.warning(
                    "Column '%s' not found in file — null count set to -1", col
                )
                result[col] = -1
                continue
            count = 0
            for val in df[col]:
                if val is None:
                    count += 1
                elif isinstance(val, float) and pd.isna(val):
                    count += 1
                elif isinstance(val, str) and val == "":
                    count += 1
            result[col] = count
        return result

    # ------------------------------------------------------------------
    # Column-level checksum helpers
    # ------------------------------------------------------------------

    def compute_column_checksums(
        self, columns: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Compute an XOR hash aggregate per column over all rows in the file.

        Each cell value is normalised to a JSON string before hashing so that
        dicts (STRUCT) and lists (ARRAY) are handled consistently.

        The XOR of Python ``hash()`` over all cells gives a single integer
        fingerprint per column.  Because Python's ``hash()`` and BigQuery's
        ``FARM_FINGERPRINT`` use different algorithms the absolute values will
        differ; the check is therefore non-zero on both sides (data present).

        Args:
            columns: Column names to hash.
                     If ``None`` (default), all file columns are hashed.

        Returns:
            Dict mapping column_name → XOR hash integer.
            Returns -1 for any column not found in the file.
            Returns 0 if a column contains only null values.
        """
        import json as _json

        df = self._df
        cols: List[str] = columns if columns is not None else list(df.columns)
        result: Dict[str, int] = {}
        for col in cols:
            if col not in df.columns:
                logger.warning(
                    "Column '%s' not found in file — checksum set to -1", col
                )
                result[col] = -1
                continue
            h = 0
            for val in df[col]:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    cell_str = "null"
                elif isinstance(val, (dict, list)):
                    cell_str = _json.dumps(val, sort_keys=True, default=str)
                else:
                    cell_str = str(val)
                h ^= hash(cell_str)
            result[col] = h
        return result
