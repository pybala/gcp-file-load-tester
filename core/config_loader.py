"""
config_loader.py
----------------
Loads and validates the YAML configuration file for the data validation framework.
Uses Pydantic v2 for schema validation and type coercion.

Config sources supported:
  1. Local filesystem path  — /path/to/config.yaml
  2. GCS URI               — gs://bucket/path/config.yaml
  3. Python dict           — load_config_from_dict(data)  (used by Cloud Function)
"""

import logging
import tempfile
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic models — one per config section
# ---------------------------------------------------------------------------

class FileFormatConfig(BaseModel):
    """
    File format settings controlling how the source file is parsed.

    All fields are optional — absent fields fall back to the documented
    defaults.  The entire section may be omitted from the YAML; when it is,
    file_reader.py auto-detects the format from the file extension.
    """

    # File type — overrides extension-based auto-detection when set.
    # Allowed: "csv" | "json" | "jsonl"
    file_type: Optional[Literal["csv", "json", "jsonl"]] = None

    # ── CSV options ──────────────────────────────────────────────────────────

    # Field separator character (default: comma).
    # Use "\t" for tab-delimited files.
    delimiter: str = ","

    # Quote / enclosure character (default: double-quote).
    enclosed_by: str = '"'

    # Escape character (default: None → enclosed_by doubling).
    escape_char: Optional[str] = None

    # True  → first data row is a header (default behaviour).
    # False → no header; column names will be integer indices.
    has_header: bool = True

    # Rows to skip at the very top of the file BEFORE the header row.
    # Must be >= 0.
    skip_rows: int = Field(default=0, ge=0)

    # Additional strings to interpret as null / missing.
    # pandas' own built-in NA list is always applied; this extends it.
    null_values: List[str] = Field(default_factory=list)

    # ── General options ──────────────────────────────────────────────────────

    # File encoding (default: utf-8).
    encoding: str = "utf-8"

    @field_validator("delimiter", "enclosed_by")
    @classmethod
    def _single_char_or_special(cls, v: str) -> str:
        """Allow single-char values or common escape sequences like \\t."""
        decoded = v.encode("raw_unicode_escape").decode("unicode_escape")
        return decoded

    model_config = {"populate_by_name": True, "extra": "ignore"}


class PartitionConfig(BaseModel):
    """Partition settings for partition-aware validation."""
    enabled: bool = False
    column: Optional[str] = None
    type: Optional[Literal["DATE", "TIMESTAMP"]] = None

    @model_validator(mode="after")
    def check_partition_fields(self) -> "PartitionConfig":
        if self.enabled:
            if not self.column:
                raise ValueError("partition.column is required when partition.enabled is true")
            if not self.type:
                raise ValueError("partition.type is required when partition.enabled is true")
        return self


class ValidationLayers(BaseModel):
    """Toggleable flags for each validation layer."""
    metadata_validation: bool = True
    row_count_validation: bool = True
    primary_key_uniqueness: bool = True
    aggregate_validation: bool = True
    partition_validation: bool = True
    hash_validation: bool = True
    random_sampling: bool = True
    column_distribution: bool = True
    null_validation: bool = True
    column_checksum: bool = True
    # Extended validation layers (file-side quality checks)
    datatype_validation: bool = True
    enum_validation: bool = True
    range_validation: bool = True
    regex_validation: bool = True
    duplicate_row_validation: bool = True
    json_schema_validation: bool = True
    non_negative_validation: bool = True


class AggregateColumnConfig(BaseModel):
    """Per-column aggregate function specification."""
    column: str
    functions: List[Literal["sum", "min", "max", "avg", "distinct_count"]] = Field(
        default_factory=lambda: ["sum", "min", "max", "avg"]
    )
    # NOTE: "count" is NOT a supported function — use "distinct_count" instead.


class DataTypeColumnConfig(BaseModel):
    """Per-column data type declaration for datatype_validation."""
    column: str
    expected_type: Literal["integer", "float", "string", "boolean", "date", "timestamp"]


class EnumColumnConfig(BaseModel):
    """Per-column allowed values declaration for enum_validation."""
    column: str
    allowed_values: List[Any]


class RangeColumnConfig(BaseModel):
    """Per-column numeric range bounds for range_validation."""
    column: str
    min: Optional[float] = None
    max: Optional[float] = None

    @model_validator(mode="after")
    def check_at_least_one_bound(self) -> "RangeColumnConfig":
        if self.min is None and self.max is None:
            raise ValueError(
                f"range_columns entry for '{self.column}' must specify at least one of: min, max"
            )
        return self


class RegexColumnConfig(BaseModel):
    """Per-column regex pattern for regex_validation."""
    column: str
    pattern: str


class JsonSchemaColumnConfig(BaseModel):
    """Per-column required JSON keys for json_schema_validation."""
    column: str
    required_keys: List[str]


class ValidationConfig(BaseModel):
    """Root configuration model for the validation framework."""
    project: Optional[str] = None                    # GCP project ID (optional)
    dataset: str                                      # BigQuery dataset name
    table: str                                        # BigQuery table name
    file_path: str                                    # Local path or gs:// URI
    file_format: FileFormatConfig = Field(           # File format / parsing options
        default_factory=FileFormatConfig
    )
    primary_keys: List[str] = Field(default_factory=list)
    partition: PartitionConfig = Field(default_factory=PartitionConfig)
    random_sample_size: int = Field(default=100, ge=1)
    validation_layers: ValidationLayers = Field(default_factory=ValidationLayers)
    aggregate_columns: List[AggregateColumnConfig] = Field(default_factory=list)
    distribution_columns: List[str] = Field(default_factory=list)
    null_check_columns: List[str] = Field(
        default_factory=list,
        description=(
            "Columns to check for NULL values. "
            "Leave empty to check ALL columns from the source file."
        ),
    )
    column_checksum_columns: List[str] = Field(
        default_factory=list,
        description=(
            "Columns to include in per-column checksum validation. "
            "Leave empty to checksum ALL columns from the source file."
        ),
    )
    # Extended validation config fields
    datatype_columns: List[DataTypeColumnConfig] = Field(
        default_factory=list,
        description="Per-column expected data type declarations for datatype_validation.",
    )
    enum_columns: List[EnumColumnConfig] = Field(
        default_factory=list,
        description="Per-column allowed value sets for enum_validation.",
    )
    range_columns: List[RangeColumnConfig] = Field(
        default_factory=list,
        description="Per-column numeric range bounds (min/max) for range_validation.",
    )
    regex_columns: List[RegexColumnConfig] = Field(
        default_factory=list,
        description="Per-column regex patterns for regex_validation.",
    )
    json_schema_columns: List[JsonSchemaColumnConfig] = Field(
        default_factory=list,
        description="Per-column required JSON key declarations for json_schema_validation.",
    )
    non_negative_columns: List[str] = Field(
        default_factory=list,
        description="Columns that must contain only non-negative numeric values.",
    )

    # Populated by load_config() — not read from the YAML file itself
    config_path: Optional[str] = Field(
        default=None,
        description="File path or GCS URI used to load this config (set at runtime).",
        exclude=True,
    )
    config_yaml: Optional[str] = Field(
        default=None,
        description="Raw YAML text of this config file (set at runtime).",
        exclude=True,
    )

    model_config = {"populate_by_name": True, "extra": "ignore"}

    @model_validator(mode="after")
    def check_primary_keys_for_random_sampling(self) -> "ValidationConfig":
        """Random sampling requires primary keys to fetch matching BQ rows."""
        if self.validation_layers.random_sampling and not self.primary_keys:
            logger.warning(
                "random_sampling is enabled but no primary_keys defined — "
                "random sampling will be skipped."
            )
        return self


# ---------------------------------------------------------------------------
# Public loader functions
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> ValidationConfig:
    """
    Load and parse a YAML configuration file.

    Supports:
      - Local file paths:  /path/to/config.yaml
      - GCS URIs:          gs://bucket/path/config.yaml

    Args:
        config_path: Path or GCS URI to the YAML config file.

    Returns:
        A validated ValidationConfig instance.

    Raises:
        FileNotFoundError: If a local config file does not exist.
        ValueError: If the config fails Pydantic validation.
    """
    logger.info("Loading configuration from: %s", config_path)

    raw_yaml_text: Optional[str] = None

    if config_path.startswith("gs://"):
        raw, raw_yaml_text = _load_yaml_from_gcs(config_path)
    else:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        with open(path, "r", encoding="utf-8") as fh:
            raw_yaml_text = fh.read()
        raw = yaml.safe_load(raw_yaml_text)

    config = _parse_config(raw)
    config.config_path = config_path
    config.config_yaml = raw_yaml_text
    return config


def load_config_from_dict(data: Dict[str, Any]) -> ValidationConfig:
    """
    Build a ValidationConfig directly from a Python dictionary.

    Used by the Cloud Function handler when config is passed inline
    in the HTTP request body.

    Args:
        data: Dictionary matching the YAML config schema.

    Returns:
        A validated ValidationConfig instance.
    """
    logger.info("Loading configuration from inline dict")
    return _parse_config(data)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _parse_config(raw: Any) -> ValidationConfig:
    """Validate and construct a ValidationConfig from a raw dict."""
    if not isinstance(raw, dict):
        raise ValueError(
            f"Configuration must be a YAML/JSON mapping, got: {type(raw)}"
        )
    config = ValidationConfig(**raw)
    logger.info(
        "Configuration parsed — dataset=%s, table=%s, active_layers=%s",
        config.dataset,
        config.table,
        [k for k, v in config.validation_layers.model_dump().items() if v],
    )
    return config


def _load_yaml_from_gcs(gcs_uri: str):
    """
    Download a YAML config file from GCS and parse it.

    Args:
        gcs_uri: GCS URI in the form gs://bucket/path/config.yaml.

    Returns:
        Tuple of (parsed YAML dict, raw YAML text string).

    Raises:
        ImportError: If google-cloud-storage is not installed.
    """
    try:
        from google.cloud import storage as gcs
    except ImportError as exc:
        raise ImportError(
            "google-cloud-storage is required for GCS config support. "
            "Install it with: pip install google-cloud-storage"
        ) from exc

    without_prefix = gcs_uri[len("gs://"):]
    bucket_name, _, blob_name = without_prefix.partition("/")

    logger.info("Downloading config from GCS: %s", gcs_uri)
    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    yaml_content = blob.download_as_text(encoding="utf-8")
    raw = yaml.safe_load(yaml_content)
    logger.info("Config downloaded and parsed from GCS: %s", gcs_uri)
    return raw, yaml_content
