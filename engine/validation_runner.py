"""
validation_runner.py
--------------------
Orchestrates all validation layers in order, driven entirely by the config.

Responsibilities:
  1. Initialise core components (BigQueryClient, FileReader).
  2. Fetch the BigQuery schema once and share it across validators.
  3. Execute each enabled validation layer in sequence.
  4. Flatten results (some validators return lists of dicts).
  5. Delegate output formatting to result_formatter.

No table-specific logic lives here — everything is driven by the config.
"""

import logging
import uuid
from typing import Any, Dict, List

from core.bigquery_client import BigQueryClient
from core.config_loader import ValidationConfig
from core.file_reader import FileReader
from engine.result_formatter import current_utc_iso, format_output
from validators import (
    aggregate_validator,
    column_checksum_validator,
    datatype_validator,
    distribution_validator,
    duplicate_row_validator,
    enum_validator,
    hash_validator,
    json_schema_validator,
    non_negative_validator,
    null_validator,
    partition_validator,
    primary_key_validator,
    random_sample_validator,
    range_validator,
    regex_validator,
    row_count_validator,
    schema_validator,
)

logger = logging.getLogger(__name__)


def run(config: ValidationConfig) -> Dict[str, Any]:
    """
    Execute the full validation suite as defined by the config.

    Args:
        config: A validated ValidationConfig instance.

    Returns:
        The final formatted output dict (ready for JSON serialisation).
    """
    run_id = str(uuid.uuid4())
    run_start = current_utc_iso()
    all_results: List[Dict[str, Any]] = []

    logger.info("=" * 60)
    logger.info("Validation run started — run_id=%s", run_id)
    logger.info("Target: %s.%s", config.dataset, config.table)
    logger.info("File:   %s", config.file_path)
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Initialise shared components
    # ------------------------------------------------------------------
    bq_client = BigQueryClient(project=config.project)
    file_reader = FileReader(config.file_path, file_format=config.file_format)

    # Fetch BQ schema once — shared by schema validator and others
    logger.info("Fetching BigQuery table schema...")
    bq_schema = bq_client.get_table_schema(config.dataset, config.table)

    layers = config.validation_layers

    # ------------------------------------------------------------------
    # Layer 1: Schema / Metadata Validation
    # ------------------------------------------------------------------
    if layers.metadata_validation:
        logger.info("[1/8] Running schema (metadata) validation...")
        result = schema_validator.validate(file_reader, bq_schema, config)
        all_results.append(result)
    else:
        logger.info("[1/8] Schema validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 2: Row Count Validation
    # ------------------------------------------------------------------
    if layers.row_count_validation:
        logger.info("[2/8] Running row count validation...")
        result = row_count_validator.validate(file_reader, bq_client, config)
        all_results.append(result)
    else:
        logger.info("[2/8] Row count validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 3: Primary Key Validation
    # ------------------------------------------------------------------
    if layers.primary_key_uniqueness:
        logger.info("[3/8] Running primary key validation...")
        pk_results = primary_key_validator.validate(file_reader, bq_client, config)
        all_results.extend(_ensure_list(pk_results))
    else:
        logger.info("[3/8] Primary key validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 4: Aggregate Validation
    # ------------------------------------------------------------------
    if layers.aggregate_validation:
        logger.info("[4/8] Running aggregate validation...")
        agg_results = aggregate_validator.validate(file_reader, bq_client, config)
        all_results.extend(_ensure_list(agg_results))
    else:
        logger.info("[4/8] Aggregate validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 5: Partition Validation
    # ------------------------------------------------------------------
    if layers.partition_validation:
        logger.info("[5/8] Running partition validation...")
        part_results = partition_validator.validate(file_reader, bq_client, config)
        all_results.extend(_ensure_list(part_results))
    else:
        logger.info("[5/8] Partition validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 6: Hash Validation
    # ------------------------------------------------------------------
    if layers.hash_validation:
        logger.info("[6/8] Running hash validation...")
        result = hash_validator.validate(file_reader, bq_client, config)
        all_results.append(result)
    else:
        logger.info("[6/8] Hash validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 7: Random Sampling Validation
    # ------------------------------------------------------------------
    if layers.random_sampling:
        logger.info("[7/8] Running random sampling validation...")
        result = random_sample_validator.validate(file_reader, bq_client, config)
        all_results.append(result)
    else:
        logger.info("[7/8] Random sampling validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 8: Column Distribution Validation
    # ------------------------------------------------------------------
    if layers.column_distribution:
        logger.info("[8/10] Running column distribution validation...")
        dist_results = distribution_validator.validate(file_reader, bq_client, config)
        all_results.extend(_ensure_list(dist_results))
    else:
        logger.info("[8/10] Column distribution validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 9: Null Value Validation
    # ------------------------------------------------------------------
    if layers.null_validation:
        logger.info("[9/10] Running null value validation...")
        null_results = null_validator.validate(file_reader, bq_client, config)
        all_results.extend(_ensure_list(null_results))
    else:
        logger.info("[9/10] Null value validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 10: Column-Level Checksum Validation
    # ------------------------------------------------------------------
    if layers.column_checksum:
        logger.info("[10/10] Running column-level checksum validation...")
        chk_results = column_checksum_validator.validate(file_reader, bq_client, config)
        all_results.extend(_ensure_list(chk_results))
    else:
        logger.info("[10/10] Column checksum validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 11: Data Type Validation (file-side)
    # ------------------------------------------------------------------
    if layers.datatype_validation:
        logger.info("[11/17] Running data type validation...")
        dt_results = datatype_validator.validate(file_reader, config)
        all_results.extend(_ensure_list(dt_results))
    else:
        logger.info("[11/17] Data type validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 12: Enum Validation (file-side)
    # ------------------------------------------------------------------
    if layers.enum_validation:
        logger.info("[12/17] Running enum validation...")
        enum_results = enum_validator.validate(file_reader, config)
        all_results.extend(_ensure_list(enum_results))
    else:
        logger.info("[12/17] Enum validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 13: Range Validation (file-side)
    # ------------------------------------------------------------------
    if layers.range_validation:
        logger.info("[13/17] Running range validation...")
        range_results = range_validator.validate(file_reader, config)
        all_results.extend(_ensure_list(range_results))
    else:
        logger.info("[13/17] Range validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 14: Regex Validation (file-side)
    # ------------------------------------------------------------------
    if layers.regex_validation:
        logger.info("[14/17] Running regex validation...")
        regex_results = regex_validator.validate(file_reader, config)
        all_results.extend(_ensure_list(regex_results))
    else:
        logger.info("[14/17] Regex validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 15: Duplicate Row Validation (file-side)
    # ------------------------------------------------------------------
    if layers.duplicate_row_validation:
        logger.info("[15/17] Running duplicate row validation...")
        dup_result = duplicate_row_validator.validate(file_reader, config)
        all_results.extend(_ensure_list(dup_result))
    else:
        logger.info("[15/17] Duplicate row validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 16: JSON Schema Validation (file-side)
    # ------------------------------------------------------------------
    if layers.json_schema_validation:
        logger.info("[16/17] Running JSON schema validation...")
        js_results = json_schema_validator.validate(file_reader, config)
        all_results.extend(_ensure_list(js_results))
    else:
        logger.info("[16/17] JSON schema validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Layer 17: Non-Negative Validation (file-side)
    # ------------------------------------------------------------------
    if layers.non_negative_validation:
        logger.info("[17/17] Running non-negative validation...")
        nn_results = non_negative_validator.validate(file_reader, config)
        all_results.extend(_ensure_list(nn_results))
    else:
        logger.info("[17/17] Non-negative validation SKIPPED (disabled in config)")

    # ------------------------------------------------------------------
    # Format and return final output
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("All validation layers complete — formatting output...")

    output = format_output(
        run_id=run_id,
        config=config,
        results=all_results,
        run_start_iso=run_start,
    )

    return output


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _ensure_list(value: Any) -> List[Dict[str, Any]]:
    """
    Ensure a validator result is always a list of dicts.

    Some validators return a single dict; others return a list.
    This normalises them so all_results.extend() always works.

    Args:
        value: A dict or list of dicts.

    Returns:
        A list of dicts.
    """
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return value
    logger.warning("Unexpected validator return type: %s — wrapping in list", type(value))
    return [value]