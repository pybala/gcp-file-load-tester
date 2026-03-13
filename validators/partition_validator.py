"""
partition_validator.py
----------------------
Validates data integrity at the partition level by comparing per-partition
row counts between the source CSV file and BigQuery.

This validator is ONLY executed when partition.enabled == true in the config.
It groups both the file data and BQ data by the partition column, then
compares row counts partition by partition.
"""

import logging
import time
from typing import Any, Dict, List

from core.bigquery_client import BigQueryClient
from core.file_reader import FileReader

logger = logging.getLogger(__name__)


def validate(
    file_reader: FileReader,
    bq_client: BigQueryClient,
    config: Any,
) -> List[Dict[str, Any]]:
    """
    Run partition-level row count validation.

    Args:
        file_reader: Loaded FileReader instance.
        bq_client:   BigQueryClient instance.
        config:      ValidationConfig — uses config.partition,
                     config.dataset, config.table.

    Returns:
        List of result dicts (one summary + one per mismatched partition).
    """
    # Guard: only run if partition is enabled
    if not config.partition.enabled:
        logger.info("Partition validation skipped — partition.enabled is false.")
        return [
            {
                "test_name": "partition_validation",
                "status": "SKIPPED",
                "expected": None,
                "actual": None,
                "details": {"reason": "partition.enabled is false in config"},
                "execution_time_ms": 0.0,
            }
        ]

    start = time.perf_counter()
    test_name = "partition_validation"
    partition_col = config.partition.column

    try:
        # ---------------------------------------------------------------
        # 1. Get file-side partition counts
        # ---------------------------------------------------------------
        file_partition_counts = file_reader.get_partition_row_counts(partition_col)
        logger.info(
            "File partitions found: %d distinct values in '%s'",
            len(file_partition_counts),
            partition_col,
        )

        # ---------------------------------------------------------------
        # 2. Get BQ-side partition counts
        # ---------------------------------------------------------------
        bq_rows = bq_client.get_partition_aggregates(
            dataset=config.dataset,
            table=config.table,
            partition_column=partition_col,
            agg_column="row_count",
            agg_function="COUNT(*)",
        )
        bq_partition_counts = {
            str(row[partition_col]): int(row["row_count"]) for row in bq_rows
        }
        logger.info(
            "BQ partitions found: %d distinct values in '%s'",
            len(bq_partition_counts),
            partition_col,
        )

        # ---------------------------------------------------------------
        # 3. Compare partition sets and counts
        # ---------------------------------------------------------------
        all_partitions = sorted(
            set(file_partition_counts.keys()) | set(bq_partition_counts.keys())
        )

        mismatches: List[Dict[str, Any]] = []
        missing_in_bq: List[str] = []
        missing_in_file: List[str] = []
        matching: List[str] = []

        for partition_val in all_partitions:
            file_cnt = file_partition_counts.get(partition_val)
            bq_cnt = bq_partition_counts.get(partition_val)

            if file_cnt is None:
                missing_in_file.append(partition_val)
                mismatches.append({
                    "partition": partition_val,
                    "file_count": None,
                    "bq_count": bq_cnt,
                    "issue": "Partition exists in BQ but not in file",
                })
            elif bq_cnt is None:
                missing_in_bq.append(partition_val)
                mismatches.append({
                    "partition": partition_val,
                    "file_count": file_cnt,
                    "bq_count": None,
                    "issue": "Partition exists in file but not in BQ",
                })
            elif file_cnt != bq_cnt:
                mismatches.append({
                    "partition": partition_val,
                    "file_count": file_cnt,
                    "bq_count": bq_cnt,
                    "delta": bq_cnt - file_cnt,
                    "issue": "Row count mismatch",
                })
            else:
                matching.append(partition_val)

        status = "PASS" if not mismatches else "FAIL"
        elapsed_ms = (time.perf_counter() - start) * 1000

        details: Dict[str, Any] = {
            "partition_column": partition_col,
            "partition_type": config.partition.type,
            "total_partitions_checked": len(all_partitions),
            "matching_partitions": len(matching),
            "mismatched_partitions": len(mismatches),
            "missing_in_bq": missing_in_bq,
            "missing_in_file": missing_in_file,
            "mismatches": mismatches,
        }

        if status == "PASS":
            logger.info(
                "Partition validation PASSED — %d partitions matched", len(matching)
            )
        else:
            logger.warning(
                "Partition validation FAILED — %d mismatch(es) out of %d partitions",
                len(mismatches),
                len(all_partitions),
            )

        return [
            {
                "test_name": test_name,
                "status": status,
                "expected": len(all_partitions),
                "actual": len(matching),
                "details": details,
                "execution_time_ms": round(elapsed_ms, 3),
            }
        ]

    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error("Partition validation error: %s", exc, exc_info=True)
        return [
            {
                "test_name": test_name,
                "status": "ERROR",
                "expected": None,
                "actual": None,
                "details": {"error": str(exc)},
                "execution_time_ms": round(elapsed_ms, 3),
            }
        ]