"""
main.py
-------
Dual-mode entry point for the BigQuery Data Validation Framework.

─────────────────────────────────────────────────────────────────────
MODE 1 — Local CLI
─────────────────────────────────────────────────────────────────────
Run directly from the command line:

    python main.py --config config/validation_config.yaml
    python main.py --config config/validation_config.yaml --output results.json
    python main.py --config gs://my-bucket/configs/validation_config.yaml

Exit codes:
    0 — PASS / WARN / SKIPPED
    1 — FAIL / ERROR (validation failures)
    2 — Framework-level error (bad config, file not found, BQ auth error)

─────────────────────────────────────────────────────────────────────
MODE 2 — Google Cloud Function (HTTP trigger)
─────────────────────────────────────────────────────────────────────
Deployed as a Cloud Function with entry point `bq_validate`.

Deploy command:
    gcloud functions deploy bq-data-validator \\
        --gen2 \\
        --runtime python311 \\
        --region us-central1 \\
        --source . \\
        --entry-point bq_validate \\
        --trigger-http \\
        --allow-unauthenticated \\
        --memory 512MB \\
        --timeout 540s

HTTP Request body (JSON) -- choose ONE of:
    { "config_path": "gs://my-bucket/configs/validation_config.yaml" }
    { "config_path": "/tmp/validation_config.yaml" }
    { "config": { ...full config dict... } }

Optional body keys:
    "gcs_output":        "gs://bucket/path/results.json"
    "metadata_project":  "my-gcp-project"   (default: data-test-automation-489413)
    "metadata_dataset":  "validation_ds"     (default: validation_ds)
    "skip_metadata":     true                (default: false)

HTTP Response: JSON with the full validation results.

─────────────────────────────────────────────────────────────────────
BigQuery Metadata Tables
─────────────────────────────────────────────────────────────────────
After every successful run the framework writes metadata to three
BigQuery tables in the project data-test-automation-489413.validation_ds:

  validation_configs -- one row per unique config file (stable config_id)
  validation_runs    -- one row per validation execution
  validation_tests   -- one row per individual test result

Override the metadata destination via CLI flags or environment variables:
    --metadata-project / METADATA_PROJECT
    --metadata-dataset / METADATA_DATASET

Pass --no-metadata (CLI) or "skip_metadata": true (HTTP) to disable.
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Logging -- configured once at module level so both modes share the same setup
# ---------------------------------------------------------------------------


def _configure_logging(level: str = "INFO") -> None:
    """Configure the root logger with a consistent format."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,  # Logs -> stderr; JSON output -> stdout
    )


# Apply a sensible default at import time (Cloud Function path)
_configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

logger = logging.getLogger(__name__)


# ===========================================================================
# GCS upload helper
# ===========================================================================


def _upload_to_gcs(json_str: str, gcs_path: str) -> str:
    """
    Upload a JSON string to a GCS path.

    Args:
        json_str:  The JSON content to upload.
        gcs_path:  Destination GCS URI (gs://bucket/path/file.json).

    Returns:
        The GCS URI on success.

    Raises:
        ValueError: If the GCS path is malformed.
    """
    from google.cloud import storage as gcs

    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    without_prefix = gcs_path[len("gs://"):]
    bucket_name, _, blob_name = without_prefix.partition("/")

    client = gcs.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(json_str, content_type="application/json")
    logger.info("Results uploaded to GCS: %s", gcs_path)
    return gcs_path


# ===========================================================================
# Metadata write helper
# ===========================================================================


def _write_metadata(
    config,
    output: dict,
    *,
    metadata_project: Optional[str] = None,
    metadata_dataset: Optional[str] = None,
    gcs_result_path: Optional[str] = None,
) -> None:
    """
    Write run metadata to the three BigQuery tracking tables.

    This is a best-effort operation: any failure is logged as a warning
    and does not affect the overall exit code / HTTP response.

    Args:
        config:            Parsed ValidationConfig instance.
        output:            Formatted output dict from result_formatter.
        metadata_project:  Override for the metadata BQ project.
        metadata_dataset:  Override for the metadata BQ dataset.
        gcs_result_path:   GCS URI where the full results JSON was uploaded.
    """
    try:
        from core.metadata_writer import MetadataWriter

        writer = MetadataWriter(
            metadata_project=metadata_project,
            metadata_dataset=metadata_dataset,
        )
        writer.write_all(
            config,
            output,
            config_path=getattr(config, "config_path", None),
            gcs_result_path=gcs_result_path,
        )
        logger.info("Metadata written to BigQuery successfully.")
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(
            "Metadata write failed (validation result unaffected): %s",
            exc,
            exc_info=True,
        )


# ===========================================================================
# MODE 2 -- Cloud Function HTTP handler
# ===========================================================================


def bq_validate(request):  # noqa: ANN001
    """
    Google Cloud Function HTTP entry point.

    Accepts a JSON body with either:
      - config_path: str  -> local or GCS path to a YAML config file
      - config:      dict -> inline config (same schema as the YAML file)

    Optional body keys:
      - gcs_output:       str   -> GCS URI to upload result JSON
      - metadata_project: str   -> override metadata BQ project
      - metadata_dataset: str   -> override metadata BQ dataset
      - skip_metadata:    bool  -> set true to skip metadata writes

    Returns a Flask JSON response with the full validation output.
    """
    import flask  # Only imported in Cloud Function context

    # Support CORS preflight
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type",
        }
        return (flask.Response("", 204), 204, headers)

    try:
        body = request.get_json(silent=True) or {}
    except Exception as exc:
        logger.error("Failed to parse request body: %s", exc)
        return flask.jsonify({"error": "Invalid JSON body", "detail": str(exc)}), 400

    # Extract metadata / GCS options before config resolution
    gcs_output = body.get("gcs_output")
    metadata_project = body.get("metadata_project")
    metadata_dataset = body.get("metadata_dataset")
    skip_metadata = bool(body.get("skip_metadata", False))

    try:
        config = _resolve_config_from_request(body)
    except (KeyError, ValueError, FileNotFoundError) as exc:
        logger.error("Config resolution failed: %s", exc)
        return flask.jsonify({"error": "Configuration error", "detail": str(exc)}), 400
    except Exception as exc:
        logger.error("Unexpected config error: %s", exc, exc_info=True)
        return flask.jsonify({"error": "Internal error", "detail": str(exc)}), 500

    try:
        from engine import validation_runner, result_formatter

        output = validation_runner.run(config)
        json_str = result_formatter.to_json(output)
    except Exception as exc:
        logger.error("Validation run failed: %s", exc, exc_info=True)
        return flask.jsonify({"error": "Validation failed", "detail": str(exc)}), 500

    # Upload results to GCS if requested
    gcs_result_path: Optional[str] = None
    if gcs_output:
        try:
            gcs_result_path = _upload_to_gcs(json_str, gcs_output)
        except Exception as exc:
            logger.warning("GCS upload failed: %s", exc, exc_info=True)

    # Write metadata to BigQuery
    if not skip_metadata:
        _write_metadata(
            config,
            output,
            metadata_project=metadata_project,
            metadata_dataset=metadata_dataset,
            gcs_result_path=gcs_result_path,
        )

    overall_status = output.get("overall_status", "ERROR")
    http_status = 200 if overall_status in ("PASS", "WARN", "SKIPPED") else 422

    return flask.Response(
        json_str,
        status=http_status,
        mimetype="application/json",
    )


def _resolve_config_from_request(body: dict):
    """
    Build a ValidationConfig from the Cloud Function request body.

    Accepted body shapes:
      { "config_path": "gs://bucket/config.yaml" }
      { "config_path": "/path/on/mounted/volume/config.yaml" }
      { "config": { ...inline config dict... } }

    Args:
        body: Parsed JSON request body.

    Returns:
        ValidationConfig instance.

    Raises:
        KeyError:  If neither config_path nor config is present.
        ValueError: If the config dict is invalid.
    """
    from core.config_loader import load_config, load_config_from_dict

    if "config_path" in body:
        return load_config(body["config_path"])
    elif "config" in body:
        return load_config_from_dict(body["config"])
    else:
        raise KeyError(
            "Request body must contain either 'config_path' (str) "
            "or 'config' (dict)."
        )


# ===========================================================================
# MODE 1 -- Local CLI
# ===========================================================================


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="bq-data-validator",
        description=(
            "BigQuery Data Validation Framework - "
            "validates CSV/JSONL file data against a BigQuery table."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local config + local CSV
  python main.py --config config/validation_config.yaml

  # Local config + write results to file
  python main.py --config config/validation_config.yaml --output results.json

  # GCS config + upload results to GCS
  python main.py --config gs://my-bucket/configs/validation_config.yaml \\
                 --gcs-output gs://my-bucket/results/run.json

  # Skip BigQuery metadata writes (useful for local dev/testing)
  python main.py --config config/validation_config.yaml --no-metadata

  # Override metadata destination project/dataset
  python main.py --config config/validation_config.yaml \\
                 --metadata-project my-other-project \\
                 --metadata-dataset my_validation_ds

  # Verbose logging
  python main.py --config config/validation_config.yaml --log-level DEBUG
        """,
    )
    parser.add_argument(
        "--config",
        required=True,
        metavar="PATH",
        help="Local path or GCS URI (gs://...) to the YAML validation config.",
    )
    parser.add_argument(
        "--output",
        required=False,
        metavar="PATH",
        default=None,
        help="Optional local path to write the JSON results file.",
    )
    parser.add_argument(
        "--gcs-output",
        required=False,
        metavar="GCS_URI",
        default=None,
        help=(
            "GCS URI to upload the JSON results file "
            "(e.g. gs://my-bucket/results/run.json). "
            "The path is also recorded in validation_runs.gcs_result_path."
        ),
    )
    parser.add_argument(
        "--metadata-project",
        required=False,
        metavar="PROJECT_ID",
        default=None,
        help=(
            "GCP project ID that hosts the validation metadata tables. "
            "Defaults to the METADATA_PROJECT env var or "
            "'data-test-automation-489413'."
        ),
    )
    parser.add_argument(
        "--metadata-dataset",
        required=False,
        metavar="DATASET",
        default=None,
        help=(
            "BigQuery dataset containing the validation metadata tables. "
            "Defaults to the METADATA_DATASET env var or 'validation_ds'."
        ),
    )
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        default=False,
        help=(
            "Skip writing run metadata to BigQuery. "
            "Useful for local development or dry-run testing."
        ),
    )
    parser.add_argument(
        "--log-level",
        required=False,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level (default: INFO).",
    )
    parser.add_argument(
        "--indent",
        required=False,
        type=int,
        default=2,
        help="JSON output indentation (default: 2).",
    )
    return parser


def main() -> int:
    """
    CLI entry point.

    Returns:
        Exit code (0 = success, 1 = validation failures, 2 = framework error).
    """
    parser = _build_parser()
    args = parser.parse_args()

    # Re-configure logging with the user-supplied level
    _configure_logging(args.log_level)

    from core.config_loader import load_config
    from engine import validation_runner, result_formatter

    # ------------------------------------------------------------------
    # 1. Load configuration
    # ------------------------------------------------------------------
    try:
        config = load_config(args.config)
    except FileNotFoundError as exc:
        logger.error("Configuration file not found: %s", exc)
        return 2
    except Exception as exc:
        logger.error("Failed to load configuration: %s", exc, exc_info=True)
        return 2

    # ------------------------------------------------------------------
    # 2. Run validation
    # ------------------------------------------------------------------
    try:
        output = validation_runner.run(config)
    except FileNotFoundError as exc:
        logger.error("Source file not found: %s", exc)
        return 2
    except Exception as exc:
        logger.error("Unexpected error during validation: %s", exc, exc_info=True)
        return 2

    # ------------------------------------------------------------------
    # 3. Serialise and output results
    # ------------------------------------------------------------------
    json_output = result_formatter.to_json(output, indent=args.indent)
    print(json_output)  # Always print to stdout

    if args.output:
        output_path = Path(args.output)
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json_output, encoding="utf-8")
            logger.info("Results written to: %s", args.output)
        except Exception as exc:
            logger.error("Failed to write output file '%s': %s", args.output, exc)

    # ------------------------------------------------------------------
    # 4. Upload results to GCS (optional)
    # ------------------------------------------------------------------
    gcs_result_path = None
    if args.gcs_output:
        try:
            gcs_result_path = _upload_to_gcs(json_output, args.gcs_output)
        except Exception as exc:
            logger.warning("GCS upload failed: %s", exc, exc_info=True)

    # ------------------------------------------------------------------
    # 5. Write metadata to BigQuery
    # ------------------------------------------------------------------
    if not args.no_metadata:
        _write_metadata(
            config,
            output,
            metadata_project=args.metadata_project,
            metadata_dataset=args.metadata_dataset,
            gcs_result_path=gcs_result_path,
        )
    else:
        logger.info("Metadata write skipped (--no-metadata flag set).")

    # ------------------------------------------------------------------
    # 6. Exit code
    # ------------------------------------------------------------------
    overall_status = output.get("overall_status", "ERROR").upper()
    summary = output.get("summary", {})
    logger.info(
        "Run complete -- status=%s | total=%d | passed=%d | failed=%d | errors=%d | skipped=%d",
        overall_status,
        summary.get("total", 0),
        summary.get("passed", 0),
        summary.get("failed", 0),
        summary.get("errors", 0),
        summary.get("skipped", 0),
    )

    return 0 if overall_status in ("PASS", "WARN", "SKIPPED") else 1


if __name__ == "__main__":
    sys.exit(main())
