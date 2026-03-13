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

HTTP Request body (JSON) — choose ONE of:
    { "config_path": "gs://my-bucket/configs/validation_config.yaml" }
    { "config_path": "/tmp/validation_config.yaml" }
    { "config": { ...full config dict... } }

HTTP Response: JSON with the full validation results.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging — configured once at module level so both modes share the same setup
# ---------------------------------------------------------------------------

def _configure_logging(level: str = "INFO") -> None:
    """Configure the root logger with a consistent format."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,  # Logs → stderr; JSON output → stdout
    )


# Apply a sensible default at import time (Cloud Function path)
_configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

logger = logging.getLogger(__name__)


# ===========================================================================
# MODE 2 — Cloud Function HTTP handler
# ===========================================================================

def bq_validate(request):  # noqa: ANN001 — flask.Request type hint omitted to avoid hard dep
    """
    Google Cloud Function HTTP entry point.

    Accepts a JSON body with either:
      - config_path: str  → local or GCS path to a YAML config file
      - config:      dict → inline config (same schema as the YAML file)

    Returns a Flask JSON response with the full validation output.

    Args:
        request: flask.Request object injected by the Cloud Function runtime.

    Returns:
        Flask Response with JSON body and appropriate HTTP status code.
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
# MODE 1 — Local CLI
# ===========================================================================

def _build_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="bq-data-validator",
        description=(
            "BigQuery Data Validation Framework — "
            "validates CSV file data against a BigQuery table."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local config + local CSV
  python main.py --config config/validation_config.yaml

  # Local config + write results to file
  python main.py --config config/validation_config.yaml --output results.json

  # GCS config (file_path inside config can also be gs://)
  python main.py --config gs://my-bucket/configs/validation_config.yaml

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
    # 3. Output results
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
    # 4. Exit code
    # ------------------------------------------------------------------
    overall_status = output.get("overall_status", "ERROR").upper()
    summary = output.get("summary", {})
    logger.info(
        "Run complete — status=%s | total=%d | passed=%d | failed=%d | errors=%d | skipped=%d",
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