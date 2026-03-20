"""
api_server.py
-------------
Flask REST API server for the Data Validator UI.

Config storage:  saved_configs/{config_id}.yaml  +  saved_configs/registry.json
Run storage:     saved_configs/runs_cache.json   +  BigQuery (best-effort)

Usage:
    python api_server.py
    python api_server.py --port 8000 --host 0.0.0.0 --no-metadata

Endpoints:
    GET    /health
    GET    /configs
    GET    /configs/<config_id>
    POST   /configs
    PUT    /configs/<config_id>
    DELETE /configs/<config_id>
    POST   /run-validation
    GET    /runs
    GET    /runs/<run_id>
"""

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from flask import Flask, jsonify, request
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_SERVER_DIR = Path(__file__).parent
_SAVED_CONFIGS_DIR = _SERVER_DIR / "saved_configs"
_REGISTRY_FILE = _SAVED_CONFIGS_DIR / "registry.json"
_RUNS_CACHE_FILE = _SAVED_CONFIGS_DIR / "runs_cache.json"

_SAVED_CONFIGS_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Set via CLI --no-metadata flag
_SKIP_METADATA = False


# ===========================================================================
# Utility helpers
# ===========================================================================

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


# ── Registry (saved configs index) ──────────────────────────────────────────

def _load_registry() -> Dict[str, Any]:
    if not _REGISTRY_FILE.exists():
        return {}
    try:
        return json.loads(_REGISTRY_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to read registry: %s", exc)
        return {}


def _save_registry(registry: Dict[str, Any]) -> None:
    _REGISTRY_FILE.write_text(
        json.dumps(registry, indent=2, default=str), encoding="utf-8"
    )


def _config_yaml_path(config_id: str) -> Path:
    return _SAVED_CONFIGS_DIR / f"{config_id}.yaml"


# ── Runs cache ───────────────────────────────────────────────────────────────

def _load_runs_cache() -> Dict[str, Any]:
    if not _RUNS_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(_RUNS_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to read runs cache: %s", exc)
        return {}


def _save_runs_cache(cache: Dict[str, Any]) -> None:
    _RUNS_CACHE_FILE.write_text(
        json.dumps(cache, indent=2, default=str), encoding="utf-8"
    )


# ── BigQuery metadata (best-effort) ─────────────────────────────────────────

def _try_write_config_bq(
    py_config,
    yaml_path: Optional[str] = None,
    config_name: Optional[str] = None,
) -> None:
    if _SKIP_METADATA:
        return
    try:
        from core.metadata_writer import MetadataWriter
        writer = MetadataWriter()
        writer.write_config(py_config, config_path=yaml_path, config_name=config_name)
    except Exception as exc:
        logger.warning("BQ config metadata write skipped: %s", exc)


def _try_write_run_bq(
    py_config,
    output: Dict[str, Any],
    config_id: Optional[str] = None,
    config_name: Optional[str] = None,
) -> None:
    if _SKIP_METADATA:
        return
    try:
        from core.metadata_writer import MetadataWriter
        writer = MetadataWriter()
        # Upsert the config row first so validation_runs has a valid config_id reference
        resolved_config_id = writer.write_config(
            py_config, config_path=config_id, config_name=config_name
        )
        writer.write_run(
            py_config, output, config_id=resolved_config_id, config_name=config_name
        )
        writer.write_tests(py_config, output)
    except Exception as exc:
        logger.warning("BQ run metadata write skipped: %s", exc)


# ── Config dict cleaner ──────────────────────────────────────────────────────

def _ui_config_to_yaml_dict(ui_config: Dict[str, Any]) -> Dict[str, Any]:
    """Strip UI-only keys that the Python validator doesn't understand."""
    drop = {"id", "created_at", "updated_at", "name", "blocks", "config_path", "config_yaml"}
    cleaned: Dict[str, Any] = {}
    for k, v in ui_config.items():
        if k in drop:
            continue
        if v is None:
            continue
        # Skip empty strings for non-required string fields
        if isinstance(v, str) and v == "" and k not in ("project", "dataset", "table", "file_path"):
            continue
        # Strip empty lists
        if isinstance(v, list) and len(v) == 0 and k not in (
            "primary_keys", "aggregate_columns", "distribution_columns",
            "null_check_columns", "column_checksum_columns"
        ):
            continue
        cleaned[k] = v
    return cleaned


# ===========================================================================
# Routes
# ===========================================================================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "data-validator-api"})


# ---------------------------------------------------------------------------
# GET /configs
# ---------------------------------------------------------------------------
@app.route("/configs", methods=["GET"])
def list_configs():
    registry = _load_registry()
    result = []
    for config_id, meta in registry.items():
        yaml_path = _config_yaml_path(config_id)
        if not yaml_path.exists():
            continue
        try:
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        except Exception:
            raw = {}
        result.append({
            "id": config_id,
            "name": meta.get("name", config_id),
            "config": raw,
            "blocks": meta.get("blocks", []),
            "created_at": meta.get("created_at", ""),
            "updated_at": meta.get("updated_at", ""),
        })
    result.sort(key=lambda c: c.get("updated_at", ""), reverse=True)
    return jsonify(result)


# ---------------------------------------------------------------------------
# GET /configs/<config_id>
# ---------------------------------------------------------------------------
@app.route("/configs/<config_id>", methods=["GET"])
def get_config(config_id: str):
    registry = _load_registry()
    if config_id not in registry:
        return jsonify({"error": f"Config '{config_id}' not found"}), 404

    yaml_path = _config_yaml_path(config_id)
    if not yaml_path.exists():
        return jsonify({"error": "Config YAML file missing"}), 404

    meta = registry[config_id]
    try:
        raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return jsonify({"error": f"Failed to parse YAML: {exc}"}), 500

    return jsonify({
        "id": config_id,
        "name": meta.get("name", config_id),
        "config": raw,
        "blocks": meta.get("blocks", []),
        "created_at": meta.get("created_at", ""),
        "updated_at": meta.get("updated_at", ""),
    })


# ---------------------------------------------------------------------------
# POST /configs
# ---------------------------------------------------------------------------
@app.route("/configs", methods=["POST"])
def create_config():
    body = request.get_json(silent=True) or {}
    name = body.get("name") or "Unnamed Config"
    ui_config = body.get("config") or {}
    blocks = body.get("blocks") or []

    if not ui_config:
        return jsonify({"error": "Request body must include 'config'"}), 400

    config_id = str(uuid.uuid4())
    now = _now_iso()

    yaml_dict = _ui_config_to_yaml_dict(ui_config)
    yaml_path = _config_yaml_path(config_id)
    yaml_text = yaml.dump(
        yaml_dict, default_flow_style=False, allow_unicode=True, sort_keys=False
    )
    yaml_path.write_text(yaml_text, encoding="utf-8")

    registry = _load_registry()
    registry[config_id] = {
        "name": name,
        "blocks": blocks,
        "created_at": now,
        "updated_at": now,
    }
    _save_registry(registry)

    # Best-effort BigQuery metadata
    try:
        from core.config_loader import load_config_from_dict
        py_cfg = load_config_from_dict(yaml_dict)
        py_cfg.config_yaml = yaml_text
        _try_write_config_bq(py_cfg, yaml_path=str(yaml_path), config_name=name)
    except Exception as exc:
        logger.warning("BQ config write skipped: %s", exc)

    logger.info("Config created: id=%s  name=%s", config_id, name)
    return jsonify({
        "id": config_id,
        "name": name,
        "config": yaml_dict,
        "blocks": blocks,
        "created_at": now,
        "updated_at": now,
    }), 201


# ---------------------------------------------------------------------------
# PUT /configs/<config_id>
# ---------------------------------------------------------------------------
@app.route("/configs/<config_id>", methods=["PUT"])
def update_config(config_id: str):
    registry = _load_registry()
    if config_id not in registry:
        return jsonify({"error": f"Config '{config_id}' not found"}), 404

    body = request.get_json(silent=True) or {}
    existing = registry[config_id]
    name = body.get("name") or existing.get("name", "Unnamed Config")
    ui_config = body.get("config") or {}
    blocks = body.get("blocks") if "blocks" in body else existing.get("blocks", [])
    now = _now_iso()

    if not ui_config:
        return jsonify({"error": "Request body must include 'config'"}), 400

    yaml_dict = _ui_config_to_yaml_dict(ui_config)
    yaml_path = _config_yaml_path(config_id)
    yaml_text = yaml.dump(
        yaml_dict, default_flow_style=False, allow_unicode=True, sort_keys=False
    )
    yaml_path.write_text(yaml_text, encoding="utf-8")

    registry[config_id] = {
        "name": name,
        "blocks": blocks,
        "created_at": existing.get("created_at", now),
        "updated_at": now,
    }
    _save_registry(registry)

    # Best-effort BigQuery metadata
    try:
        from core.config_loader import load_config_from_dict
        py_cfg = load_config_from_dict(yaml_dict)
        py_cfg.config_yaml = yaml_text
        _try_write_config_bq(py_cfg, yaml_path=str(yaml_path), config_name=name)
    except Exception as exc:
        logger.warning("BQ config write skipped: %s", exc)

    logger.info("Config updated: id=%s  name=%s", config_id, name)
    return jsonify({
        "id": config_id,
        "name": name,
        "config": yaml_dict,
        "blocks": blocks,
        "created_at": existing.get("created_at", now),
        "updated_at": now,
    })


# ---------------------------------------------------------------------------
# DELETE /configs/<config_id>
# ---------------------------------------------------------------------------
@app.route("/configs/<config_id>", methods=["DELETE"])
def delete_config(config_id: str):
    registry = _load_registry()
    if config_id not in registry:
        return jsonify({"error": f"Config '{config_id}' not found"}), 404

    yaml_path = _config_yaml_path(config_id)
    if yaml_path.exists():
        yaml_path.unlink()

    del registry[config_id]
    _save_registry(registry)
    logger.info("Config deleted: id=%s", config_id)
    return jsonify({"deleted": config_id})


# ---------------------------------------------------------------------------
# POST /run-validation
# ---------------------------------------------------------------------------
@app.route("/run-validation", methods=["POST"])
def run_validation():
    body = request.get_json(silent=True) or {}
    ui_config = body.get("config") or {}
    config_id_hint = body.get("config_id")

    if not ui_config:
        return jsonify({"error": "Request body must include 'config'"}), 400

    # run_id will be taken from the validation_runner output (single source of truth)
    provisional_run_id = f"run-{uuid.uuid4().hex[:12]}"
    now = _now_iso()

    try:
        from core.config_loader import load_config_from_dict
        from engine import validation_runner, result_formatter

        yaml_dict = _ui_config_to_yaml_dict(ui_config)
        py_cfg = load_config_from_dict(yaml_dict)
        py_cfg.config_yaml = yaml.dump(
            yaml_dict, default_flow_style=False, allow_unicode=True, sort_keys=False
        )

        logger.info("Starting validation run ...")
        output = validation_runner.run(py_cfg)
        json_str = result_formatter.to_json(output)
        result_dict = json.loads(json_str)

        # Use the run_id generated by validation_runner as the canonical ID
        run_id = result_dict.get("run_id") or provisional_run_id

    except Exception as exc:
        logger.error("Validation run failed: %s", exc, exc_info=True)
        run_id = provisional_run_id
        # Store a failed run in cache
        failed_run = {
            "run_id": run_id,
            "status": "failed",
            "config": ui_config,
            "summary": {"total": 0, "passed": 0, "failed": 0,
                        "skipped": 0, "errors": 1, "execution_time_ms": 0},
            "results": [{"test_name": "run_error", "status": "ERROR",
                         "message": str(exc), "execution_time_ms": 0}],
            "created_at": now,
            "completed_at": _now_iso(),
            "error": str(exc),
        }
        cache = _load_runs_cache()
        cache[run_id] = failed_run
        _save_runs_cache(cache)
        return jsonify({"error": str(exc), "run_id": run_id}), 500

    # Normalise the output format for the UI
    summary_raw = result_dict.get("summary", {})
    exec_ms = int(result_dict.get("total_execution_time_ms", 0))

    # Map result_formatter output fields to UI test result format
    raw_results = result_dict.get("results", [])
    ui_results = []
    for r in raw_results:
        ui_results.append({
            "test_name": r.get("test_name", ""),
            "status": r.get("status", "ERROR"),
            "expected": str(r.get("expected", "")) if r.get("expected") is not None else None,
            "actual": str(r.get("actual", "")) if r.get("actual") is not None else None,
            "execution_time_ms": int(r.get("execution_time_ms", 0)),
            "details": r.get("details"),
            "message": r.get("message"),
        })

    ui_run = {
        "run_id": run_id,
        "status": "completed",
        "config": ui_config,
        "summary": {
            "total": summary_raw.get("total", 0),
            "passed": summary_raw.get("passed", 0),
            "failed": summary_raw.get("failed", 0),
            "skipped": summary_raw.get("skipped", 0),
            "errors": summary_raw.get("errors", 0),
            "execution_time_ms": exec_ms,
        },
        "results": ui_results,
        "created_at": now,
        "completed_at": _now_iso(),
    }

    # Store in local runs cache
    cache = _load_runs_cache()
    cache[run_id] = ui_run
    _save_runs_cache(cache)

    # Best-effort BigQuery metadata — use the canonical run_id from result_dict
    result_dict["run_id"] = run_id  # ensure they are the same

    # Look up the human-readable name from the registry if a config_id was provided
    bq_config_name: Optional[str] = None
    if config_id_hint:
        reg = _load_registry()
        bq_config_name = reg.get(config_id_hint, {}).get("name")

    try:
        _try_write_run_bq(
            py_cfg, result_dict, config_id=config_id_hint, config_name=bq_config_name
        )
    except Exception as exc:
        logger.warning("BQ run write skipped: %s", exc)

    logger.info(
        "Run complete: run_id=%s  status=%s  total=%d  passed=%d  failed=%d",
        run_id,
        result_dict.get("overall_status", "?"),
        summary_raw.get("total", 0),
        summary_raw.get("passed", 0),
        summary_raw.get("failed", 0),
    )
    return jsonify({"run_id": run_id})


# ---------------------------------------------------------------------------
# GET /runs
# ---------------------------------------------------------------------------
@app.route("/runs", methods=["GET"])
def list_runs():
    cache = _load_runs_cache()
    runs = []
    for run_id, run_data in cache.items():
        runs.append({
            "run_id": run_id,
            "status": run_data.get("status", "unknown"),
            "created_at": run_data.get("created_at", ""),
            "completed_at": run_data.get("completed_at"),
            "summary": run_data.get("summary", {}),
        })
    runs.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return jsonify(runs)


# ---------------------------------------------------------------------------
# GET /runs/<run_id>
# ---------------------------------------------------------------------------
@app.route("/runs/<run_id>", methods=["GET"])
def get_run(run_id: str):
    cache = _load_runs_cache()
    run_data = cache.get(run_id)
    if run_data is None:
        return jsonify({"error": f"Run '{run_id}' not found"}), 404
    return jsonify(run_data)


# ===========================================================================
# CLI entry point
# ===========================================================================

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="api_server",
        description="Data Validator UI — Flask REST API server",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    parser.add_argument(
        "--no-metadata",
        action="store_true",
        default=False,
        help="Skip BigQuery metadata writes (useful for local dev without GCP credentials)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable Flask debug mode (auto-reload on file changes)",
    )
    return parser


if __name__ == "__main__":
    args = _build_parser().parse_args()

    # Propagate --no-metadata flag to the module-level variable
    _SKIP_METADATA = args.no_metadata

    logger.info(
        "Starting Data Validator API server on %s:%d  skip_metadata=%s",
        args.host,
        args.port,
        _SKIP_METADATA,
    )
    app.run(host=args.host, port=args.port, debug=args.debug)
