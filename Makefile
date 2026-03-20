# =============================================================================
# Makefile — BigQuery Data Validation Framework
# =============================================================================
# Targets:
#   make setup        — Create venv and install dependencies
#   make api          — Start the Flask REST API server (for the UI)
#   make api-no-bq    — Start API server without BigQuery writes
#   make run          — Run validator locally (CLI mode)
#   make serve        — Start local Cloud Function dev server
#   make invoke       — Send a test HTTP request to the local dev server
#   make deploy       — Deploy to Google Cloud Functions (Gen 2)
#   make lint         — Run flake8 linter
#   make clean        — Remove venv and temp files
#   make help         — Show this help
# =============================================================================

# ---------------------------------------------------------------------------
# Config — override on the command line:  make deploy REGION=europe-west1
# ---------------------------------------------------------------------------
VENV            := .venv
PYTHON          := $(VENV)/bin/python
PIP             := $(VENV)/bin/pip
FUNCTIONS_FW    := $(VENV)/bin/functions-framework

# Load .env if it exists (for local runs)
-include .env
export

# Cloud Function deployment settings
FUNCTION_NAME   ?= bq-data-validator
FUNCTION_TARGET ?= bq_validate
REGION          ?= us-central1
RUNTIME         ?= python311
MEMORY          ?= 512MB
TIMEOUT         ?= 540s
GCP_PROJECT     ?= $(GOOGLE_CLOUD_PROJECT)

# Local dev server settings
FUNCTION_PORT   ?= 8080
CONFIG          ?= config/validation_config.yaml
OUTPUT          ?=
LOG_LEVEL       ?= INFO

# API server settings
API_PORT        ?= 8000
API_HOST        ?= 0.0.0.0

# ---------------------------------------------------------------------------
# Phony targets
# ---------------------------------------------------------------------------
.PHONY: setup api api-no-bq run serve invoke deploy lint clean help

# ---------------------------------------------------------------------------
# Default target
# ---------------------------------------------------------------------------
.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# setup — Create virtual environment and install all dependencies
# ---------------------------------------------------------------------------
setup: ## Create venv and install dependencies
	@echo "▶  Setting up virtual environment..."
	bash setup_venv.sh
	@echo "✓  Setup complete. Activate with: source $(VENV)/bin/activate"

# ---------------------------------------------------------------------------
# api — Start the Flask REST API server (used by the React UI)
# ---------------------------------------------------------------------------
api: $(VENV)/bin/activate ## Start the Flask REST API server (for the data-validator-ui)
	@echo "▶  Starting Data Validator API server..."
	@echo "   URL  : http://$(API_HOST):$(API_PORT)"
	@echo "   Stop : Ctrl+C"
	@echo ""
	$(PYTHON) api_server.py --host $(API_HOST) --port $(API_PORT)

# ---------------------------------------------------------------------------
# api-no-bq — Start API server without BigQuery metadata writes
# ---------------------------------------------------------------------------
api-no-bq: $(VENV)/bin/activate ## Start the Flask API server with --no-metadata (no GCP credentials needed)
	@echo "▶  Starting Data Validator API server (no BigQuery writes)..."
	@echo "   URL  : http://$(API_HOST):$(API_PORT)"
	@echo "   Stop : Ctrl+C"
	@echo ""
	$(PYTHON) api_server.py --host $(API_HOST) --port $(API_PORT) --no-metadata

# ---------------------------------------------------------------------------
# run — Run the validator locally via CLI
# ---------------------------------------------------------------------------
run: $(VENV)/bin/activate ## Run validator locally (CLI mode)
	@echo "▶  Running validator (local CLI)..."
	@echo "   Config : $(CONFIG)"
	@if [ -n "$(OUTPUT)" ]; then \
		$(PYTHON) main.py --config "$(CONFIG)" --output "$(OUTPUT)" --log-level $(LOG_LEVEL); \
	else \
		$(PYTHON) main.py --config "$(CONFIG)" --log-level $(LOG_LEVEL); \
	fi

# ---------------------------------------------------------------------------
# serve — Start the local Cloud Function dev server (functions-framework)
# ---------------------------------------------------------------------------
serve: $(VENV)/bin/activate ## Start local Cloud Function HTTP server
	@echo "▶  Starting local Cloud Function dev server on port $(FUNCTION_PORT)..."
	@echo "   Entry point : $(FUNCTION_TARGET)"
	@echo "   URL         : http://localhost:$(FUNCTION_PORT)"
	@echo "   Stop with   : Ctrl+C"
	@echo ""
	$(FUNCTIONS_FW) \
		--target=$(FUNCTION_TARGET) \
		--port=$(FUNCTION_PORT) \
		--debug

# ---------------------------------------------------------------------------
# invoke — Send a test HTTP request to the local dev server
# ---------------------------------------------------------------------------
invoke: ## POST a test request to the local dev server (requires 'make serve' running)
	@echo "▶  Sending test request to http://localhost:$(FUNCTION_PORT)..."
	curl -s -X POST \
		-H "Content-Type: application/json" \
		-d '{"config_path": "$(CONFIG)"}' \
		http://localhost:$(FUNCTION_PORT) \
		| python3 -m json.tool

# ---------------------------------------------------------------------------
# deploy — Deploy to Google Cloud Functions (Gen 2)
# ---------------------------------------------------------------------------
deploy: ## Deploy to Google Cloud Functions Gen 2
	@if [ -z "$(GCP_PROJECT)" ]; then \
		echo "✗  GCP_PROJECT is not set. Add it to .env or pass as: make deploy GCP_PROJECT=my-project"; \
		exit 1; \
	fi
	@echo "▶  Deploying to Cloud Functions..."
	@echo "   Project  : $(GCP_PROJECT)"
	@echo "   Function : $(FUNCTION_NAME)"
	@echo "   Region   : $(REGION)"
	@echo "   Runtime  : $(RUNTIME)"
	gcloud functions deploy $(FUNCTION_NAME) \
		--gen2 \
		--project=$(GCP_PROJECT) \
		--region=$(REGION) \
		--runtime=$(RUNTIME) \
		--source=. \
		--entry-point=$(FUNCTION_TARGET) \
		--trigger-http \
		--memory=$(MEMORY) \
		--timeout=$(TIMEOUT) \
		--set-env-vars LOG_LEVEL=$(LOG_LEVEL) \
		--allow-unauthenticated
	@echo "✓  Deployment complete."
	@echo ""
	@echo "  Invoke with:"
	@echo "    curl -X POST \\"
	@echo "      -H 'Content-Type: application/json' \\"
	@echo "      -d '{\"config_path\": \"gs://YOUR_BUCKET/config.yaml\"}' \\"
	@echo "      \$$(gcloud functions describe $(FUNCTION_NAME) --region=$(REGION) --format='value(serviceConfig.uri)')"

# ---------------------------------------------------------------------------
# lint — Run flake8 (style and error checking)
# ---------------------------------------------------------------------------
lint: $(VENV)/bin/activate ## Run flake8 linter
	@if ! $(VENV)/bin/python -m flake8 --version &>/dev/null; then \
		$(PIP) install --quiet flake8; \
	fi
	$(VENV)/bin/python -m flake8 \
		--max-line-length=100 \
		--exclude=$(VENV),__pycache__ \
		core/ validators/ engine/ main.py

# ---------------------------------------------------------------------------
# clean — Remove venv and cache files
# ---------------------------------------------------------------------------
clean: ## Remove virtual environment and cache files
	@echo "▶  Cleaning up..."
	rm -rf $(VENV)
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "✓  Clean complete."

# ---------------------------------------------------------------------------
# help — Print available targets
# ---------------------------------------------------------------------------
help: ## Show this help message
	@echo ""
	@echo "BigQuery Data Validation Framework — Makefile targets"
	@echo "══════════════════════════════════════════════════════"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make setup"
	@echo "  make api                          # start API server (requires GCP credentials)"
	@echo "  make api-no-bq                    # start API server without BigQuery writes"
	@echo "  make api API_PORT=9000            # use a different port"
	@echo "  make run CONFIG=config/validation_config.yaml"
	@echo "  make run CONFIG=config/validation_config.yaml OUTPUT=results.json"
	@echo "  make serve"
	@echo "  make invoke CONFIG=config/validation_config.yaml"
	@echo "  make deploy GCP_PROJECT=my-gcp-project REGION=us-central1"
	@echo ""

# ---------------------------------------------------------------------------
# Internal: ensure venv exists before running Python commands
# ---------------------------------------------------------------------------
$(VENV)/bin/activate:
	@if [ ! -d "$(VENV)" ]; then \
		echo "Virtual environment not found. Run 'make setup' first."; \
		exit 1; \
	fi