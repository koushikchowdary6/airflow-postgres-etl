.DEFAULT_GOAL := help
.PHONY: help install up down destroy ps logs init-db fetch run rerun full-refresh \
        quality state examples psql test test-unit lint fmt dag-test trigger verify clean

PYTHON ?= python3
COMPOSE ?= docker compose

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies (including dev tools)
	$(PYTHON) -m pip install -r requirements-dev.txt

# ---- Docker stack -----------------------------------------------------------

up: ## Start PostgreSQL and Airflow, waiting for health checks
	$(COMPOSE) up -d
	@echo "Waiting for services to report healthy..."
	@$(COMPOSE) ps

down: ## Stop the stack, keeping data
	$(COMPOSE) down

destroy: ## Stop the stack and delete all data volumes
	$(COMPOSE) down -v

ps: ## Show service status and health
	$(COMPOSE) ps

logs: ## Follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

trigger: ## Trigger the DAG once via the Airflow CLI
	$(COMPOSE) exec airflow-scheduler airflow dags trigger congress_demographics_etl

# ---- Pipeline ---------------------------------------------------------------

init-db: ## Create schemas, tables, constraints and indexes
	$(PYTHON) -m etl.cli init-db

fetch: ## Download the source CSV into data/raw
	$(PYTHON) -m etl.cli fetch

run: ## Run the full pipeline (downloads the source if missing)
	$(PYTHON) -m etl.cli run

rerun: ## Run again without downloading, to show idempotency
	$(PYTHON) -m etl.cli run --offline

full-refresh: ## Re-process every row, ignoring the watermark
	$(PYTHON) -m etl.cli run --offline --full-refresh

quality: ## Re-run the data-quality checks
	$(PYTHON) -m etl.cli quality

state: ## Show the watermark and recent batch runs
	$(PYTHON) -m etl.cli state

examples: ## Run the example analytical queries
	$(PYTHON) -m etl.cli examples

psql: ## Open psql against the warehouse
	PGPASSWORD=$${POSTGRES_PASSWORD} psql -h $${POSTGRES_HOST:-localhost} \
		-p $${POSTGRES_PORT:-5432} -U $${POSTGRES_USER:-etl} -d $${POSTGRES_DB:-congress_etl}

# ---- Checks -----------------------------------------------------------------

test: ## Run the full suite (integration tests need PostgreSQL)
	POSTGRES_DB=congress_etl_test $(PYTHON) -m pytest -v

test-unit: ## Run only the tests that need no database
	$(PYTHON) -m pytest tests/test_schema.py tests/test_extract.py tests/test_dag.py -v

dag-test: ## Verify the DAG with Airflow, if installed
	$(PYTHON) -m pytest tests/test_dag.py -v -W "ignore::DeprecationWarning"

lint: ## Lint and check formatting
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

fmt: ## Auto-fix lint findings and format
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

verify: lint test ## Everything that runs locally

clean: ## Remove generated data and caches
	rm -rf data/staged .pytest_cache .ruff_cache result.txt
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
