.PHONY: up down install init ingest poll peek silver gold orchestrate dashboard test lint

up:            ## Start MinIO and create the bucket
	docker compose up -d

down:          ## Stop containers (data is kept in the volume)
	docker compose down

install:       ## Install dependencies into .venv
	uv sync

init:          ## Create Iceberg namespace and tables
	uv run python -m gridtime.storage.tables

ingest:        ## Poll Elia once
	uv run python -m gridtime.ingestion.poller --once

poll:          ## Poll Elia continuously
	uv run python -m gridtime.ingestion.poller

silver:        ## Process new Bronze rows into the bitemporal Silver table
	uv run python -m gridtime.transform.silver

gold:          ## Build and test the dbt Gold models from the latest Silver snapshot
	cd transforms && \
	GRIDTIME_SILVER_PARQUET=$(CURDIR)/data/silver/balancing_minute.parquet \
	GRIDTIME_GOLD_DB=$(CURDIR)/data/warehouse.duckdb \
	uv run dbt build --profiles-dir .

orchestrate:   ## Run everything on a schedule with Dagster at http://localhost:3000
	mkdir -p .dagster && cp dagster.yaml .dagster/dagster.yaml
	DAGSTER_HOME=$(CURDIR)/.dagster uv run dagster dev

peek:          ## Show what's in Bronze
	uv run python -m gridtime.storage.peek

dashboard:     ## Open the dashboard at http://localhost:8501
	uv run streamlit run src/gridtime/dashboard/app.py

test:
	uv run pytest -q

lint:
	uv run ruff check src tests
