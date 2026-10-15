.PHONY: up down install init ingest poll sink backfill forecast mlflow peek silver gold orchestrate dashboard test lint

up:            ## Start MinIO, Redpanda and Redpanda Console
	docker compose up -d

down:          ## Stop containers (data is kept in the volume)
	docker compose down

install:       ## Install dependencies into .venv
	uv sync

init:          ## Create/evolve Iceberg tables and the Redpanda topics
	uv run python -m gridtime.storage.tables
	uv run python -c "from gridtime.ingestion.stream import ensure_topics; print('topics created:', ensure_topics() or 'none (already exist)')"

ingest:        ## Poll Elia once into Redpanda, then drain the topics into Bronze
	uv run python -m gridtime.ingestion.poller --once
	uv run python -m gridtime.ingestion.bronze_sink --once

poll:          ## Publish Elia observations to Redpanda continuously
	uv run python -m gridtime.ingestion.poller

sink:          ## Consume Redpanda into Bronze continuously (don't run alongside Dagster)
	uv run python -m gridtime.ingestion.bronze_sink

backfill:      ## Publish 8 weeks of Elia history to Redpanda (the sink ingests it)
	uv run python -m gridtime.ingestion.backfill --days 56

forecast:      ## Retrain, run both backtests, then make a live forecast
	uv run python -m gridtime.forecasting.pipeline backtest
	uv run python -m gridtime.forecasting.pipeline live

mlflow:        ## MLflow UI at http://localhost:5001
	uv run mlflow ui --backend-store-uri sqlite:///$(CURDIR)/data/mlflow.db --port 5001

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
