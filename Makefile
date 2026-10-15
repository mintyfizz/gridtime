.PHONY: up down install init ingest poll peek test lint

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

peek:          ## Show what's in Bronze
	uv run python -m gridtime.storage.peek

test:
	uv run pytest -q

lint:
	uv run ruff check src tests
