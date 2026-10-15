# 003 – Dagster with one run at a time; Gold on DuckDB via a Silver snapshot

**Status:** accepted · 2026-10-15

**Context.** Milestone 3 replaces the `make` commands with scheduled, observable runs and adds
a modelled Gold layer. Two constraints shaped the design:

1. Silver's watermark (decision 002) assumes Bronze rows never arrive with a
   `transaction_time` older than the last processed one. With independent schedules for the
   two Elia feeds, overlapping runs would break that: a slow historical poll could commit after
   a faster near-real-time poll had already advanced the watermark, and its rows would be
   skipped for good. Concurrent Iceberg commits through the SQL catalog would also conflict.
2. DuckDB's Iceberg reader cannot use a PyIceberg SQL catalog, so dbt-duckdb cannot read
   Silver through the catalog directly.

**Decision.**
- **Dagster, asset-based.** Bronze polls are scheduled assets (ods175 every 5 minutes,
  ods165 hourly at :07). Silver and every dbt model use `AutomationCondition.eager()`, so they
  rebuild as soon as something upstream changed. There is no hand-written DAG.
- **One run at a time.** The instance uses a queued run coordinator with
  `max_concurrent_runs: 1` (`dagster.yaml`). Every run is short (seconds), so serialising them
  costs nothing and makes the single-writer assumption true by construction.
- **Duplicate-version guard.** A Dagster asset check and a dbt test both fail if Silver ever
  holds two versions of one (minute, measure) with the same transaction time, the signature
  of concurrent writers.
- **Gold on DuckDB.** The Silver asset publishes an atomic Parquet snapshot after each run;
  dbt-duckdb reads it as a source and writes Gold tables to `data/warehouse.duckdb`. The dbt
  source is mapped onto the Silver asset key, so lineage runs Bronze → Silver → Gold in one
  graph.

**Consequences.** One orchestrated graph with lineage, run history, metadata (rows fetched,
versions written, watermark) and checks. Gold is fast and file-based. Cost: the snapshot is a
full export, fine at current size (tens of thousands of rows per day) but not forever.
Milestone 6 replaces it with Trino reading Iceberg through a Nessie catalog, and the
single-run limit with per-table commit retries.
