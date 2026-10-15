# Architecture

## Data flow (milestone 1)

1. `make up` starts MinIO (local S3) and creates the `lakehouse` bucket.
2. The poller, for each configured Elia dataset:
   - fetches records with `datetime >= now - lookback`
   - validates each record (must have a timezone-aware `datetime`)
   - stamps it with `transaction_time = now` and a SHA-256 `payload_hash`
   - skips hashes already stored, so re-runs are safe
   - appends the rest to the Bronze Iceberg table
3. DuckDB reads Bronze through an Iceberg scan for inspection.

## Silver (milestone 2)

`make silver` reads Bronze rows newer than the watermark stored on the latest Silver snapshot,
maps each payload onto canonical measures, and appends a version only where the value, Elia's
quality flag or the publication stage changed. See
[decisions/002](decisions/002-silver-versioning.md).

| Column | Meaning |
|--------|---------|
| `valid_from`, `valid_to` | the minute the value describes |
| `measure`, `value`, `unit` | canonical measure name and its value |
| `stage` | `near_real_time` (ods175) or `historical` (ods165) |
| `quality_status` | Elia's own flag: `NotValidated`, `DataIssue`, … |
| `transaction_time` | when GridTime received this version |
| `source_dataset`, `payload_hash` | lineage back to the Bronze record |

## Orchestration and Gold (milestone 3)

`make orchestrate` starts Dagster. The asset graph:

```
bronze/elia_nrt ─────────┐
  (every 5 min)          ├─► silver/balancing_minute ─► staging/stg_silver__versions ─┬─► gold/fct_balancing_minute ─► gold/fct_quarter_hour
bronze/elia_historical ──┘     (eager)                       (dbt)                   └─► gold/fct_revisions ─► gold/agg_revision_stats
  (hourly at :07)
```

Runs are serialised (`dagster.yaml`, one concurrent run). Checks: near-real-time freshness
(< 30 min), no duplicate Silver versions, and every dbt test. See
[decisions/003](decisions/003-orchestration-and-gold.md).

## Layers

| Layer | Contents | Rule |
|-------|----------|------|
| Bronze | Raw payloads + metadata | Append-only, never updated |
| Silver | One row per (minute, measure, version) with valid time and transaction time | Append-only, bitemporal, change-only |
| Gold | dbt models on DuckDB (`data/warehouse.duckdb`) | Rebuilt from a Silver snapshot after every change |

## Why two times

The same quarter-hour is published several times: forecasts that update every minute,
a non-validated near-real-time value, and a validated value the next day. Keeping each
version with the time it was received allows "as-of" queries and leak-free backtests.
