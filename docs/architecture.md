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

## Layers

| Layer | Contents | Rule |
|-------|----------|------|
| Bronze | Raw payloads + metadata | Append-only, never updated |
| Silver | One row per (quarter-hour, measure, version) with `valid_time` and `transaction_time` | Append-only, bitemporal |
| Gold | Analysis-ready models (dbt) | Rebuilt from Silver |

## Why two times

The same quarter-hour is published several times: forecasts that update every minute,
a non-validated near-real-time value, and a validated value the next day. Keeping each
version with the time it was received allows "as-of" queries and leak-free backtests.
