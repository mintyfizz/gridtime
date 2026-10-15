# 001 – Apache Iceberg for all lakehouse tables

**Status:** accepted · 2026-10-15

**Context.** Bronze and Silver are append-only histories of a dataset that revises itself.
We need cheap appends, schema evolution as Elia adds fields, and the ability to inspect
past states of a table.

**Decision.** Store every table as Iceberg on S3-compatible storage (MinIO), managed with
PyIceberg. Start with a SQLite catalog; move to Nessie in milestone 6.

**Consequences.** Engine-independent tables (DuckDB now, Trino later), snapshot time travel
for free, no lock-in to a vendor platform. Cost: one more concept to learn, and small files
from frequent appends will need periodic compaction.
