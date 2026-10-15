# 004 – Redpanda between pollers and Bronze; Silver tracks ingestion time

**Status:** accepted · 2026-10-15

**Context.** Until milestone 3 the poller wrote straight to Bronze. That couples fetching to
storage: if Iceberg or MinIO is down, observations are lost, and there is no replayable log of
what Elia served. Adding a buffer changes one assumption: an observation can now reach Bronze
*after* newer observations, so its `transaction_time` (when it was observed) can be older than
rows Silver has already processed.

**Decision.**
- **Redpanda (Kafka API) as the buffer.** One topic per Elia dataset (`elia.ods175`,
  `elia.ods165`), key `dataset|minute` so a minute's observations stay ordered, 7-day retention.
  Redpanda over Kafka: same client API, single binary, no ZooKeeper/KRaft tuning, light enough
  for a laptop. Kafka itself is already covered in an earlier project.
- **Topics hold every observation, not just changes.** The topic is the faithful log of what
  was served; deduplication moves to the sink. Volume is small (~50k messages/day).
- **Dead-letter topic.** Records failing validation go to `elia.dlq` with the error instead of
  being dropped silently.
- **At-least-once sink, effectively-once storage.** The Bronze sink commits consumer offsets
  only after the Iceberg append succeeds; a crash replays messages, and the payload-hash check
  against Bronze drops them. Within a batch the earliest observation of a payload wins, because
  `transaction_time` means *first known at*.
- **`ingested_at` on Bronze, added by schema evolution.** The sink stamps when Bronze committed
  each row (plus `source_offset` for lineage). Silver's watermark now tracks `ingested_at`,
  so late-arriving observations are always processed and still land at their true
  `transaction_time`. Rows written before the column existed fall back to `transaction_time`.

**Lessons from the switchover.** A run fired between deploying the new code and running the
schema migration, and failed because `ingested_at` did not exist yet. Two rules came out of it:
migrate schemas before deploying code that depends on them, and make readers tolerate the
previous schema (Silver now does, with a test).

**Consequences.** Polling survives storage outages for up to the retention period; Bronze can
be rebuilt by replaying the topics; Redpanda Console shows messages and consumer lag. Cost: one
more service, and late arrivals may store a redundant version in Silver (as-of answers stay
correct; only minimality is affected).
