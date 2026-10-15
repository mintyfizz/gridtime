"""Consume Elia topics → deduplicate → append to Bronze → commit offsets.

Delivery is at-least-once (offsets are committed only after the Iceberg append
succeeds) and deduplication by payload hash makes the result effectively-once:
a replayed message finds its hash already in Bronze and is skipped.

Each row keeps the poller's `received_at` as its transaction_time (when the value
was first observed) and gets `ingested_at` (when Bronze committed it). Silver
reads Bronze by ingested_at, so late-arriving observations are never skipped.

Run once:     python -m gridtime.ingestion.bronze_sink --once
Run forever:  python -m gridtime.ingestion.bronze_sink
"""

import argparse
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition
from pydantic import ValidationError

from gridtime.config import get_settings
from gridtime.ingestion.schemas import BronzeRow, to_bronze_row
from gridtime.ingestion.stream import Observation, ensure_topics, make_consumer

log = logging.getLogger("gridtime.sink")


@dataclass(frozen=True)
class Consumed:
    observation: Observation
    topic: str
    partition: int
    offset: int

    @property
    def source_offset(self) -> str:
        return f"{self.topic}:{self.partition}:{self.offset}"


@dataclass
class SinkStats:
    consumed: int = 0
    new_rows: int = 0
    duplicates: int = 0
    rejected: int = 0
    undecodable: int = 0
    lag_after: dict[str, int] = field(default_factory=dict)

    @property
    def total_lag(self) -> int:
        return sum(self.lag_after.values())


# ---------------------------------------------------------------- pure logic


def to_rows(batch: Iterable[Consumed], ingested_at: datetime) -> tuple[list[BronzeRow], int]:
    """Validate and convert; within the batch keep the earliest observation per payload.

    Returns (rows, rejected). The earliest received_at wins because transaction_time
    means "first known at".
    """
    best: dict[str, BronzeRow] = {}
    rejected = 0
    for item in batch:
        obs = item.observation
        try:
            row = to_bronze_row(obs.dataset_id, obs.payload, obs.received_at, obs.source_url)
        except ValidationError as exc:
            rejected += 1
            log.warning("%s: invalid record skipped (%s)", item.source_offset, exc.errors()[0])
            continue
        row = row.model_copy(update={"ingested_at": ingested_at,
                                     "source_offset": item.source_offset})
        current = best.get(row.payload_hash)
        if current is None or row.transaction_time < current.transaction_time:
            best[row.payload_hash] = row
    return list(best.values()), rejected


def drop_stored(rows: list[BronzeRow], stored_hashes: set[str]) -> list[BronzeRow]:
    return [r for r in rows if r.payload_hash not in stored_hashes]


# ---------------------------------------------------------------- Kafka I/O


def _assign_from_committed(consumer: Consumer, topics: list[str]) -> list[TopicPartition]:
    metadata = consumer.list_topics(timeout=10)
    partitions = [
        TopicPartition(t, p) for t in topics for p in metadata.topics[t].partitions
    ]
    committed = consumer.committed(partitions, timeout=10)
    for tp in committed:
        if tp.offset < 0:
            tp.offset = OFFSET_BEGINNING
    consumer.assign(committed)
    return committed


def consume_available(
    consumer: Consumer, topics: list[str], max_messages: int = 200_000, timeout_s: float = 60
) -> tuple[list[Consumed], int, dict[tuple[str, int], int]]:
    """Read everything currently in the topics past the committed offsets.

    Returns (messages, undecodable, next_offsets) where next_offsets maps
    (topic, partition) → offset to commit.
    """
    # For each partition: where we start (committed offset, or the log start) and
    # where the log currently ends. Only partitions with start < end have work.
    pending: dict[tuple[str, int], int] = {}
    for tp in _assign_from_committed(consumer, topics):
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        start = low if tp.offset == OFFSET_BEGINNING else tp.offset
        if start < high:
            pending[(tp.topic, tp.partition)] = high

    out: list[Consumed] = []
    next_offsets: dict[tuple[str, int], int] = {}
    undecodable = 0
    deadline = time.monotonic() + timeout_s

    def caught_up() -> bool:
        return all(next_offsets.get(key, -1) >= end for key, end in pending.items())

    while not caught_up() and len(out) < max_messages and time.monotonic() < deadline:
        for msg in consumer.consume(num_messages=1000, timeout=1.0):
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Kafka error: {msg.error()}")
            key = (msg.topic(), msg.partition())
            next_offsets[key] = msg.offset() + 1
            try:
                obs = Observation.decode(msg.value())
            except (ValueError, KeyError) as exc:
                undecodable += 1
                log.error("%s:%s:%s undecodable (%s)", *key, msg.offset(), exc)
                continue
            out.append(Consumed(obs, msg.topic(), msg.partition(), msg.offset()))
    return out, undecodable, next_offsets


def commit(consumer: Consumer, next_offsets: dict[tuple[str, int], int]) -> None:
    if next_offsets:
        consumer.commit(
            offsets=[TopicPartition(t, p, o) for (t, p), o in next_offsets.items()],
            asynchronous=False,
        )


def lag(consumer: Consumer, topics: list[str]) -> dict[str, int]:
    metadata = consumer.list_topics(timeout=10)
    partitions = [TopicPartition(t, p) for t in topics for p in metadata.topics[t].partitions]
    result = {}
    for tp in consumer.committed(partitions, timeout=10):
        _low, high = consumer.get_watermark_offsets(tp, timeout=10)
        committed = tp.offset if tp.offset >= 0 else 0
        result[f"{tp.topic}:{tp.partition}"] = max(high - committed, 0)
    return result


# ---------------------------------------------------------------- run


def run_once() -> SinkStats:
    from gridtime.storage.tables import append_bronze, existing_hashes

    settings = get_settings()
    topics = [settings.topic_for(d) for d in settings.dataset_ids]
    consumer = make_consumer(settings)
    try:
        batch, undecodable, next_offsets = consume_available(consumer, topics)
        stats = SinkStats(consumed=len(batch), undecodable=undecodable)
        if batch:
            ingested_at = datetime.now(UTC)
            rows, stats.rejected = to_rows(batch, ingested_at)
            new_rows: list[BronzeRow] = []
            for dataset_id in {r.dataset_id for r in rows}:
                ds_rows = [r for r in rows if r.dataset_id == dataset_id]
                # Look back as far as the poller does, plus a day of margin.
                oldest = min(r.transaction_time for r in ds_rows)
                window = timedelta(minutes=settings.lookback_minutes(dataset_id), days=1)
                new_rows += drop_stored(ds_rows, existing_hashes(dataset_id, oldest - window))
            append_bronze(new_rows)
            stats.new_rows = len(new_rows)
            stats.duplicates = len(batch) - stats.rejected - len(new_rows)
        commit(consumer, next_offsets)  # only after Bronze has the rows
        stats.lag_after = lag(consumer, topics)
    finally:
        consumer.close()
    log.info(
        "consumed=%d new=%d duplicate=%d rejected=%d undecodable=%d lag=%d",
        stats.consumed, stats.new_rows, stats.duplicates, stats.rejected,
        stats.undecodable, stats.total_lag,
    )
    return stats


def run_forever(interval_s: float = 5) -> None:
    ensure_topics()
    while True:
        try:
            run_once()
        except Exception:
            log.exception("sink iteration failed; retrying")
        time.sleep(interval_s)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="drain the topics once and exit")
    if parser.parse_args().once:
        ensure_topics()
        run_once()
    else:
        run_forever()
