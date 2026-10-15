"""Streaming layer: message format, sink deduplication, and a live Redpanda round trip."""

import os
import socket
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from gridtime.ingestion.bronze_sink import Consumed, drop_stored, to_rows
from gridtime.ingestion.stream import Observation

T0 = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
INGESTED = datetime(2026, 10, 15, 12, 30, tzinfo=UTC)


def obs(value: float, received_minutes: int, minute: int = 0, dataset: str = "ods175"):
    payload = {
        "datetime": (T0 + timedelta(minutes=minute)).isoformat(),
        "systemimbalance": value,
    }
    return Observation(dataset, T0 + timedelta(minutes=received_minutes), "u", payload)


def consumed(o: Observation, offset: int) -> Consumed:
    return Consumed(o, f"elia.{o.dataset_id}", 0, offset)


def test_message_round_trip():
    o = obs(-300.0, 1)
    back = Observation.decode(o.encode())
    assert back == o
    assert o.key() == b"ods175|2026-10-15T12:00:00+00:00"


def test_batch_keeps_earliest_observation_of_same_payload():
    batch = [consumed(obs(-300.0, 6), 1), consumed(obs(-300.0, 1), 0)]
    rows, rejected = to_rows(batch, INGESTED)
    assert rejected == 0 and len(rows) == 1
    assert rows[0].transaction_time == T0 + timedelta(minutes=1)
    assert rows[0].ingested_at == INGESTED
    assert rows[0].source_offset == "elia.ods175:0:0"


def test_changed_value_is_a_separate_row():
    rows, _ = to_rows([consumed(obs(-300.0, 1), 0), consumed(obs(-310.0, 6), 1)], INGESTED)
    assert sorted(r.transaction_time.minute for r in rows) == [1, 6]


def test_invalid_payload_is_rejected_not_fatal():
    bad = Observation("ods175", T0, "u", {"systemimbalance": 1.0})  # no datetime
    rows, rejected = to_rows([consumed(bad, 0), consumed(obs(1.0, 1), 1)], INGESTED)
    assert rejected == 1 and len(rows) == 1


def test_replayed_message_is_dropped_against_bronze():
    rows, _ = to_rows([consumed(obs(-300.0, 1), 0)], INGESTED)
    assert drop_stored(rows, {rows[0].payload_hash}) == []
    assert drop_stored(rows, set()) == rows


# ---------------------------------------------------------------- live broker


def _broker_up(host="localhost", port=19092) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.mark.skipif(
    not _broker_up() or os.environ.get("GRIDTIME_SKIP_KAFKA"), reason="Redpanda not running"
)
def test_live_round_trip_through_redpanda(monkeypatch):
    """Produce to a throwaway topic, then drain it with the sink's consumer logic."""
    from gridtime.config import get_settings
    from gridtime.ingestion import bronze_sink
    from gridtime.ingestion.stream import make_consumer, make_producer

    settings = get_settings()
    prefix = f"test-{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(settings, "kafka_topic_prefix", prefix)
    monkeypatch.setattr(settings, "kafka_consumer_group", f"{prefix}-group")
    from gridtime.ingestion.stream import ensure_topics

    ensure_topics(settings)
    topic = settings.topic_for("ods175")
    producer = make_producer(settings)
    for i in range(5):
        o = obs(float(i), i, minute=i)
        producer.produce(topic, key=o.key(), value=o.encode())
    assert producer.flush(10) == 0

    consumer = make_consumer(settings)
    try:
        batch, undecodable, next_offsets = bronze_sink.consume_available(consumer, [topic])
        assert len(batch) == 5 and undecodable == 0
        bronze_sink.commit(consumer, next_offsets)
        assert bronze_sink.lag(consumer, [topic]) == {f"{topic}:0": 0}
        # A second drain finds nothing new: offsets were committed.
        again, _, _ = bronze_sink.consume_available(consumer, [topic], timeout_s=3)
        assert again == []
    finally:
        consumer.close()
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
        test_topics = [t for t in admin.list_topics(timeout=10).topics if t.startswith(prefix)]
        for future in admin.delete_topics(test_topics).values():
            future.result()
