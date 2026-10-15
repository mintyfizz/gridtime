"""Redpanda (Kafka API) plumbing: topics, message format, producer and consumer.

Topics
    elia.<dataset_id>   every record the poller observed, valid ones only
    elia.dlq            records that failed validation, with the error

A message is one *observation*: "at received_at, Elia served this payload". The
same payload is observed many times (overlapping poll windows); deduplication
happens in the Bronze sink, so the topic stays a faithful, replayable log.
"""

import json
from dataclasses import dataclass
from datetime import datetime

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from gridtime.config import Settings, get_settings

SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class Observation:
    dataset_id: str
    received_at: datetime
    source_url: str
    payload: dict

    def key(self) -> bytes:
        # Same minute → same key → same partition, so its observations stay ordered.
        return f"{self.dataset_id}|{self.payload.get('datetime')}".encode()

    def encode(self) -> bytes:
        return json.dumps(
            {
                "dataset_id": self.dataset_id,
                "received_at": self.received_at.isoformat(),
                "source_url": self.source_url,
                "payload": self.payload,
            },
            separators=(",", ":"),
            default=str,
        ).encode()

    @classmethod
    def decode(cls, value: bytes) -> "Observation":
        data = json.loads(value)
        return cls(
            dataset_id=data["dataset_id"],
            received_at=datetime.fromisoformat(data["received_at"]),
            source_url=data["source_url"],
            payload=data["payload"],
        )


def dlq_message(dataset_id: str, received_at: datetime, raw: dict, error: str) -> bytes:
    return json.dumps(
        {
            "dataset_id": dataset_id,
            "received_at": received_at.isoformat(),
            "error": error,
            "payload": raw,
        },
        default=str,
    ).encode()


def ensure_topics(settings: Settings | None = None) -> list[str]:
    """Create the dataset topics and the DLQ if missing. Returns the topics created."""
    s = settings or get_settings()
    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    wanted = [s.topic_for(d) for d in s.dataset_ids] + [s.dlq_topic]
    existing = set(admin.list_topics(timeout=10).topics)
    retention_ms = str(s.kafka_retention_days * 24 * 3600 * 1000)
    new = [
        NewTopic(t, num_partitions=1, replication_factor=1, config={"retention.ms": retention_ms})
        for t in wanted
        if t not in existing
    ]
    if not new:
        return []
    for future in admin.create_topics(new).values():
        future.result()
    return [t.topic for t in new]


def make_producer(settings: Settings | None = None) -> Producer:
    s = settings or get_settings()
    return Producer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "enable.idempotence": True,  # no duplicates from producer retries
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 50,
        }
    )


def make_consumer(settings: Settings | None = None) -> Consumer:
    s = settings or get_settings()
    return Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": s.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after Bronze has the data
            "isolation.level": "read_committed",
        }
    )
