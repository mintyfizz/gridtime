"""Fetch from Elia → validate → publish to Redpanda.

Valid records go to elia.<dataset_id>; invalid ones to elia.dlq. Nothing is
written to Bronze here: the Bronze sink (bronze_sink.py) consumes the topics.

Run once:        python -m gridtime.ingestion.poller --once
Run forever:     python -m gridtime.ingestion.poller
"""

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from confluent_kafka import Producer
from pydantic import ValidationError

from gridtime.config import get_settings
from gridtime.ingestion.elia_client import EliaClient
from gridtime.ingestion.schemas import EliaRecord
from gridtime.ingestion.stream import (
    Observation,
    dlq_message,
    ensure_topics,
    make_producer,
)

log = logging.getLogger("gridtime.poller")


@dataclass(frozen=True)
class PollStats:
    dataset_id: str
    fetched: int
    published: int
    rejected: int
    received_at: datetime


class DeliveryError(RuntimeError):
    pass


def poll_dataset(
    client: EliaClient, producer: Producer, dataset_id: str, lookback: timedelta
) -> PollStats:
    settings = get_settings()
    received_at = datetime.now(UTC)
    since = received_at - lookback
    raw_records = client.fetch_since(dataset_id, since)
    if not raw_records:
        # Elia occasionally serves an empty page while a dataset refreshes; the next
        # poll catches up. Logged so a real outage doesn't go unnoticed.
        log.warning("%s: no records since %s", dataset_id, since.isoformat())

    failures: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    published = rejected = 0
    url = client.records_url(dataset_id)
    for raw in raw_records:
        try:
            EliaRecord.model_validate(raw)
        except ValidationError as exc:
            rejected += 1
            error = exc.errors()[0]["msg"]
            log.warning("%s: rejected record → DLQ (%s)", dataset_id, error)
            producer.produce(
                settings.dlq_topic,
                value=dlq_message(dataset_id, received_at, raw, error),
                on_delivery=on_delivery,
            )
            continue
        obs = Observation(dataset_id, received_at, url, raw)
        producer.produce(
            settings.topic_for(dataset_id), key=obs.key(), value=obs.encode(),
            on_delivery=on_delivery,
        )
        published += 1
        producer.poll(0)

    remaining = producer.flush(30)
    if failures or remaining:
        raise DeliveryError(
            f"{dataset_id}: {len(failures)} failed deliveries, {remaining} undelivered "
            f"(first error: {failures[0] if failures else 'timeout'})"
        )

    stats = PollStats(dataset_id, len(raw_records), published, rejected, received_at)
    log.info(
        "%s: fetched=%d published=%d rejected=%d",
        dataset_id, stats.fetched, stats.published, stats.rejected,
    )
    return stats


def run(once: bool) -> None:
    settings = get_settings()
    ensure_topics(settings)
    client = EliaClient(settings.elia_base_url, page_size=settings.elia_page_size)
    producer = make_producer(settings)
    try:
        while True:
            for dataset_id in settings.dataset_ids:
                lookback = timedelta(minutes=settings.lookback_minutes(dataset_id))
                try:
                    poll_dataset(client, producer, dataset_id, lookback)
                except Exception:  # one bad dataset must not stop the others
                    log.exception("%s: poll failed", dataset_id)
            if once:
                return
            time.sleep(settings.poll_interval_seconds)
    finally:
        client.close()
        producer.flush(10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="poll a single time and exit")
    run(parser.parse_args().once)
