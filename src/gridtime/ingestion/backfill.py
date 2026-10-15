"""Backfill Elia's historical dataset (ods165) through Redpanda.

The Opendatasoft records API stops at 10,000 records per query, so the backfill
uses the bulk export endpoint, one week at a time. It only *publishes*: the Bronze
sink (run by Dagster, or `make ingest`) drains the topic like any other poll, so
Iceberg still has a single writer.

Backfilled observations are stamped with the time of the backfill. That is the
truth: GridTime did not know these values before today, so as-of queries for
earlier moments correctly do not see them.

    python -m gridtime.ingestion.backfill --days 56
"""

import argparse
import logging
from datetime import UTC, datetime, timedelta

import httpx

from gridtime.config import get_settings
from gridtime.ingestion.schemas import EliaRecord
from gridtime.ingestion.stream import Observation, ensure_topics, make_producer
from gridtime.transform.measures import HISTORICAL

log = logging.getLogger("gridtime.backfill")


def export_url(base_url: str, dataset_id: str) -> str:
    return f"{base_url.rstrip('/')}/catalog/datasets/{dataset_id}/exports/json"


def fetch_window(client: httpx.Client, url: str, start: datetime, end: datetime) -> list[dict]:
    params = {
        "where": f"datetime >= '{start.isoformat()}' and datetime < '{end.isoformat()}'",
        "order_by": "datetime",
    }
    resp = client.get(url, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def windows(start: datetime, end: datetime, step: timedelta = timedelta(days=7)):
    cursor = start
    while cursor < end:
        yield cursor, min(cursor + step, end)
        cursor += step


def backfill(days: int, dataset_id: str = HISTORICAL) -> int:
    settings = get_settings()
    ensure_topics(settings)
    end = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    url = export_url(settings.elia_base_url, dataset_id)
    topic = settings.topic_for(dataset_id)
    producer = make_producer(settings)
    failures: list[str] = []
    total = 0

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(str(err))

    with httpx.Client(headers={"User-Agent": "gridtime/0.5"}) as client:
        for lo, hi in windows(start, end):
            records = fetch_window(client, url, lo, hi)
            received_at = datetime.now(UTC)
            published = 0
            for raw in records:
                EliaRecord.model_validate(raw)  # export data is clean; fail loudly if not
                obs = Observation(dataset_id, received_at, url, raw)
                producer.produce(topic, key=obs.key(), value=obs.encode(), on_delivery=on_delivery)
                published += 1
                if published % 5000 == 0:
                    producer.poll(0)
            remaining = producer.flush(60)
            if failures or remaining:
                raise RuntimeError(f"delivery failed: {failures[:1]} remaining={remaining}")
            total += published
            log.info("%s → %s: published %d records to %s", lo.date(), hi.date(), published, topic)
    log.info("Backfill done: %d records. The Bronze sink will ingest them on its next run.", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=56)
    backfill(parser.parse_args().days)
