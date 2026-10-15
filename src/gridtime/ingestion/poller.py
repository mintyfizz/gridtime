"""Fetch → validate → deduplicate → append to Bronze.

Run once:        python -m gridtime.ingestion.poller --once
Run forever:     python -m gridtime.ingestion.poller
"""

import argparse
import logging
import time
from datetime import UTC, datetime, timedelta

from pydantic import ValidationError

from gridtime.config import get_settings
from gridtime.ingestion.elia_client import EliaClient
from gridtime.ingestion.schemas import BronzeRow, to_bronze_row
from gridtime.storage.tables import append_bronze, existing_hashes

log = logging.getLogger("gridtime.poller")


def poll_dataset(client: EliaClient, dataset_id: str, lookback: timedelta) -> int:
    received_at = datetime.now(UTC)
    since = received_at - lookback
    raw_records = client.fetch_since(dataset_id, since)
    if not raw_records:
        # Elia occasionally serves an empty page while a dataset refreshes; the next
        # poll catches up. Logged so a real outage doesn't go unnoticed.
        log.warning("%s: no records since %s", dataset_id, since.isoformat())

    rows: list[BronzeRow] = []
    rejected = 0
    for raw in raw_records:
        try:
            rows.append(
                to_bronze_row(dataset_id, raw, received_at, client.records_url(dataset_id))
            )
        except ValidationError as exc:
            rejected += 1
            log.warning("%s: rejected record (%s)", dataset_id, exc.errors()[0]["msg"])

    # Idempotency: skip payloads we've already stored. A *changed* value for the
    # same quarter-hour has a new hash, so revisions are kept as new versions.
    seen = existing_hashes(dataset_id, since=since - timedelta(days=1))
    new_rows = [r for r in rows if r.payload_hash not in seen]
    append_bronze(new_rows)

    log.info(
        "%s: fetched=%d new=%d duplicate=%d rejected=%d",
        dataset_id, len(raw_records), len(new_rows), len(rows) - len(new_rows), rejected,
    )
    return len(new_rows)


def run(once: bool) -> None:
    settings = get_settings()
    client = EliaClient(settings.elia_base_url, page_size=settings.elia_page_size)
    try:
        while True:
            for dataset_id in settings.dataset_ids:
                lookback = timedelta(minutes=settings.lookback_minutes(dataset_id))
                try:
                    poll_dataset(client, dataset_id, lookback)
                except Exception:  # one bad dataset must not stop the others
                    log.exception("%s: poll failed", dataset_id)
            if once:
                return
            time.sleep(settings.poll_interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="poll a single time and exit")
    run(parser.parse_args().once)
