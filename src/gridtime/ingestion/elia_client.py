"""Thin client for the Elia Open Data API (Opendatasoft Explore API v2.1)."""

import logging
import time
from datetime import datetime

import httpx

log = logging.getLogger(__name__)


class EliaClient:
    def __init__(self, base_url: str, page_size: int = 100, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.max_retries = max_retries
        self._http = httpx.Client(timeout=30, headers={"User-Agent": "gridtime/0.1"})

    def records_url(self, dataset_id: str) -> str:
        return f"{self.base_url}/catalog/datasets/{dataset_id}/records"

    def fetch_since(self, dataset_id: str, since: datetime) -> list[dict]:
        """All records with datetime >= since, newest first, across pages."""
        records: list[dict] = []
        offset = 0
        while True:
            page = self._get(
                self.records_url(dataset_id),
                params={
                    "where": f"datetime >= '{since.isoformat()}'",
                    "order_by": "datetime desc",
                    "limit": self.page_size,
                    "offset": offset,
                },
            )
            batch = page.get("results", [])
            records.extend(batch)
            if len(batch) < self.page_size:
                return records
            offset += self.page_size

    def _get(self, url: str, params: dict) -> dict:
        """GET with exponential backoff on network errors and 429/5xx."""
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._http.get(url, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"retryable status {resp.status_code}", request=resp.request, response=resp
                    )
                resp.raise_for_status()
                return resp.json()
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                retryable = isinstance(exc, httpx.TransportError) or (
                    exc.response.status_code == 429 or exc.response.status_code >= 500
                )
                if not retryable or attempt == self.max_retries:
                    raise
                wait = 2**attempt
                log.warning("Request failed (%s), retry %d in %ds", exc, attempt, wait)
                time.sleep(wait)
        raise RuntimeError("unreachable")

    def close(self) -> None:
        self._http.close()
