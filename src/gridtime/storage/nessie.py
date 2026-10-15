"""Small Nessie v1 client. Every branch mutation uses optimistic concurrency."""

from urllib.parse import quote

import httpx

from gridtime.config import get_settings


class Nessie:
    def __init__(self):
        self.url = get_settings().nessie_uri.rstrip("/") + "/api/v1/trees"

    def request(self, method: str, path: str, **kwargs):
        response = httpx.request(method, self.url + path, timeout=60, **kwargs)
        response.raise_for_status()
        return response.json() if response.content else None

    def ref(self, name: str) -> dict:
        return self.request("GET", f"/tree/{quote(name, safe='')}")

    def create(self, name: str, source: dict, kind: str = "BRANCH") -> dict:
        return self.request(
            "POST",
            "/tree",
            params={"sourceRefName": source["name"]},
            json={"type": kind, "name": name, "hash": source["hash"]},
        )

    def reset_wap(self, name: str, source: dict):
        try:
            current = self.ref(name)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            self.create(name, source)
            return
        self.request(
            "PUT",
            f"/branch/{quote(name, safe='')}",
            params={"expectedHash": current["hash"]},
            json=source,
        )

    def merge(self, source: dict, expected_main_hash: str):
        return self.request(
            "POST",
            "/branch/main/merge",
            params={"expectedHash": expected_main_hash},
            json={"fromRefName": source["name"], "fromHash": source["hash"]},
        )
