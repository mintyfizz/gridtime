"""Prove an invalid Silver candidate is blocked, without changing main."""

import json
from datetime import UTC, datetime

from gridtime.config import get_settings
from gridtime.quality.scan import QualityError
from gridtime.storage.catalog import nessie_catalog
from gridtime.storage.nessie import Nessie
from gridtime.storage.tables import SILVER_TABLE
from gridtime.transform.wap import publish, writer_lock


def run() -> dict:
    s = get_settings()
    with writer_lock():
        nessie = Nessie()
        main = nessie.ref("main")
        original = nessie_catalog().load_table(SILVER_TABLE)
        original_count = original.scan().count()
        nessie.reset_wap(s.wap_branch, main)
        table = nessie_catalog(s.wap_branch).load_table(SILVER_TABLE)
        row = table.scan(limit=1).to_arrow()
        if row.num_rows != 1:
            raise RuntimeError("Demo needs an existing Silver row")
        table.append(row)  # duplicate version, confined to the audit branch
        blocked = False
        try:
            publish(nessie, s.wap_branch, main["hash"])
        except QualityError:
            blocked = True
        current = nessie_catalog().load_table(SILVER_TABLE)
        unchanged = (
            nessie.ref("main")["hash"] == main["hash"]
            and current.metadata_location == original.metadata_location
            and current.scan().count() == original_count
        )
        tag = "demo-rejected-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        nessie.create(tag, nessie.ref(s.wap_branch), kind="TAG")
        report = {
            "blocked": blocked,
            "main_unchanged": unchanged,
            "main_rows": original_count,
            "candidate_rows": table.scan().count(),
            "evidence_tag": tag,
            "main_hash": main["hash"],
        }
        path = s.data_path / "quality" / "failure-demo.json"
        path.write_text(json.dumps(report, indent=2))
        # Retain the invalid candidate in the tag, leave the reusable branch clean.
        nessie.reset_wap(s.wap_branch, main)
        if not blocked or not unchanged:
            raise RuntimeError(f"Failure-path proof failed: {report}")
        return report


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
