"""Write Silver on a branch, audit via Trino/Soda, then merge the audited commit.

The fixed branch corresponds to Trino's lakehouse_wap catalog. A host lock prevents
parallel local writers. Production multi-host use needs a distributed lease.
Failed branches are tagged before reuse, so their evidence is retained.
"""

import fcntl
import json
from contextlib import contextmanager
from datetime import UTC, datetime

from gridtime.config import get_settings
from gridtime.quality.scan import scan_layer
from gridtime.storage.catalog import nessie_catalog
from gridtime.storage.nessie import Nessie
from gridtime.transform import silver


@contextmanager
def writer_lock():
    path = get_settings().data_path / "silver-wap.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def publish(nessie: Nessie, branch: str, base_hash: str, audit=scan_layer) -> dict:
    candidate = nessie.ref(branch)
    report = audit("silver", "lakehouse_wap")
    if not report.get("passed"):
        raise RuntimeError("Audit did not pass; refusing publication")
    if nessie.ref(branch)["hash"] != candidate["hash"]:
        raise RuntimeError("Branch changed during audit; refusing publication")
    if candidate["hash"] == base_hash:
        if nessie.ref("main")["hash"] != base_hash:
            raise RuntimeError("Main changed during audit; retry from a fresh branch")
        return {"audited_hash": candidate["hash"], "audit_report": report["report_path"]}
    # Never refresh expected main hash: a concurrent main writer requires a new run.
    response = nessie.merge(candidate, base_hash)
    if response and response.get("wasApplied") is False:
        raise RuntimeError("Nessie did not apply the audited merge")
    return {"audited_hash": candidate["hash"], "audit_report": report["report_path"]}


def run() -> dict:
    s = get_settings()
    if s.catalog_backend != "nessie":
        raise RuntimeError("Silver WAP requires Nessie; SQLite is a frozen recovery catalog")
    if s.wap_branch != "silver-wap":
        raise ValueError("WAP branch must match the static lakehouse_wap Trino catalog")
    with writer_lock():
        nessie = Nessie()
        base = nessie.ref("main")
        nessie.reset_wap(s.wap_branch, base)
        try:
            stats = silver.run(nessie_catalog(s.wap_branch))
            stats.update(publish(nessie, s.wap_branch, base["hash"]))
            stats["silver_total_rows"] = (
                nessie_catalog().load_table("silver.balancing_minute").scan().count()
            )
            return stats
        except Exception:
            failed = nessie.ref(s.wap_branch)
            tag = "audit-failed-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            nessie.create(tag, failed, kind="TAG")
            raise


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
