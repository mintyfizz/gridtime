from datetime import UTC, datetime

import pyarrow as pa

from gridtime.dashboard import data
from gridtime.ingestion.schemas import to_bronze_row
from gridtime.storage.tables import ARROW_SCHEMA

T0 = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
T_FIRST = datetime(2026, 10, 14, 12, 1, tzinfo=UTC)
T_LATER = datetime(2026, 10, 14, 12, 5, tzinfo=UTC)
T_NEXT_DAY = datetime(2026, 10, 15, 6, 0, tzinfo=UTC)


def bronze(*rows) -> pa.Table:
    return pa.Table.from_pylist([r.model_dump() for r in rows], schema=ARROW_SCHEMA)


def row(dataset, value, received):
    raw = {"datetime": T0.isoformat(), "systemimbalance": value, "qualitystatus": "DataIssue"}
    return to_bronze_row(dataset, raw, received, "u")


def sample():
    return bronze(
        row("ods175", -300.0, T_FIRST),    # first preliminary
        row("ods175", -310.0, T_LATER),    # revised preliminary
        row("ods165", -320.0, T_NEXT_DAY),  # validated
    )


def test_current_view_keeps_latest_version_per_feed():
    cur = data.current_view(sample()).to_pylist()
    by_feed = {r["dataset_id"]: r["systemimbalance"] for r in cur}
    assert by_feed == {"ods175": -310.0, "ods165": -320.0}


def test_version_counts_detect_revision():
    counts = {
        (r["dataset_id"], r["versions"]): r["minutes"]
        for r in data.version_counts(sample()).to_pylist()
    }
    assert counts == {("ods175", 2): 1, ("ods165", 1): 1}


def test_revision_is_validated_minus_latest_preliminary():
    rev = data.revisions(data.current_view(sample()), "systemimbalance").to_pylist()
    assert len(rev) == 1
    assert rev[0]["revision"] == -10.0
