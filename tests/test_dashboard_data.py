from datetime import UTC, datetime, timedelta

import pyarrow as pa

from gridtime.dashboard import data
from gridtime.ingestion.schemas import to_bronze_row
from gridtime.storage.tables import ARROW_SCHEMA
from gridtime.transform import silver

MINUTE = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


def silver_rows() -> pa.Table:
    def raw(v):
        return {"datetime": MINUTE.isoformat(), "systemimbalance": v,
                "qualitystatus": "NotValidated"}

    bronze = pa.Table.from_pylist(
        [
            to_bronze_row("ods175", raw(-300.0), MINUTE + timedelta(minutes=1), "u").model_dump(),
            to_bronze_row("ods175", raw(-310.0), MINUTE + timedelta(minutes=5), "u").model_dump(),
            to_bronze_row("ods165", raw(-320.0), MINUTE + timedelta(hours=18), "u").model_dump(),
        ],
        schema=ARROW_SCHEMA,
    )
    cand = silver.explode(bronze)
    return silver.new_versions(cand, cand.slice(0, 0))


def test_known_at_rewinds_to_earlier_value():
    s = silver_rows()
    early = data.known_at(s, "system_imbalance", MINUTE + timedelta(minutes=2)).to_pylist()
    assert [r["value"] for r in early] == [-300.0]
    latest = data.known_at(s, "system_imbalance", None).to_pylist()
    assert [(r["stage"], r["value"]) for r in latest] == [("historical", -320.0)]


def test_version_distribution_counts_revisions():
    dist = data.version_distribution(silver_rows(), "system_imbalance").to_pylist()
    assert dist == [{"versions": 3, "minutes": 1}]


def test_transaction_times_are_sorted_and_distinct():
    times = data.transaction_times(silver_rows())
    assert times == sorted(set(times))
    assert len(times) == 3
