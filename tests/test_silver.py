"""Bitemporal behaviour of the Silver layer."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from gridtime.ingestion.schemas import to_bronze_row
from gridtime.storage import tables
from gridtime.storage.tables import ARROW_SCHEMA
from gridtime.transform import asof, silver
from gridtime.transform.measures import HISTORICAL, NRT

MINUTE = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


def at(minutes: int) -> datetime:
    """A transaction time `minutes` after the measured minute."""
    return MINUTE + timedelta(minutes=minutes)


def payload(**fields) -> dict:
    base = {
        "datetime": MINUTE.isoformat(),
        "resolutioncode": "PT1M",
        "quarterhour": MINUTE.isoformat(),
        "qualitystatus": "NotValidated",
    }
    return base | fields


def bronze(*rows: tuple[str, dict, datetime]) -> pa.Table:
    return pa.Table.from_pylist(
        [to_bronze_row(ds, raw, received, "u").model_dump() for ds, raw, received in rows],
        schema=ARROW_SCHEMA,
    )


def values(table: pa.Table, measure: str) -> list:
    return [r["value"] for r in table.to_pylist() if r["measure"] == measure]


# ---------------------------------------------------------------- mapping


def test_renamed_fields_map_to_one_canonical_measure():
    rows = silver.explode(
        bronze(
            (NRT, payload(mfrrup=441.0), at(1)),
            (HISTORICAL, payload(upwardavailablemfrrprice=440.0), at(900)),
        )
    )
    mfrr = [(r["stage"], r["value"]) for r in rows.to_pylist() if r["measure"] == "price_mfrr_up"]
    assert sorted(mfrr) == [("historical", 440.0), ("near_real_time", 441.0)]


def test_json_null_becomes_null_value():
    rows = silver.explode(bronze((NRT, payload(systemimbalance=None), at(1))))
    assert values(rows, "system_imbalance") == [None]


def test_valid_to_follows_resolution():
    row = silver.explode(bronze((NRT, payload(systemimbalance=1.0), at(1)))).to_pylist()[0]
    assert row["valid_to"] - row["valid_from"] == timedelta(minutes=1)


# ---------------------------------------------------------------- versioning


def test_unchanged_refetch_creates_no_version():
    empty = silver.explode(bronze()).slice(0, 0)
    cand = silver.explode(
        bronze(
            (NRT, payload(systemimbalance=-300.0), at(1)),
            (NRT, payload(systemimbalance=-300.0, extra="ignored-field"), at(2)),
        )
    )
    out = silver.new_versions(cand, empty)
    assert values(out, "system_imbalance") == [-300.0]


def test_changed_value_creates_version():
    empty = silver.explode(bronze()).slice(0, 0)
    cand = silver.explode(
        bronze(
            (NRT, payload(systemimbalance=-300.0), at(1)),
            (NRT, payload(systemimbalance=-310.0), at(5)),
        )
    )
    assert values(silver.new_versions(cand, empty), "system_imbalance") == [-300.0, -310.0]


def test_quality_flag_change_alone_creates_version():
    empty = silver.explode(bronze()).slice(0, 0)
    cand = silver.explode(
        bronze(
            (NRT, payload(systemimbalance=-300.0), at(1)),
            (NRT, payload(systemimbalance=-300.0, qualitystatus="DataIssue"), at(5)),
        )
    )
    out = [r for r in silver.new_versions(cand, empty).to_pylist()
           if r["measure"] == "system_imbalance"]
    assert [r["quality_status"] for r in out] == ["NotValidated", "DataIssue"]


def test_historical_confirmation_is_recorded_even_if_value_unchanged():
    empty = silver.explode(bronze()).slice(0, 0)
    cand = silver.explode(
        bronze(
            (NRT, payload(systemimbalance=-300.0), at(1)),
            (HISTORICAL, payload(systemimbalance=-300.0), at(900)),
        )
    )
    out = [r for r in silver.new_versions(cand, empty).to_pylist()
           if r["measure"] == "system_imbalance"]
    assert [r["stage"] for r in out] == ["near_real_time", "historical"]


def test_prior_state_suppresses_duplicate_across_runs():
    first = silver.new_versions(
        silver.explode(bronze((NRT, payload(systemimbalance=-300.0), at(1)))),
        silver.explode(bronze()).slice(0, 0),
    )
    second_batch = silver.explode(bronze((NRT, payload(systemimbalance=-300.0), at(3))))
    assert silver.new_versions(second_batch, silver.latest_versions(first)).num_rows == 0


# ---------------------------------------------------------------- as-of reads


@pytest.fixture
def three_versions() -> pa.Table:
    cand = silver.explode(
        bronze(
            (NRT, payload(systemimbalance=-300.0), at(1)),
            (NRT, payload(systemimbalance=-310.0), at(5)),
            (HISTORICAL, payload(systemimbalance=-320.0), at(900)),
        )
    )
    return silver.new_versions(cand, silver.explode(bronze()).slice(0, 0))


@pytest.mark.parametrize(
    ("known_at", "expected"),
    [(at(0), []), (at(1), [-300.0]), (at(4), [-300.0]), (at(5), [-310.0]), (at(901), [-320.0])],
)
def test_as_of_returns_value_known_at_that_time(three_versions, known_at, expected):
    assert values(asof.as_of(three_versions, known_at), "system_imbalance") == expected


def test_history_lists_every_version_in_order(three_versions):
    hist = asof.history(three_versions, MINUTE, "system_imbalance")
    assert [r["value"] for r in hist.to_pylist()] == [-300.0, -310.0, -320.0]


def test_revision_compares_first_publication_with_historical(three_versions):
    rev = asof.revisions(three_versions, "system_imbalance").to_pylist()
    assert len(rev) == 1
    assert rev[0]["first_value"] == -300.0
    assert rev[0]["latest_value"] == -320.0
    assert rev[0]["revision"] == -20.0
    assert rev[0]["versions"] == 3


# ---------------------------------------------------------------- incremental run


@pytest.fixture
def local_catalog(tmp_path, monkeypatch):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/wh"
    )
    monkeypatch.setattr(tables, "get_catalog", lambda: catalog)
    tables.create_tables()
    return catalog


def test_run_is_incremental_and_idempotent(local_catalog):
    tables.bronze_table().append(bronze((NRT, payload(systemimbalance=-300.0), at(1))))
    first = silver.run()
    assert first["new_versions"] == 12  # one per canonical measure

    assert silver.run()["bronze_rows"] == 0  # watermark: nothing to reprocess

    tables.bronze_table().append(bronze((NRT, payload(systemimbalance=-310.0), at(5))))
    third = silver.run()
    assert third["bronze_rows"] == 1
    assert third["new_versions"] == 1  # only system_imbalance changed

    stored = tables.silver_table().scan().to_arrow()
    assert values(asof.current(stored), "system_imbalance") == [-310.0]
    assert silver.read_watermark(tables.silver_table()) == at(5)
