"""The Dagster graph loads, is wired as intended, and preserves isolated Silver processing."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from dagster import AssetKey
from pyiceberg.catalog.sql import SqlCatalog

from gridtime.config import get_settings
from gridtime.ingestion.schemas import to_bronze_row
from gridtime.orchestration import assets
from gridtime.orchestration.definitions import defs
from gridtime.storage import tables
from gridtime.storage.tables import ARROW_SCHEMA

START = datetime(2026, 10, 14, 10, 0, tzinfo=UTC)


def test_definitions_load():
    defs.get_repository_def().load_all_definitions()


def test_schedules():
    crons = {s.job_name: s.cron_schedule for s in defs.schedules}
    assert crons == {
        "poll_nrt": "*/5 * * * *",
        "poll_historical": "7 * * * *",
        "retrain_forecast": "13 6 * * *",
    }


def test_gold_depends_on_silver():
    graph = defs.resolve_asset_graph()
    stg = AssetKey(["staging", "stg_silver__versions"])
    assert assets.SILVER in graph.get(stg).parent_keys
    assert stg in graph.get(AssetKey(["gold", "fct_revisions"])).parent_keys
    assert graph.get(assets.SILVER).parent_keys == {assets.BRONZE}
    assert graph.get(assets.BRONZE).parent_keys == {assets.STREAM_NRT, assets.STREAM_HISTORICAL}
    assert graph.get(assets.FORECAST_LIVE).parent_keys == {assets.SILVER, assets.FORECAST_MODEL}


@pytest.fixture
def lakehouse(tmp_path, monkeypatch):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/wh"
    )
    monkeypatch.setattr(tables, "get_catalog", lambda: catalog)
    settings = get_settings()
    monkeypatch.setattr(settings, "data_dir", str(tmp_path / "data"))
    tables.create_tables()

    rows = []
    for i in range(30):
        minute = START + timedelta(minutes=i)
        raw = {
            "datetime": minute.isoformat(),
            "resolutioncode": "PT1M",
            "qualitystatus": "NotValidated",
            "systemimbalance": float(i),
        }
        rows.append(("ods175", raw, minute + timedelta(minutes=1)))
        hist = raw | {"systemimbalance": float(i) + (2.0 if i % 3 == 0 else 0.0)}
        rows.append(("ods165", hist, minute + timedelta(hours=20)))
    tables.bronze_table().append(
        pa.Table.from_pylist(
            [to_bronze_row(d, r, t, "u").model_dump() for d, r, t in rows], schema=ARROW_SCHEMA
        )
    )
    return settings


def test_incremental_silver_in_isolated_catalog(lakehouse):
    from gridtime.transform import silver

    first = silver.run()
    assert first["new_versions"] == 60 * 12
    second = silver.run()
    assert second["new_versions"] == 0
    assert tables.silver_table().scan().count() == 720


def test_soda_checks_registered():
    names = {spec.name for check in defs.asset_checks for spec in check.check_specs}
    assert {"soda_bronze", "soda_silver", "soda_gold"} <= names
