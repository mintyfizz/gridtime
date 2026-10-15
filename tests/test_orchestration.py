"""The Dagster graph loads, is wired as intended, and runs Silver → Gold end to end."""

from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest
from dagster import AssetKey, materialize
from dagster_dbt import DbtCliResource
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
    assert crons == {"poll_nrt": "*/5 * * * *", "poll_historical": "7 * * * *"}


def test_gold_depends_on_silver():
    graph = defs.resolve_asset_graph()
    stg = AssetKey(["staging", "stg_silver__versions"])
    assert assets.SILVER in graph.get(stg).parent_keys
    assert stg in graph.get(AssetKey(["gold", "fct_revisions"])).parent_keys
    assert {assets.BRONZE_NRT, assets.BRONZE_HISTORICAL} <= graph.get(assets.SILVER).parent_keys


@pytest.fixture
def lakehouse(tmp_path, monkeypatch):
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/wh"
    )
    monkeypatch.setattr(tables, "get_catalog", lambda: catalog)
    settings = get_settings()
    monkeypatch.setattr(settings, "data_dir", str(tmp_path / "data"))
    monkeypatch.setenv("GRIDTIME_SILVER_PARQUET", str(settings.silver_export_path))
    monkeypatch.setenv("GRIDTIME_GOLD_DB", str(settings.gold_db_path))
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


def test_silver_then_gold_end_to_end(lakehouse):
    result = materialize(
        [assets.balancing_minute, assets.silver_has_no_duplicate_versions, assets.gold_models],
        resources={"dbt": DbtCliResource(project_dir=assets.dbt_project)},
    )
    assert result.success

    silver_meta = next(
        e.materialization.metadata
        for e in result.get_asset_materialization_events()
        if e.asset_key == assets.SILVER
    )
    assert silver_meta["new_versions"].value == 60 * 12  # 60 bronze rows × 12 measures
    checks = result.get_asset_check_evaluations()
    assert checks and all(c.passed for c in checks)

    con = duckdb.connect(str(lakehouse.gold_db_path), read_only=True)
    stats = con.sql(
        "SELECT minutes_compared, share_changed, max_abs_revision "
        "FROM gold.agg_revision_stats WHERE measure = 'system_imbalance'"
    ).fetchone()
    assert stats == (30, pytest.approx(10 / 30), 2.0)
    assert con.sql("SELECT count(*) FROM gold.fct_quarter_hour").fetchone()[0] == 2
