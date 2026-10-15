"""Bronze tables created before milestone 4 gain the new columns without a rewrite."""

from datetime import UTC, datetime

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema

from gridtime.ingestion.schemas import to_bronze_row
from gridtime.storage import tables

T = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
OLD_FIELDS = tables.BRONZE_SCHEMA.fields[:6]  # the milestone 1 schema


def test_old_bronze_table_is_evolved_in_place(tmp_path, monkeypatch):
    catalog = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/w")
    monkeypatch.setattr(tables, "get_catalog", lambda: catalog)
    catalog.create_namespace("bronze")
    old = catalog.create_table(
        tables.BRONZE_TABLE, schema=Schema(*OLD_FIELDS), partition_spec=tables.BRONZE_PARTITIONING
    )
    old_row = to_bronze_row("ods175", {"datetime": T.isoformat()}, T, "u").model_dump()
    old.append(pa.Table.from_pylist(
        [{k: old_row[k] for k in tables.ARROW_SCHEMA.names[:6]}],
        schema=pa.schema(tables.ARROW_SCHEMA.field(i) for i in range(6)),
    ))

    tables.create_tables()  # runs the evolution
    names = [f.name for f in tables.bronze_table().schema().fields]
    assert names[-2:] == ["ingested_at", "source_offset"]
    assert tables.evolve_bronze_schema() == []  # idempotent

    new_row = to_bronze_row("ods175", {"datetime": T.isoformat(), "x": 1}, T, "u")
    tables.append_bronze([new_row.model_copy(update={"ingested_at": T, "source_offset": "t:0:0"})])

    data = tables.bronze_table().scan().to_arrow().sort_by("source_offset")
    assert data.column("source_offset").to_pylist() == ["t:0:0", None]  # old row reads null


def test_silver_runs_against_unevolved_bronze(tmp_path, monkeypatch):
    """Code deployed before the schema migration must not crash (seen live on the switchover)."""
    from gridtime.storage.tables import SILVER_PARTITIONING, SILVER_SCHEMA, SILVER_TABLE
    from gridtime.transform import silver

    catalog = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/w")
    monkeypatch.setattr(tables, "get_catalog", lambda: catalog)
    catalog.create_namespace("bronze")
    catalog.create_namespace("silver")
    old = catalog.create_table(
        tables.BRONZE_TABLE, schema=Schema(*OLD_FIELDS), partition_spec=tables.BRONZE_PARTITIONING
    )
    catalog.create_table(SILVER_TABLE, schema=SILVER_SCHEMA, partition_spec=SILVER_PARTITIONING)
    row = to_bronze_row("ods175", {"datetime": T.isoformat(), "systemimbalance": 1.0}, T, "u")
    old.append(pa.Table.from_pylist(
        [{k: row.model_dump()[k] for k in tables.ARROW_SCHEMA.names[:6]}],
        schema=pa.schema(tables.ARROW_SCHEMA.field(i) for i in range(6)),
    ))
    assert silver.run()["bronze_rows"] == 1
