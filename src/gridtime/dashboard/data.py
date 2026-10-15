"""Queries behind the dashboard. Pure functions over Arrow tables, so they are testable.

The dashboard reads Silver for everything about values, and Bronze only for
pipeline health (what each poll wrote).
"""

from datetime import datetime

import duckdb
import pyarrow as pa

from gridtime.transform import asof
from gridtime.transform.measures import BY_NAME

STAGE_LABELS = {
    "near_real_time": "Near real-time (ods175)",
    "historical": "Historical (ods165)",
}

DASHBOARD_MEASURES = [
    "system_imbalance",
    "ace",
    "price_up_marginal",
    "price_down_marginal",
]


def label(measure: str) -> str:
    return BY_NAME[measure].label


def unit(measure: str) -> str:
    return BY_NAME[measure].unit.replace("EUR", "€")


def _to_table(rel: duckdb.DuckDBPyRelation) -> pa.Table:
    result = rel.arrow()
    return result.read_all() if hasattr(result, "read_all") else result


def known_at(silver: pa.Table, measure: str, at: datetime | None) -> pa.Table:
    """One measure, as GridTime knew it at `at` (None = now)."""
    snapshot = asof.as_of(silver, at)
    return snapshot.filter(pa.compute.equal(snapshot["measure"], measure))


def version_distribution(silver: pa.Table, measure: str) -> pa.Table:
    """How many minutes have 1, 2, 3… stored versions of this measure."""
    con = duckdb.connect()
    con.register("silver", silver)
    return _to_table(
        con.sql(
            """
            SELECT versions, count(*) AS minutes
            FROM (
                SELECT valid_from, count(*) AS versions
                FROM silver WHERE measure = $measure GROUP BY valid_from
            )
            GROUP BY versions ORDER BY versions
            """,
            params={"measure": measure},
        )
    )


def quality_by_stage(current: pa.Table) -> pa.Table:
    con = duckdb.connect()
    con.register("cur", current)
    return _to_table(
        con.sql(
            """
            SELECT stage, coalesce(quality_status, 'missing') AS quality_status,
                   count(*) AS value_count
            FROM cur GROUP BY ALL ORDER BY stage, quality_status
            """
        )
    )


def transaction_times(silver: pa.Table) -> list[datetime]:
    """Every moment at which Silver learned something new, oldest first."""
    con = duckdb.connect()
    con.register("silver", silver)
    result = _to_table(
        con.sql("SELECT DISTINCT transaction_time FROM silver ORDER BY transaction_time")
    )
    return result.column("transaction_time").to_pylist()


def ingest_batches(bronze: pa.Table) -> pa.Table:
    """Rows written per poll, per feed (from Bronze)."""
    con = duckdb.connect()
    con.register("bronze", bronze)
    return _to_table(
        con.sql(
            """
            SELECT dataset_id, transaction_time, count(*) AS rows
            FROM bronze GROUP BY ALL ORDER BY transaction_time
            """
        )
    )
