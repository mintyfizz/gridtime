"""Bitemporal reads over Silver.

    as_of(silver, t)       what every (minute, measure) looked like, as known at t
    current(silver)        the latest known value (as_of now)
    history(silver, ...)   every version of one (minute, measure)
    revisions(silver, m)   first-published vs latest value per minute
"""

from datetime import datetime

import duckdb
import pyarrow as pa


def _to_table(rel: duckdb.DuckDBPyRelation) -> pa.Table:
    result = rel.arrow()
    return result.read_all() if hasattr(result, "read_all") else result


def as_of(silver: pa.Table, known_at: datetime | None = None) -> pa.Table:
    con = duckdb.connect()
    con.register("silver", silver)
    where = "WHERE transaction_time <= $known_at" if known_at else ""
    params = {"known_at": known_at} if known_at else {}
    return _to_table(
        con.sql(
            f"""
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY valid_from, measure ORDER BY transaction_time DESC
                ) AS rn
                FROM silver {where}
            ) WHERE rn = 1
            ORDER BY valid_from, measure
            """,
            params=params,
        )
    )


def current(silver: pa.Table) -> pa.Table:
    return as_of(silver, None)


def history(silver: pa.Table, valid_from: datetime, measure: str) -> pa.Table:
    con = duckdb.connect()
    con.register("silver", silver)
    return _to_table(
        con.sql(
            """
            SELECT * FROM silver
            WHERE valid_from = $valid_from AND measure = $measure
            ORDER BY transaction_time
            """,
            params={"valid_from": valid_from, "measure": measure},
        )
    )


def revisions(silver: pa.Table, measure: str) -> pa.Table:
    """Per minute: the first value ever published vs the latest one.

    Only minutes that have reached the historical stage are included, so the
    comparison is near-real-time publication vs the daily historical figure.
    """
    con = duckdb.connect()
    con.register("silver", silver)
    return _to_table(
        con.sql(
            """
            WITH v AS (
                SELECT *,
                       row_number() OVER (PARTITION BY valid_from ORDER BY transaction_time)
                           AS first_rank,
                       row_number() OVER (PARTITION BY valid_from ORDER BY transaction_time DESC)
                           AS last_rank,
                       count(*) OVER (PARTITION BY valid_from) AS versions
                FROM silver WHERE measure = $measure
            )
            SELECT f.valid_from,
                   f.value AS first_value,
                   l.value AS latest_value,
                   l.value - f.value AS revision,
                   f.transaction_time AS first_seen,
                   l.transaction_time AS latest_seen,
                   l.quality_status AS latest_quality,
                   f.versions
            FROM v f JOIN v l USING (valid_from)
            WHERE f.first_rank = 1 AND l.last_rank = 1
              AND f.stage = 'near_real_time' AND l.stage = 'historical'
            ORDER BY valid_from
            """,
            params={"measure": measure},
        )
    )
