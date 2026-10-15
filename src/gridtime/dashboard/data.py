"""Queries behind the dashboard. Pure functions over an Arrow table, so they are testable.

Milestone 1 reads Bronze directly and parses the JSON payloads. Once the Silver
layer exists (milestone 2) these queries move there.
"""

import duckdb
import pyarrow as pa

FEEDS = {
    "ods175": "Preliminary (ods175)",
    "ods165": "Validated (ods165)",
}

# Fields present under the same name in both feeds.
MEASURES = {
    "systemimbalance": ("System imbalance", "MW"),
    "ace": ("Area control error", "MW"),
    "marginalincrementalprice": ("Upward activation price", "€/MWh"),
    "marginaldecrementalprice": ("Downward activation price", "€/MWh"),
}


def _to_table(rel: duckdb.DuckDBPyRelation) -> pa.Table:
    """DuckDB >= 1.4 returns a stream from .arrow(); always hand back a Table."""
    result = rel.arrow()
    return result.read_all() if hasattr(result, "read_all") else result


def _con(bronze: pa.Table) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.register("bronze", bronze)
    return con


def current_view(bronze: pa.Table) -> pa.Table:
    """Latest known version of every (feed, minute), with measures parsed out."""
    measure_cols = ",\n".join(
        f"TRY_CAST(json_extract(payload, '$.{m}') AS DOUBLE) AS {m}" for m in MEASURES
    )
    return _to_table(
        _con(bronze).sql(
            f"""
        WITH parsed AS (
            SELECT dataset_id,
                   valid_time,
                   transaction_time,
                   json_extract_string(payload, '$.qualitystatus') AS qualitystatus,
                   {measure_cols},
                   row_number() OVER (
                       PARTITION BY dataset_id, valid_time ORDER BY transaction_time DESC
                   ) AS rn
            FROM bronze
        )
        SELECT * EXCLUDE (rn) FROM parsed WHERE rn = 1 ORDER BY valid_time
        """
        )
    )


def version_counts(bronze: pa.Table) -> pa.Table:
    """How many distinct versions each minute has per feed (1 = never revised)."""
    return _to_table(
        _con(bronze).sql(
            """
        SELECT dataset_id, versions, count(*) AS minutes
        FROM (
            SELECT dataset_id, valid_time, count(*) AS versions
            FROM bronze GROUP BY ALL
        )
        GROUP BY ALL ORDER BY dataset_id, versions
        """
        )
    )


def revisions(current: pa.Table, measure: str) -> pa.Table:
    """Minutes present in both feeds: preliminary vs validated value for one measure."""
    if measure not in MEASURES:
        raise ValueError(f"unknown measure {measure}")
    con = duckdb.connect()
    con.register("cur", current)
    return _to_table(
        con.sql(
            f"""
        SELECT p.valid_time,
               p.{measure} AS preliminary,
               v.{measure} AS validated,
               v.{measure} - p.{measure} AS revision
        FROM cur p
        JOIN cur v USING (valid_time)
        WHERE p.dataset_id = 'ods175' AND v.dataset_id = 'ods165'
          AND p.{measure} IS NOT NULL AND v.{measure} IS NOT NULL
        ORDER BY valid_time
        """
        )
    )


def ingest_batches(bronze: pa.Table) -> pa.Table:
    """Rows written per poll, per feed."""
    return _to_table(
        _con(bronze).sql(
            """
        SELECT dataset_id, transaction_time, count(*) AS rows
        FROM bronze GROUP BY ALL ORDER BY transaction_time
        """
        )
    )
