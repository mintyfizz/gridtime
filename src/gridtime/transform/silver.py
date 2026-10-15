"""Bronze → Silver: one row per (minute, measure, version).

Silver is bitemporal and append-only:
  * valid_from / valid_to   – the minute the value describes
  * transaction_time        – when GridTime first received this version

A new version is written only when something actually changed for that
(minute, measure): the value, Elia's quality flag, or the publication stage.
Re-fetching an unchanged record never creates a version, so "versions per
minute" counts real revisions.

Processing is incremental. The Bronze high-water mark is stored as a property
on each Silver snapshot, so the job needs no external state.

Run:  python -m gridtime.transform.silver
"""

import logging
from datetime import UTC, datetime

import duckdb
import pyarrow as pa
from pyiceberg.expressions import And, GreaterThan, GreaterThanOrEqual, LessThanOrEqual

from gridtime.transform.measures import STAGES, mapping_rows

log = logging.getLogger("gridtime.silver")

WATERMARK_KEY = "gridtime.bronze-watermark"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

SILVER_ARROW_SCHEMA = pa.schema(
    [
        pa.field("valid_from", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("valid_to", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("quarter_hour", pa.timestamp("us", tz="UTC")),
        pa.field("measure", pa.string(), nullable=False),
        pa.field("value", pa.float64()),
        pa.field("unit", pa.string(), nullable=False),
        pa.field("stage", pa.string(), nullable=False),
        pa.field("quality_status", pa.string()),
        pa.field("transaction_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source_dataset", pa.string(), nullable=False),
        pa.field("payload_hash", pa.string(), nullable=False),
    ]
)

STAGE_RANK = {"near_real_time": 1, "historical": 2}


def _to_table(rel: duckdb.DuckDBPyRelation) -> pa.Table:
    result = rel.arrow()
    return result.read_all() if hasattr(result, "read_all") else result


def explode(bronze: pa.Table) -> pa.Table:
    """Bronze payloads → long rows (one per mapped measure), not yet deduplicated."""
    con = duckdb.connect()
    con.register("bronze", bronze)
    con.register(
        "mapping",
        pa.table(
            dict(
                zip(
                    ["dataset_id", "field", "measure", "unit"],
                    map(list, zip(*mapping_rows(), strict=True)),
                    strict=True,
                )
            )
        ),
    )
    con.register(
        "stages",
        pa.table({"dataset_id": list(STAGES), "stage": list(STAGES.values())}),
    )
    rel = con.sql(
        """
        SELECT b.valid_time AS valid_from,
               b.valid_time + CASE json_extract_string(b.payload, '$.resolutioncode')
                                  WHEN 'PT15M' THEN INTERVAL 15 MINUTE
                                  ELSE INTERVAL 1 MINUTE END AS valid_to,
               TRY_CAST(json_extract_string(b.payload, '$.quarterhour') AS TIMESTAMPTZ)
                   AS quarter_hour,
               m.measure,
               TRY_CAST(json_extract_string(b.payload, '$.' || m.field) AS DOUBLE) AS value,
               m.unit,
               s.stage,
               json_extract_string(b.payload, '$.qualitystatus') AS quality_status,
               b.transaction_time,
               b.dataset_id AS source_dataset,
               b.payload_hash
        FROM bronze b
        JOIN mapping m USING (dataset_id)
        JOIN stages s USING (dataset_id)
        """
    )
    return _to_table(rel).cast(SILVER_ARROW_SCHEMA)


def new_versions(candidates: pa.Table, prior_latest: pa.Table) -> pa.Table:
    """Keep only candidate rows that differ from the version before them.

    `prior_latest` holds the latest Silver row for each (valid_from, measure)
    already stored. Candidates are ordered by transaction_time (then stage) and
    each is compared with its predecessor, so a burst of identical re-fetches
    collapses to the first one.
    """
    con = duckdb.connect()
    con.register("cand", candidates)
    con.register("prior", prior_latest.cast(SILVER_ARROW_SCHEMA))
    con.register(
        "stage_rank",
        pa.table({"stage": list(STAGE_RANK), "rank": list(STAGE_RANK.values())}),
    )
    rel = con.sql(
        """
        WITH unioned AS (
            SELECT *, TRUE AS is_prior FROM prior
            UNION ALL
            SELECT *, FALSE AS is_prior FROM cand
        ),
        ordered AS (
            SELECT u.*,
                   lag(value)          OVER w AS prev_value,
                   lag(quality_status) OVER w AS prev_quality,
                   lag(stage)          OVER w AS prev_stage,
                   row_number()        OVER w AS seq
            FROM unioned u JOIN stage_rank r USING (stage)
            WINDOW w AS (
                PARTITION BY valid_from, measure
                ORDER BY transaction_time, is_prior DESC, r.rank, payload_hash
            )
        )
        SELECT * EXCLUDE (is_prior, prev_value, prev_quality, prev_stage, seq)
        FROM ordered
        WHERE NOT is_prior
          AND (
                seq = 1
             OR value IS DISTINCT FROM prev_value
             OR quality_status IS DISTINCT FROM prev_quality
             OR stage IS DISTINCT FROM prev_stage
          )
        ORDER BY valid_from, measure, transaction_time
        """
    )
    return _to_table(rel).select(SILVER_ARROW_SCHEMA.names).cast(SILVER_ARROW_SCHEMA)


def latest_versions(silver: pa.Table) -> pa.Table:
    """Latest stored row per (valid_from, measure)."""
    con = duckdb.connect()
    con.register("silver", silver)
    return _to_table(
        con.sql(
            """
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY valid_from, measure ORDER BY transaction_time DESC
                ) AS rn
                FROM silver
            ) WHERE rn = 1
            """
        )
    ).cast(SILVER_ARROW_SCHEMA)


# ---------------------------------------------------------------- Iceberg I/O


def read_watermark(silver_table) -> datetime:
    snapshot = silver_table.current_snapshot()
    if snapshot is None:
        return EPOCH
    value = snapshot.summary.additional_properties.get(WATERMARK_KEY)
    return datetime.fromisoformat(value) if value else EPOCH


def run() -> dict:
    """Process Bronze rows received since the last run. Returns run statistics."""
    from gridtime.storage.tables import bronze_table, silver_table

    bronze_t, silver_t = bronze_table(), silver_table()
    watermark = read_watermark(silver_t)

    new_bronze = bronze_t.scan(
        row_filter=GreaterThan("transaction_time", watermark.isoformat())
    ).to_arrow()
    if new_bronze.num_rows == 0:
        log.info("No new Bronze rows since %s", watermark.isoformat())
        return {"bronze_rows": 0, "candidates": 0, "new_versions": 0}

    candidates = explode(new_bronze)
    lo = pa.compute.min(candidates["valid_from"]).as_py()
    hi = pa.compute.max(candidates["valid_from"]).as_py()
    prior = latest_versions(
        silver_t.scan(
            row_filter=And(
                GreaterThanOrEqual("valid_from", lo.isoformat()),
                LessThanOrEqual("valid_from", hi.isoformat()),
            )
        ).to_arrow()
    )
    versions = new_versions(candidates, prior)
    new_watermark = pa.compute.max(new_bronze["transaction_time"]).as_py()

    # Always commit (even zero rows) so the watermark advances with the snapshot.
    silver_t.append(versions, snapshot_properties={WATERMARK_KEY: new_watermark.isoformat()})

    stats = {
        "bronze_rows": new_bronze.num_rows,
        "candidates": candidates.num_rows,
        "new_versions": versions.num_rows,
        "watermark": new_watermark.isoformat(),
    }
    log.info(
        "bronze_rows=%d candidates=%d new_versions=%d unchanged=%d watermark=%s",
        stats["bronze_rows"], stats["candidates"], stats["new_versions"],
        stats["candidates"] - stats["new_versions"], stats["watermark"],
    )
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
