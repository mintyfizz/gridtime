"""Dagster assets: Elia → Redpanda topics → Bronze sink → Silver → dbt Gold.

Every run is serialised (one concurrent run, see dagster.yaml) because each step
commits to Iceberg through Nessie. Silver reads Bronze by
`ingested_at`, so buffered observations that reach Bronze late are still picked
up (docs/decisions/002–004).
"""

from datetime import UTC, datetime, timedelta

import duckdb
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual

from gridtime.config import REPO_ROOT, get_settings
from gridtime.ingestion import bronze_sink
from gridtime.ingestion.elia_client import EliaClient
from gridtime.ingestion.poller import poll_dataset
from gridtime.ingestion.stream import ensure_topics, make_producer
from gridtime.storage.tables import bronze_table, silver_table
from gridtime.transform import wap
from gridtime.transform.measures import HISTORICAL, NRT

STREAM_NRT = AssetKey(["stream", "elia_ods175"])
STREAM_HISTORICAL = AssetKey(["stream", "elia_ods165"])
BRONZE = AssetKey(["bronze", "elia_raw"])
SILVER = AssetKey(["silver", "balancing_minute"])  # same key dbt uses for its source
FORECAST_MODEL = AssetKey(["forecast", "model"])
FORECAST_LIVE = AssetKey(["forecast", "live"])

NRT_MAX_AGE = timedelta(minutes=30)


def _publish(dataset_id: str) -> MaterializeResult:
    settings = get_settings()
    ensure_topics(settings)
    client = EliaClient(settings.elia_base_url, page_size=settings.elia_page_size)
    producer = make_producer(settings)
    try:
        stats = poll_dataset(
            client, producer, dataset_id,
            timedelta(minutes=settings.lookback_minutes(dataset_id)),
        )
    finally:
        client.close()
    return MaterializeResult(
        metadata={
            "topic": settings.topic_for(dataset_id),
            "fetched": stats.fetched,
            "published": stats.published,
            "rejected_to_dlq": stats.rejected,
            "received_at": MetadataValue.timestamp(stats.received_at),
        }
    )


@asset(
    key=STREAM_NRT,
    group_name="stream",
    kinds={"python", "kafka"},
    description="Poll Elia ods175 (near real-time) and publish each observation to Redpanda.",
)
def elia_ods175() -> MaterializeResult:
    return _publish(NRT)


@asset(
    key=STREAM_HISTORICAL,
    group_name="stream",
    kinds={"python", "kafka"},
    description="Poll Elia ods165 (historical, daily) and publish each observation to Redpanda.",
)
def elia_ods165() -> MaterializeResult:
    return _publish(HISTORICAL)


@asset(
    key=BRONZE,
    deps=[STREAM_NRT, STREAM_HISTORICAL],
    group_name="bronze",
    kinds={"python", "kafka", "iceberg"},
    automation_condition=AutomationCondition.eager(),
    description=(
        "Drain the Elia topics into Bronze: deduplicate by payload hash, append to "
        "Iceberg, then commit consumer offsets."
    ),
)
def elia_raw() -> MaterializeResult:
    stats = bronze_sink.run_once()
    return MaterializeResult(
        metadata={
            "consumed": stats.consumed,
            "new_rows": stats.new_rows,
            "duplicates": stats.duplicates,
            "rejected": stats.rejected,
            "undecodable": stats.undecodable,
            "consumer_lag_after": stats.total_lag,
        }
    )


@asset(
    key=SILVER,
    deps=[BRONZE],
    group_name="silver",
    kinds={"python", "iceberg", "trino"},
    automation_condition=AutomationCondition.eager(),
    description=(
        "Write bitemporal Silver versions on a Nessie branch; Soda audits through Trino "
        "before merging the audited commit into main."
    ),
)
def balancing_minute(context: AssetExecutionContext) -> MaterializeResult:
    stats = wap.run()
    context.log.info("Silver stats: %s", stats)
    return MaterializeResult(
        metadata={
            "bronze_rows_processed": stats["bronze_rows"],
            "new_versions": stats["new_versions"],
            "unchanged_skipped": stats["candidates"] - stats["new_versions"],
            "watermark": stats.get("watermark", "unchanged"),
            "silver_total_rows": stats["silver_total_rows"],
            "audited_hash": stats["audited_hash"],
            "audit_report": stats["audit_report"],
        }
    )


# ---------------------------------------------------------------- forecasting


@asset(
    key=FORECAST_MODEL,
    deps=[SILVER],
    group_name="forecast",
    kinds={"python", "sklearn", "mlflow"},
    description=(
        "Retrain the quarter-hour imbalance forecasters and run both backtests "
        "(revised data, and as known at each decision time). Logged to MLflow."
    ),
)
def forecast_model() -> MaterializeResult:
    from gridtime.forecasting import pipeline

    result = pipeline.run_backtest()
    m = result["metrics"]
    md = {k: v for k, v in result["summary"].items() if isinstance(v, int | float | str)}
    for mode in ("revised", "as_known", "revised_same_period"):
        row = m[(m["mode"] == mode) & (m["model"] == "gradient_boosting") & (m["horizon"] == 1)]
        if len(row):
            md[f"{mode}_h1_mae"] = round(float(row["mae"].iloc[0]), 2)
            md[f"{mode}_h1_skill"] = round(float(row["skill_vs_persistence"].iloc[0]), 3)
    md["metrics"] = MetadataValue.md(
        m[m["horizon"] == 1].round(3).to_markdown(index=False) if len(m) else "no metrics"
    )
    return MaterializeResult(metadata=md)


@asset(
    key=FORECAST_LIVE,
    deps=[SILVER, FORECAST_MODEL],
    group_name="forecast",
    kinds={"python", "parquet"},
    automation_condition=AutomationCondition.eager(),
    description="Forecast the next two quarter-hours from what is known now; append to the log.",
)
def forecast_live() -> MaterializeResult:
    from gridtime.forecasting import pipeline

    new = pipeline.run_live()
    gb = new[(new["model"] == "gradient_boosting") & (new["horizon"] == 1)].iloc[0]
    return MaterializeResult(
        metadata={
            "target_qh": MetadataValue.timestamp(gb["target_qh"].to_pydatetime()),
            "forecast_mw": round(float(gb["q50"]), 1),
            "interval_80": f"[{gb['q10']:.0f}, {gb['q90']:.0f}] MW",
            "lag_1_minutes_known": int(gb["lag_1_minutes_known"]),
        }
    )


# ---------------------------------------------------------------- checks


@asset_check(asset=BRONZE, description="The sink has consumed everything in the Elia topics.")
def stream_fully_consumed() -> AssetCheckResult:
    settings = get_settings()
    topics = [settings.topic_for(d) for d in settings.dataset_ids]
    consumer = bronze_sink.make_consumer(settings)
    try:
        lag = bronze_sink.lag(consumer, topics)
    finally:
        consumer.close()
    return AssetCheckResult(
        passed=sum(lag.values()) == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={"lag_by_partition": MetadataValue.json(lag)},
    )


@asset_check(asset=BRONZE, description="Near real-time data in Bronze is no older than 30 minutes.")
def nrt_is_fresh() -> AssetCheckResult:
    now = datetime.now(UTC)
    recent = bronze_table().scan(
        row_filter=And(
            EqualTo("dataset_id", NRT),
            GreaterThanOrEqual("transaction_time", (now - timedelta(days=1)).isoformat()),
        ),
        selected_fields=("valid_time",),
    ).to_arrow()
    if recent.num_rows == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"reason": "no ods175 rows received in the last 24 h"},
        )
    newest = max(recent.column("valid_time").to_pylist())
    age = now - newest
    return AssetCheckResult(
        passed=age <= NRT_MAX_AGE,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "newest_minute": MetadataValue.timestamp(newest),
            "age_minutes": round(age.total_seconds() / 60, 1),
        },
    )


@asset_check(
    asset=SILVER,
    description="No (minute, measure) has two versions with the same transaction time.",
)
def silver_has_no_duplicate_versions() -> AssetCheckResult:
    con = duckdb.connect()
    con.register(
        "silver",
        silver_table()
        .scan(selected_fields=("valid_from", "measure", "transaction_time"))
        .to_arrow(),
    )
    duplicates = con.sql(
        """
        SELECT count(*) FROM (
            SELECT 1 FROM silver GROUP BY valid_from, measure, transaction_time
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    return AssetCheckResult(passed=duplicates == 0, metadata={"duplicate_groups": duplicates})


# ---------------------------------------------------------------- dbt (Gold)

DBT_DIR = REPO_ROOT / "transforms"
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()
if not dbt_project.manifest_path.exists():  # outside `dagster dev` (tests, CI, CLI)
    dbt_project.preparer.prepare(dbt_project)


class GoldTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        # Models: <schema>/<name> (staging/…, gold/…). Sources keep <source>/<table>,
        # so silver.balancing_minute maps onto the Silver asset above.
        if dbt_resource_props["resource_type"] == "model":
            return AssetKey([dbt_resource_props["schema"], dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)

    def get_automation_condition(self, dbt_resource_props):
        # Rebuild Gold whenever Silver (or an upstream model) has new data.
        return AutomationCondition.eager()

    def get_group_name(self, dbt_resource_props):
        return "gold"


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=GoldTranslator())
def gold_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# Soda checks are visible in Dagster in addition to the pre-publication Silver gate.
def _soda_result(layer: str) -> AssetCheckResult:
    from gridtime.quality.scan import scan_layer

    report = scan_layer(layer, require_pass=False)
    return AssetCheckResult(
        passed=report["passed"],
        metadata={"checks": len(report["checks"]), "report": report["report_path"]},
    )


@asset_check(asset=BRONZE, blocking=True)
def soda_bronze() -> AssetCheckResult:
    return _soda_result("bronze")


@asset_check(asset=SILVER)
def soda_silver() -> AssetCheckResult:
    return _soda_result("silver")


@asset_check(
    asset=AssetKey(["gold", "fct_quarter_hour"]),
    additional_deps=[AssetKey(["gold", "fct_revisions"]), AssetKey(["gold", "agg_revision_stats"])],
)
def soda_gold() -> AssetCheckResult:
    return _soda_result("gold")
