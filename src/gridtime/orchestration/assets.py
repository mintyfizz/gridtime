"""Dagster assets: Bronze polls → Silver → dbt Gold.

Every asset that writes to the lakehouse runs one at a time (the instance allows a
single concurrent run, see dagster.yaml). Silver's watermark assumes Bronze rows
never arrive with a transaction_time older than the last processed one, which
only holds with a single writer (docs/decisions/002 and 003).
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
from gridtime.ingestion.elia_client import EliaClient
from gridtime.ingestion.poller import poll_dataset
from gridtime.storage.tables import bronze_table, silver_table
from gridtime.transform import silver
from gridtime.transform.measures import HISTORICAL, NRT

BRONZE_NRT = AssetKey(["bronze", "elia_nrt"])
BRONZE_HISTORICAL = AssetKey(["bronze", "elia_historical"])
SILVER = AssetKey(["silver", "balancing_minute"])  # same key dbt uses for its source

NRT_MAX_AGE = timedelta(minutes=30)


def _poll(dataset_id: str) -> MaterializeResult:
    settings = get_settings()
    client = EliaClient(settings.elia_base_url, page_size=settings.elia_page_size)
    try:
        stats = poll_dataset(
            client, dataset_id, timedelta(minutes=settings.lookback_minutes(dataset_id))
        )
    finally:
        client.close()
    return MaterializeResult(
        metadata={
            "fetched": stats.fetched,
            "new_rows": stats.new,
            "duplicates": stats.duplicate,
            "rejected": stats.rejected,
            "received_at": MetadataValue.timestamp(stats.received_at),
        }
    )


@asset(
    key=BRONZE_NRT,
    group_name="bronze",
    kinds={"python", "iceberg"},
    description="Poll Elia ods175 (near real-time, current hours) into Bronze.",
)
def elia_nrt() -> MaterializeResult:
    return _poll(NRT)


@asset(
    key=BRONZE_HISTORICAL,
    group_name="bronze",
    kinds={"python", "iceberg"},
    description="Poll Elia ods165 (historical, refreshed daily) into Bronze.",
)
def elia_historical() -> MaterializeResult:
    return _poll(HISTORICAL)


@asset(
    key=SILVER,
    deps=[BRONZE_NRT, BRONZE_HISTORICAL],
    group_name="silver",
    kinds={"python", "iceberg", "parquet"},
    automation_condition=AutomationCondition.eager(),
    description=(
        "Turn new Bronze rows into bitemporal Silver versions, then publish a Parquet "
        "snapshot for dbt."
    ),
)
def balancing_minute(context: AssetExecutionContext) -> MaterializeResult:
    stats = silver.run()
    exported = silver.export_snapshot()
    context.log.info("Silver stats: %s", stats)
    return MaterializeResult(
        metadata={
            "bronze_rows_processed": stats["bronze_rows"],
            "new_versions": stats["new_versions"],
            "unchanged_skipped": stats["candidates"] - stats["new_versions"],
            "watermark": stats.get("watermark", "unchanged"),
            "silver_total_rows": exported,
        }
    )


# ---------------------------------------------------------------- checks


@asset_check(asset=BRONZE_NRT, description="Near real-time data is no older than 30 minutes.")
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
