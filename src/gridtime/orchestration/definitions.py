"""Dagster entry point.

    make orchestrate   →   http://localhost:3000

Schedules (Europe/Brussels):
  * poll_nrt          every 5 minutes   (ods175 near real-time)
  * poll_historical   hourly at :07     (ods165, refreshed once a day by Elia)
Silver and Gold rebuild automatically after each poll that wrote something upstream.
"""

import os

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from gridtime.config import get_settings
from gridtime.orchestration import assets

settings = get_settings()
# dbt runs as a subprocess and reads its paths from the environment.
os.environ.setdefault("GRIDTIME_SILVER_PARQUET", str(settings.silver_export_path))
os.environ.setdefault("GRIDTIME_GOLD_DB", str(settings.gold_db_path))
settings.gold_db_path.parent.mkdir(parents=True, exist_ok=True)

poll_nrt = define_asset_job("poll_nrt", selection=AssetSelection.assets(assets.BRONZE_NRT))
poll_historical = define_asset_job(
    "poll_historical", selection=AssetSelection.assets(assets.BRONZE_HISTORICAL)
)

defs = Definitions(
    assets=[assets.elia_nrt, assets.elia_historical, assets.balancing_minute, assets.gold_models],
    asset_checks=[assets.nrt_is_fresh, assets.silver_has_no_duplicate_versions],
    jobs=[poll_nrt, poll_historical],
    schedules=[
        ScheduleDefinition(
            job=poll_nrt,
            cron_schedule="*/5 * * * *",
            execution_timezone="Europe/Brussels",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=poll_historical,
            cron_schedule="7 * * * *",
            execution_timezone="Europe/Brussels",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
    sensors=[
        AutomationConditionSensorDefinition(
            "rebuild_downstream",
            target=AssetSelection.all(),
            default_status=DefaultSensorStatus.RUNNING,
            minimum_interval_seconds=30,
        )
    ],
    resources={"dbt": DbtCliResource(project_dir=assets.dbt_project)},
)
