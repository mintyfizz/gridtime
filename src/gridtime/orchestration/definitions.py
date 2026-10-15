"""Dagster entry point.

    make orchestrate   →   http://localhost:3000

Schedules (Europe/Brussels) publish Elia observations to Redpanda:
  * poll_nrt          every 5 minutes   (ods175 near real-time)
  * poll_historical   hourly at :07     (ods165, refreshed once a day by Elia)
The Bronze sink, Silver and Gold then rebuild automatically (eager automation).
  * retrain_forecast  daily at 06:13    (retrain + backtests; live forecasts follow
                                          every Silver update)
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
# Share host configuration with dbt subprocesses.
os.environ.setdefault("TRINO_HOST", settings.trino_host)
os.environ.setdefault("TRINO_PORT", str(settings.trino_port))
os.environ.setdefault("TRINO_USER", settings.trino_user)

poll_nrt = define_asset_job("poll_nrt", selection=AssetSelection.assets(assets.STREAM_NRT))
poll_historical = define_asset_job(
    "poll_historical", selection=AssetSelection.assets(assets.STREAM_HISTORICAL)
)
retrain_forecast = define_asset_job(
    "retrain_forecast", selection=AssetSelection.assets(assets.FORECAST_MODEL)
)

refresh_lakehouse = define_asset_job(
    "refresh_lakehouse",
    selection=AssetSelection.all() - AssetSelection.assets(assets.FORECAST_MODEL),
)

defs = Definitions(
    assets=[
        assets.elia_ods175,
        assets.elia_ods165,
        assets.elia_raw,
        assets.balancing_minute,
        assets.gold_models,
        assets.forecast_model,
        assets.forecast_live,
    ],
    asset_checks=[
        assets.soda_bronze,
        assets.soda_silver,
        assets.soda_gold,
        assets.stream_fully_consumed,
        assets.nrt_is_fresh,
        assets.silver_has_no_duplicate_versions,
    ],
    jobs=[poll_nrt, poll_historical, retrain_forecast, refresh_lakehouse],
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
        ScheduleDefinition(
            job=retrain_forecast,
            cron_schedule="13 6 * * *",
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
