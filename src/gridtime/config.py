"""All settings in one place, loaded from environment variables / .env."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Storage
    minio_root_user: str = "gridtime"
    minio_root_password: str = "change-me-please"
    s3_endpoint: str = "http://localhost:9000"
    s3_bucket: str = "lakehouse"

    # Catalog
    catalog_uri: str = "sqlite:///catalog/catalog.db"

    # Elia
    elia_base_url: str = "https://opendata.elia.be/api/explore/v2.1"
    elia_datasets: str = "ods175,ods165"
    elia_page_size: int = Field(default=100, le=100)  # API maximum per page
    elia_lookback_minutes: int = 120
    # Per-dataset overrides, e.g. "ods165=2880". Historical datasets are refreshed
    # daily and end at yesterday, so they need a longer window than the default.
    elia_lookback_overrides: str = "ods165=2880"
    poll_interval_seconds: int = 60

    @property
    def dataset_ids(self) -> list[str]:
        return [d.strip() for d in self.elia_datasets.split(",") if d.strip()]

    def lookback_minutes(self, dataset_id: str) -> int:
        overrides = {}
        for pair in self.elia_lookback_overrides.split(","):
            if "=" in pair:
                key, value = pair.split("=", 1)
                overrides[key.strip()] = int(value)
        return overrides.get(dataset_id, self.elia_lookback_minutes)

    # Streaming (Redpanda, Kafka API)
    kafka_bootstrap: str = "localhost:19092"
    kafka_topic_prefix: str = "elia"
    kafka_consumer_group: str = "bronze-sink"
    kafka_retention_days: int = 7

    def topic_for(self, dataset_id: str) -> str:
        return f"{self.kafka_topic_prefix}.{dataset_id}"

    @property
    def dlq_topic(self) -> str:
        return f"{self.kafka_topic_prefix}.dlq"

    # Local analytics files (Gold layer). Relative paths resolve against the repo root.
    data_dir: str = "data"

    @property
    def warehouse(self) -> str:
        return f"s3://{self.s3_bucket}/warehouse"

    @property
    def data_path(self) -> Path:
        path = Path(self.data_dir)
        return path if path.is_absolute() else (REPO_ROOT / path)

    @property
    def silver_export_path(self) -> Path:
        """Parquet snapshot of Silver that dbt reads to build Gold."""
        return self.data_path / "silver" / "balancing_minute.parquet"

    @property
    def gold_db_path(self) -> Path:
        return self.data_path / "warehouse.duckdb"


@lru_cache
def get_settings() -> Settings:
    return Settings()
