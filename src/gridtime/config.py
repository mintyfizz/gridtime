"""All settings in one place, loaded from environment variables / .env."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    poll_interval_seconds: int = 60

    @property
    def dataset_ids(self) -> list[str]:
        return [d.strip() for d in self.elia_datasets.split(",") if d.strip()]

    @property
    def warehouse(self) -> str:
        return f"s3://{self.s3_bucket}/warehouse"


@lru_cache
def get_settings() -> Settings:
    return Settings()
