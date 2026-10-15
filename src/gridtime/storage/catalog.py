"""Iceberg catalogs: Nessie REST in production, frozen SQLite for recovery/tests."""

from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog

from gridtime.config import get_settings


def sql_catalog(uri: str | None = None) -> SqlCatalog:
    s = get_settings()
    uri = uri or s.catalog_uri
    if uri.startswith("sqlite:///"):
        Path(uri.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("gridtime", uri=uri, warehouse=s.warehouse, **_s3_properties())


def _s3_properties() -> dict:
    s = get_settings()
    return {
        "s3.endpoint": s.s3_endpoint,
        "s3.access-key-id": s.minio_root_user,
        "s3.secret-access-key": s.minio_root_password,
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }


def nessie_catalog(ref: str = "main") -> RestCatalog:
    s = get_settings()
    return RestCatalog(
        "gridtime",
        uri=f"{s.nessie_uri.rstrip('/')}/iceberg/{quote(ref, safe='')}",
        warehouse="warehouse",
        **_s3_properties(),
    )


@lru_cache
def get_catalog() -> Catalog:
    s = get_settings()
    if s.catalog_backend == "nessie":
        return nessie_catalog()
    if s.catalog_backend == "sqlite":
        return sql_catalog()
    raise ValueError(f"Unsupported catalog backend: {s.catalog_backend}")
