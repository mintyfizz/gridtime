"""Connect PyIceberg to MinIO.

The catalog (table metadata pointers) lives in a local SQLite file for now;
the data and metadata files live in MinIO. Milestone 6 swaps SQLite for Nessie.
"""

from functools import lru_cache
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog

from gridtime.config import get_settings


@lru_cache
def get_catalog() -> SqlCatalog:
    s = get_settings()
    if s.catalog_uri.startswith("sqlite:///"):
        Path(s.catalog_uri.removeprefix("sqlite:///")).parent.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "gridtime",
        uri=s.catalog_uri,
        warehouse=s.warehouse,
        **{
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.minio_root_user,
            "s3.secret-access-key": s.minio_root_password,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
