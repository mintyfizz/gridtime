"""Iceberg table definitions and the read/write helpers the poller uses.

Run directly to create the namespace and tables:
    python -m gridtime.storage.tables
"""

from datetime import datetime

import pyarrow as pa
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import NestedField, StringType, TimestamptzType

from gridtime.ingestion.schemas import BronzeRow
from gridtime.storage.catalog import get_catalog

NAMESPACE = "bronze"
BRONZE_TABLE = f"{NAMESPACE}.elia_raw"

BRONZE_SCHEMA = Schema(
    NestedField(1, "dataset_id", StringType(), required=True),
    NestedField(2, "valid_time", TimestamptzType(), required=True),
    NestedField(3, "transaction_time", TimestamptzType(), required=True),
    NestedField(4, "payload", StringType(), required=True),
    NestedField(5, "payload_hash", StringType(), required=True),
    NestedField(6, "source_url", StringType(), required=True),
)

# Partition by dataset and by the day we *received* the data: appends stay cheap
# and "what arrived since X" scans only recent files.
BRONZE_PARTITIONING = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dataset_id"),
    PartitionField(source_id=3, field_id=1001, transform=DayTransform(), name="ingest_day"),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("valid_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("transaction_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
        pa.field("payload_hash", pa.string(), nullable=False),
        pa.field("source_url", pa.string(), nullable=False),
    ]
)


def create_tables() -> None:
    catalog = get_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    catalog.create_table_if_not_exists(
        BRONZE_TABLE, schema=BRONZE_SCHEMA, partition_spec=BRONZE_PARTITIONING
    )


def bronze_table() -> Table:
    return get_catalog().load_table(BRONZE_TABLE)


def append_bronze(rows: list[BronzeRow]) -> None:
    if not rows:
        return
    table = pa.Table.from_pylist([r.model_dump() for r in rows], schema=ARROW_SCHEMA)
    bronze_table().append(table)


def existing_hashes(dataset_id: str, since: datetime) -> set[str]:
    """Hashes already stored for this dataset, received on or after `since`."""
    scan = bronze_table().scan(
        row_filter=And(
            EqualTo("dataset_id", dataset_id),
            GreaterThanOrEqual("transaction_time", since.isoformat()),
        ),
        selected_fields=("payload_hash",),
    )
    return set(scan.to_arrow().column("payload_hash").to_pylist())


if __name__ == "__main__":
    create_tables()
    print(f"Created {BRONZE_TABLE}")
