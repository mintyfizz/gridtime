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
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

from gridtime.ingestion.schemas import BronzeRow
from gridtime.storage.catalog import get_catalog

NAMESPACE = "bronze"
BRONZE_TABLE = f"{NAMESPACE}.elia_raw"
SILVER_NAMESPACE = "silver"
SILVER_TABLE = f"{SILVER_NAMESPACE}.balancing_minute"

BRONZE_SCHEMA = Schema(
    NestedField(1, "dataset_id", StringType(), required=True),
    NestedField(2, "valid_time", TimestamptzType(), required=True),
    NestedField(3, "transaction_time", TimestamptzType(), required=True),
    NestedField(4, "payload", StringType(), required=True),
    NestedField(5, "payload_hash", StringType(), required=True),
    NestedField(6, "source_url", StringType(), required=True),
    NestedField(7, "ingested_at", TimestamptzType(), required=False),
    NestedField(8, "source_offset", StringType(), required=False),
)

# Columns added after the table first went live. Existing tables get them through
# Iceberg schema evolution (metadata only, no data rewrite); old rows read as null.
BRONZE_EVOLVED_COLUMNS = [
    ("ingested_at", TimestamptzType(), "When the Bronze sink committed the row"),
    ("source_offset", StringType(), "topic:partition:offset the row was consumed from"),
]

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
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("source_offset", pa.string(), nullable=True),
    ]
)


SILVER_SCHEMA = Schema(
    NestedField(1, "valid_from", TimestamptzType(), required=True),
    NestedField(2, "valid_to", TimestamptzType(), required=True),
    NestedField(3, "quarter_hour", TimestamptzType(), required=False),
    NestedField(4, "measure", StringType(), required=True),
    NestedField(5, "value", DoubleType(), required=False),
    NestedField(6, "unit", StringType(), required=True),
    NestedField(7, "stage", StringType(), required=True),
    NestedField(8, "quality_status", StringType(), required=False),
    NestedField(9, "transaction_time", TimestamptzType(), required=True),
    NestedField(10, "source_dataset", StringType(), required=True),
    NestedField(11, "payload_hash", StringType(), required=True),
)

# Partition by the day the value describes: as-of queries ask about valid-time
# ranges, and revisions of a minute land in the same partition as the original.
SILVER_PARTITIONING = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="valid_day"),
)


def create_tables() -> None:
    catalog = get_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    catalog.create_table_if_not_exists(
        BRONZE_TABLE, schema=BRONZE_SCHEMA, partition_spec=BRONZE_PARTITIONING
    )
    evolve_bronze_schema()
    catalog.create_namespace_if_not_exists(SILVER_NAMESPACE)
    catalog.create_table_if_not_exists(
        SILVER_TABLE, schema=SILVER_SCHEMA, partition_spec=SILVER_PARTITIONING
    )


def evolve_bronze_schema() -> list[str]:
    """Add any missing evolved columns to an existing Bronze table. Returns names added."""
    table = bronze_table()
    existing = {f.name for f in table.schema().fields}
    missing = [c for c in BRONZE_EVOLVED_COLUMNS if c[0] not in existing]
    if missing:
        with table.update_schema() as update:
            for name, field_type, doc in missing:
                update.add_column(name, field_type, doc=doc)
    return [c[0] for c in missing]


def bronze_table() -> Table:
    return get_catalog().load_table(BRONZE_TABLE)


def silver_table() -> Table:
    return get_catalog().load_table(SILVER_TABLE)


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
    print(f"Created {BRONZE_TABLE} and {SILVER_TABLE}")
