"""What a valid Elia record looks like, and how it becomes a Bronze row.

Bronze stores the raw payload untouched. Validation here is deliberately minimal:
we only reject records we could never place in time. Field-level typing happens
in Silver (milestone 2), once we know each dataset's columns.
"""

import hashlib
import json
from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator


class EliaRecord(BaseModel):
    """One record from the Elia API. Unknown fields are kept, not dropped."""

    model_config = ConfigDict(extra="allow")

    datetime: datetime

    @field_validator("datetime")
    @classmethod
    def must_be_timezone_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("datetime must include a timezone offset")
        return v


class BronzeRow(BaseModel):
    dataset_id: str
    valid_time: datetime          # what the record describes
    transaction_time: datetime    # when we received it
    payload: str                  # raw JSON, exactly as received
    payload_hash: str             # sha256 of dataset_id + canonical payload
    source_url: str
    ingested_at: datetime | None = None   # when the Bronze sink committed it (milestone 4)
    source_offset: str | None = None      # topic:partition:offset it was consumed from


def canonical_json(record: dict) -> str:
    """Stable serialisation so identical payloads always hash the same."""
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)


def payload_hash(dataset_id: str, record: dict) -> str:
    return hashlib.sha256(f"{dataset_id}|{canonical_json(record)}".encode()).hexdigest()


def to_bronze_row(
    dataset_id: str, raw: dict, received_at: datetime, source_url: str
) -> BronzeRow:
    """Validate a raw record and wrap it as a Bronze row. Raises ValidationError if invalid."""
    record = EliaRecord.model_validate(raw)
    return BronzeRow(
        dataset_id=dataset_id,
        valid_time=record.datetime,
        transaction_time=received_at,
        payload=canonical_json(raw),
        payload_hash=payload_hash(dataset_id, raw),
        source_url=source_url,
    )
