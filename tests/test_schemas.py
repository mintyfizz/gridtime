from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from gridtime.ingestion.schemas import payload_hash, to_bronze_row

RECEIVED = datetime(2026, 10, 15, 12, 7, tzinfo=UTC)
URL = "https://example/records"


def test_valid_record_becomes_bronze_row():
    raw = {"datetime": "2026-10-15T12:00:00+00:00", "imbalanceprice": 84.2}
    row = to_bronze_row("ods175", raw, RECEIVED, URL)
    assert row.valid_time == datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
    assert row.transaction_time == RECEIVED
    assert '"imbalanceprice":84.2' in row.payload


def test_missing_datetime_is_rejected():
    with pytest.raises(ValidationError):
        to_bronze_row("ods175", {"imbalanceprice": 84.2}, RECEIVED, URL)


def test_naive_datetime_is_rejected():
    with pytest.raises(ValidationError):
        to_bronze_row("ods175", {"datetime": "2026-10-15T12:00:00"}, RECEIVED, URL)


def test_hash_ignores_key_order():
    a = {"datetime": "2026-10-15T12:00:00+00:00", "x": 1, "y": 2}
    b = {"y": 2, "x": 1, "datetime": "2026-10-15T12:00:00+00:00"}
    assert payload_hash("ods175", a) == payload_hash("ods175", b)


def test_revised_value_gets_new_hash():
    """A revision of the same quarter-hour must be stored as a new version."""
    v1 = {"datetime": "2026-10-15T12:00:00+00:00", "imbalanceprice": 80.0}
    v2 = {"datetime": "2026-10-15T12:00:00+00:00", "imbalanceprice": 84.0}
    assert payload_hash("ods175", v1) != payload_hash("ods175", v2)


def test_same_payload_in_different_datasets_differs():
    raw = {"datetime": "2026-10-15T12:00:00+00:00", "imbalanceprice": 84.0}
    assert payload_hash("ods175", raw) != payload_hash("ods165", raw)
