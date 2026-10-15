from gridtime.config import Settings


def test_lookback_override_applies_to_named_dataset():
    s = Settings(elia_lookback_minutes=120, elia_lookback_overrides="ods165=2880")
    assert s.lookback_minutes("ods165") == 2880


def test_lookback_falls_back_to_default():
    s = Settings(elia_lookback_minutes=120, elia_lookback_overrides="ods165=2880")
    assert s.lookback_minutes("ods175") == 120


def test_empty_overrides_use_default():
    s = Settings(elia_lookback_minutes=90, elia_lookback_overrides="")
    assert s.lookback_minutes("ods165") == 90
