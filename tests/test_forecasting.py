"""Forecasting: leakage guarantees, feature semantics, models and the full pipeline."""

import numpy as np
import pandas as pd
import pytest

from gridtime.forecasting import backtest, pipeline
from gridtime.forecasting.features import FEATURES, FeatureSource
from gridtime.forecasting.metrics import score
from gridtime.forecasting.series import QH, known_minutes, quarter_hours

START = pd.Timestamp("2026-09-01T00:00", tz="UTC")
BACKFILL_AT = START + pd.Timedelta(days=19)  # history becomes known all at once here
DAYS = 21


def synthetic_versions(seed: int = 0) -> pd.DataFrame:
    """Daily cycle + AR(1) noise. Days before BACKFILL_AT arrive as one backfill.
    Later minutes arrive live: Elia publishes ~2 minutes after the minute starts,
    and the poller picks it up at its next 5-minute run."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range(START, periods=DAYS * 1440, freq="1min")
    noise = np.zeros(len(idx))
    for i in range(1, len(idx)):
        noise[i] = 0.97 * noise[i - 1] + rng.normal(0, 20)
    qh_of_day = (idx.hour * 4 + idx.minute // 15).to_numpy()
    values = 150 * np.sin(2 * np.pi * qh_of_day / 96) + noise
    live = idx >= BACKFILL_AT
    polled = (idx + pd.Timedelta(minutes=2)).floor("5min") + pd.Timedelta(minutes=5)
    tx = np.where(live, polled, BACKFILL_AT)
    return pd.DataFrame({
        "valid_from": idx, "transaction_time": pd.to_datetime(tx, utc=True), "value": values,
        "stage": np.where(live, "near_real_time", "historical"),
    }).sort_values(["transaction_time", "valid_from"]).reset_index(drop=True)


@pytest.fixture(scope="module")
def versions():
    return synthetic_versions()


def features_at(versions, t):
    return FeatureSource(known_minutes(versions, known_at=t)).row(t)


# ---------------------------------------------------------------- leakage


def test_future_and_late_data_never_change_features(versions):
    t = BACKFILL_AT + pd.Timedelta(hours=6)
    before = features_at(versions, t)
    extra = pd.DataFrame({
        # a minute from the future, and a revision of the past received after t
        "valid_from": [t + pd.Timedelta(minutes=3), t - pd.Timedelta(minutes=20)],
        "transaction_time": [t - pd.Timedelta(minutes=1), t + pd.Timedelta(minutes=1)],
        "value": [9999.0, -9999.0],
        "stage": ["near_real_time", "historical"],
    })
    polluted = pd.concat([versions, extra]).sort_values(["transaction_time", "valid_from"])
    assert features_at(polluted, t) == before


def test_known_minutes_excludes_minute_at_decision_time(versions):
    t = BACKFILL_AT + pd.Timedelta(hours=1)
    assert known_minutes(versions, known_at=t).index.max() < t


def test_as_known_sees_partial_last_quarter_hour(versions):
    """At a QH boundary the last minutes have not been polled yet."""
    t = BACKFILL_AT + pd.Timedelta(hours=3)
    known = features_at(versions, t)
    revised = FeatureSource(known_minutes(versions)).row(t)
    assert revised["lag_1_minutes"] == 15
    assert known["lag_1_minutes"] < 15
    assert known["last_minute_age"] > revised["last_minute_age"] == 1


def test_identical_features_when_everything_was_known_long_ago(versions):
    t = BACKFILL_AT - pd.Timedelta(days=2)  # known since BACKFILL_AT
    long_after = BACKFILL_AT + pd.Timedelta(days=1)
    as_known_later = FeatureSource(known_minutes(versions, known_at=long_after)).row(t)
    revised = FeatureSource(known_minutes(versions)).row(t)
    assert as_known_later == revised


def test_knowledge_start_is_when_history_arrived(versions):
    assert backtest.knowledge_start(versions) == BACKFILL_AT


# ---------------------------------------------------------------- models and metrics


def test_feature_row_has_all_features(versions):
    assert set(features_at(versions, BACKFILL_AT + QH)) == set(FEATURES)


def test_metrics_skill_is_relative_to_persistence():
    preds = pd.DataFrame({
        "mode": "revised", "horizon": 1,
        "model": ["persistence"] * 2 + ["better"] * 2,
        "y_true": [10.0, -10.0, 10.0, -10.0],
        "q50": [0.0, 0.0, 5.0, -5.0],
        "q10": [-20.0, -20.0, 0.0, -15.0], "q90": [20.0, 20.0, 15.0, 0.0],
    })
    m = score(preds).set_index("model")
    assert m.loc["persistence", "skill_vs_persistence"] == 0
    assert m.loc["better", "skill_vs_persistence"] == pytest.approx(0.5)
    assert m.loc["better", "sign_accuracy"] == 1.0


# ---------------------------------------------------------------- pipeline


def test_pipeline_end_to_end(versions, tmp_path, monkeypatch):
    from gridtime.config import get_settings

    monkeypatch.setattr(get_settings(), "data_dir", str(tmp_path))
    result = pipeline.run_backtest(versions, log_mlflow=False)
    metrics = result["metrics"].set_index(["mode", "model", "horizon"])

    assert {"revised", "as_known", "revised_same_period"} <= set(
        metrics.index.get_level_values(0)
    )
    gb = metrics.loc[("revised", "gradient_boosting", 1)]
    clim = metrics.loc[("revised", "climatology", 1)]
    assert gb["mae"] < clim["mae"]  # learns the AR structure, not just the daily cycle
    assert 0.6 <= gb["coverage_80"] <= 0.95
    assert result["summary"]["as_known_start"] == str(BACKFILL_AT)

    live = pipeline.run_live(versions, now=BACKFILL_AT + pd.Timedelta(days=1, minutes=7))
    assert len(live) == 4  # 2 models × 2 horizons
    assert set(live["target_qh"]) == {
        BACKFILL_AT + pd.Timedelta(days=1, minutes=15),
        BACKFILL_AT + pd.Timedelta(days=1, minutes=30),
    }
    assert (tmp_path / "forecast" / "live.parquet").exists()


def test_quarter_hours_count_minutes():
    idx = pd.date_range(START, periods=20, freq="1min")
    qh = quarter_hours(pd.Series(range(20), index=idx, dtype=float))
    assert qh["n"].tolist() == [15, 5]
