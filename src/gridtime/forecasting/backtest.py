"""Two backtests over the same models and features.

revised  — walk-forward over the full history, features built from the *latest*
           values. Assumes every past minute was known, final, at decision time.
           This is how forecasting projects are usually evaluated.
as_known — decision times inside the period GridTime has actually been running.
           Features come from Silver *as of the decision time*: late polls,
           partial quarter-hours and not-yet-revised values included. The same
           decision times are also scored with revised features, so the gap
           between the two isolates the effect of data availability.
"""

from dataclasses import dataclass

import pandas as pd

from gridtime.forecasting.features import FeatureSource, frame
from gridtime.forecasting.models import Forecaster, all_models
from gridtime.forecasting.series import (
    QH,
    decision_times,
    known_minutes,
    qh_ceil,
    quarter_hours,
    targets,
)

HORIZONS = (1, 2)
HISTORY = pd.Timedelta(days=8)  # lag_672 (one week) plus a day of rolling windows
FOLD = pd.Timedelta(days=7)


@dataclass
class Prepared:
    minutes: pd.Series     # latest known minute values
    truth: pd.Series       # complete quarter-hour means to score against
    source: FeatureSource  # revised (latest) feature source


def prepare(versions: pd.DataFrame) -> Prepared:
    minutes = known_minutes(versions)
    return Prepared(minutes, targets(quarter_hours(minutes)), FeatureSource(minutes))


def _target(truth: pd.Series, times: pd.DatetimeIndex, horizon: int) -> pd.Series:
    return pd.Series(truth.reindex(times + (horizon - 1) * QH).to_numpy(), index=times)


def _rows(mode, model, horizon, times, pred, y) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "mode": mode,
            "model": model.name,
            "horizon": horizon,
            "decision_time": times,
            "target_qh": times + (horizon - 1) * QH,
            "y_true": y.to_numpy(),
            "q10": pred["q10"].to_numpy(),
            "q50": pred["q50"].to_numpy(),
            "q90": pred["q90"].to_numpy(),
        }
    )


def fit_models(p: Prepared, cutoff: pd.Timestamp) -> dict[int, list[Forecaster]]:
    """Fit every model per horizon on decisions whose target ended before `cutoff`."""
    start = p.minutes.index.min() + HISTORY
    times = decision_times(start, cutoff)
    X = frame(p.source, times)
    fitted = {}
    for h in HORIZONS:
        y = _target(p.truth, times, h)
        keep = (times + h * QH <= cutoff) & y.notna().to_numpy()
        fitted[h] = [m.fit(X[keep], y[keep]) for m in all_models()]
    return fitted


def revised_backtest(p: Prepared, n_folds: int = 4, fold: pd.Timedelta = FOLD,
                     end: pd.Timestamp | None = None) -> pd.DataFrame:
    end = end or p.truth.index.max()
    out = []
    for k in range(n_folds, 0, -1):
        f_start, f_end = end - k * fold, end - (k - 1) * fold
        if f_start - HISTORY < p.minutes.index.min():
            continue  # not enough history before this fold
        fitted = fit_models(p, f_start)
        times = decision_times(f_start, f_end - QH)
        X = frame(p.source, times)
        for h, models in fitted.items():
            y = _target(p.truth, times, h)
            for m in models:
                out.append(_rows("revised", m, h, times, m.predict(X), y))
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def knowledge_start(versions: pd.DataFrame) -> pd.Timestamp | None:
    """First moment at which GridTime knew at least HISTORY of past data."""
    v = versions.sort_values("transaction_time")
    depth = v["transaction_time"] - v["valid_from"].cummin()
    ok = v.loc[depth >= HISTORY, "transaction_time"]
    return qh_ceil(ok.iloc[0]) if len(ok) else None


def as_known_features(versions: pd.DataFrame, times: pd.DatetimeIndex) -> pd.DataFrame:
    rows = []
    for t in times:
        window = versions[versions["valid_from"] >= t - HISTORY - QH]
        rows.append(FeatureSource(known_minutes(window, known_at=t)).row(t))
    return pd.DataFrame(rows, index=times)


def as_known_backtest(versions: pd.DataFrame, p: Prepared) -> tuple[pd.DataFrame, dict]:
    """Score decisions made while GridTime was live, with as-known and revised features."""
    start = knowledge_start(versions)
    last_target = p.truth.index.max()
    if start is None or start > last_target:
        return pd.DataFrame(), {"as_known_start": start, "decisions": 0}
    times = decision_times(start, last_target)
    fitted = fit_models(p, start)  # trained only on what could be known at the start
    X_known = as_known_features(versions, times)
    X_revised = frame(p.source, times)
    out = []
    for h, models in fitted.items():
        y = _target(p.truth, times, h)
        for m in models:
            out.append(_rows("as_known", m, h, times, m.predict(X_known), y))
            out.append(_rows("revised_same_period", m, h, times, m.predict(X_revised), y))
    preds = pd.concat(out, ignore_index=True)
    info = {
        "as_known_start": start,
        "decisions": int(preds.dropna(subset=["y_true"])["decision_time"].nunique()),
        "mean_lag1_minutes_known": float(X_known["lag_1_minutes"].mean()),
        "mean_last_minute_age": float(X_known["last_minute_age"].mean()),
    }
    return preds, info
