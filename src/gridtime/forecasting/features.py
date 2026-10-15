"""Features for a forecast made at decision time t (a quarter-hour boundary).

A feature row is built from two inputs only: the minute series and its
quarter-hour aggregate *as known at t*. Revised-data and as-known backtests call
the same function; they differ only in which minutes they pass in.
"""

import numpy as np
import pandas as pd

from gridtime.forecasting.series import QH, nan_if_missing

LAGS = (1, 2, 3, 4, 96, 672)  # quarter-hours back: last hour, same QH yesterday, last week
FEATURES = [
    *(f"lag_{k}" for k in LAGS),
    "lag_1_minutes",
    "roll_4",
    "roll_16",
    "roll_96",
    "last_minute",
    "last_minute_age",
    "qh_of_day",
    "day_of_week",
    "is_weekend",
]
TZ = "Europe/Brussels"


def feature_row(qh_mean: dict, qh_n: dict, minute_index: np.ndarray, minute_values: np.ndarray,
                t: pd.Timestamp) -> dict:
    """Features at decision time t. Inputs must already exclude anything unknown at t."""
    row: dict = {}
    for k in LAGS:
        row[f"lag_{k}"] = nan_if_missing(qh_mean, t - k * QH)
    row["lag_1_minutes"] = float(qh_n.get(t - QH, 0))
    for window in (4, 16, 96):
        vals = [qh_mean.get(t - k * QH) for k in range(1, window + 1)]
        vals = [v for v in vals if v is not None]
        row[f"roll_{window}"] = float(np.mean(vals)) if vals else np.nan

    t64 = t.tz_convert("UTC").tz_localize(None).to_datetime64()  # minute_index is naive UTC
    pos = np.searchsorted(minute_index, t64, side="left") - 1
    if pos >= 0:
        row["last_minute"] = float(minute_values[pos])
        row["last_minute_age"] = (t64 - minute_index[pos]) / np.timedelta64(1, "m")
    else:
        row["last_minute"] = np.nan
        row["last_minute_age"] = np.nan

    local = t.tz_convert(TZ)
    row["qh_of_day"] = local.hour * 4 + local.minute // 15
    row["day_of_week"] = local.dayofweek
    row["is_weekend"] = int(local.dayofweek >= 5)
    return row


class FeatureSource:
    """Precomputed lookups over one minute series, for fast repeated feature rows."""

    def __init__(self, minutes: pd.Series):
        from gridtime.forecasting.series import quarter_hours

        qh = quarter_hours(minutes)
        self.qh_mean = qh["mean"].to_dict()
        self.qh_n = qh["n"].to_dict()
        self.minute_index = minutes.index.tz_convert("UTC").tz_localize(None).to_numpy()
        self.minute_values = minutes.to_numpy(dtype=float)

    def row(self, t: pd.Timestamp) -> dict:
        return feature_row(
            self.qh_mean, self.qh_n, self.minute_index, self.minute_values,
            pd.Timestamp(t).tz_convert("UTC"),
        )


def frame(source: FeatureSource, times) -> pd.DataFrame:
    return pd.DataFrame([source.row(t) for t in times], index=pd.DatetimeIndex(times),
                        columns=FEATURES)
