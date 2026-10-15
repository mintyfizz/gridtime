"""From Silver versions to the series a forecaster can see at a given moment."""

from datetime import datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

QH = pd.Timedelta(minutes=15)
TARGET_MEASURE = "system_imbalance"


def load_versions(silver: pa.Table, measure: str = TARGET_MEASURE) -> pd.DataFrame:
    """All stored versions of one measure: valid_from, transaction_time, value, stage."""
    t = silver.filter(pc.equal(silver["measure"], measure)).select(
        ["valid_from", "transaction_time", "value", "stage"]
    )
    df = t.to_pandas()
    for col in ("valid_from", "transaction_time"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df.sort_values(["transaction_time", "valid_from"], kind="stable").reset_index(drop=True)


def known_minutes(versions: pd.DataFrame, known_at: datetime | None = None) -> pd.Series:
    """Minute values as known at `known_at` (latest version per minute); None = now.

    Only minutes strictly before `known_at` are returned: a forecast made at t
    cannot use the minute starting at t.
    """
    v = versions
    if known_at is not None:
        ts = pd.Timestamp(known_at)
        v = v[(v["transaction_time"] <= ts) & (v["valid_from"] < ts)]
    latest = v.drop_duplicates("valid_from", keep="last")  # sorted by transaction_time
    return latest.set_index("valid_from")["value"].dropna().sort_index()


def quarter_hours(minutes: pd.Series) -> pd.DataFrame:
    """Quarter-hour mean and number of minutes observed, indexed by quarter-hour start."""
    if minutes.empty:
        return pd.DataFrame(columns=["mean", "n"], index=pd.DatetimeIndex([], tz="UTC"))
    grouped = minutes.groupby(minutes.index.floor("15min"))
    return pd.DataFrame({"mean": grouped.mean(), "n": grouped.size()})


def targets(qh: pd.DataFrame, min_minutes: int = 13) -> pd.Series:
    """Quarter-hour means complete enough to score against."""
    return qh.loc[qh["n"] >= min_minutes, "mean"]


def qh_floor(ts: datetime) -> pd.Timestamp:
    return pd.Timestamp(ts).floor("15min")


def qh_ceil(ts: datetime) -> pd.Timestamp:
    return pd.Timestamp(ts).ceil("15min")


def decision_times(start, end) -> pd.DatetimeIndex:
    """Quarter-hour boundaries in [start, end]."""
    lo, hi = qh_ceil(start), qh_floor(end)
    if lo > hi:
        return pd.DatetimeIndex([], tz="UTC")
    return pd.date_range(lo, hi, freq="15min")


def nan_if_missing(mapping: dict, key) -> float:
    value = mapping.get(key)
    return np.nan if value is None else float(value)
