"""Forecasters for the next quarter-hours' mean system imbalance.

All models share one interface: fit(X, y) on a feature frame, predict(X) returning
a frame with columns q10, q50, q90 (MW). Baselines get their intervals from
empirical training residuals, so every model is scored the same way.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

from gridtime.forecasting.features import FEATURES

QUANTILES = (0.1, 0.5, 0.9)


class Forecaster:
    name = "base"

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "Forecaster":
        raise NotImplementedError

    def predict(self, X: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


@dataclass
class _ResidualBands:
    """q10/q90 from training residuals around a point forecast."""

    lo: float = 0.0
    hi: float = 0.0

    def fit(self, y: pd.Series, point: pd.Series) -> None:
        resid = (y - point).dropna()
        self.lo, self.hi = (np.quantile(resid, [0.1, 0.9]) if len(resid) else (0.0, 0.0))

    def wrap(self, point: pd.Series) -> pd.DataFrame:
        return pd.DataFrame({"q10": point + self.lo, "q50": point, "q90": point + self.hi})


@dataclass
class Persistence(Forecaster):
    """The last quarter-hour's mean (falls back to the one before if it is missing)."""

    name: str = "persistence"
    bands: _ResidualBands = field(default_factory=_ResidualBands)

    def _point(self, X):
        return X["lag_1"].fillna(X["lag_2"]).fillna(X["last_minute"])

    def fit(self, X, y):
        self.bands.fit(y, self._point(X))
        return self

    def predict(self, X):
        return self.bands.wrap(self._point(X))


@dataclass
class SeasonalNaive(Forecaster):
    """Same quarter-hour yesterday."""

    name: str = "seasonal_naive"
    bands: _ResidualBands = field(default_factory=_ResidualBands)

    def _point(self, X):
        return X["lag_96"].fillna(X["lag_672"])

    def fit(self, X, y):
        self.bands.fit(y, self._point(X))
        return self

    def predict(self, X):
        return self.bands.wrap(self._point(X))


@dataclass
class Climatology(Forecaster):
    """Training mean for that quarter-hour of the day."""

    name: str = "climatology"
    by_qh: dict = field(default_factory=dict)
    overall: float = 0.0
    bands: _ResidualBands = field(default_factory=_ResidualBands)

    def _point(self, X):
        return X["qh_of_day"].map(self.by_qh).fillna(self.overall)

    def fit(self, X, y):
        df = pd.DataFrame({"qh": X["qh_of_day"], "y": y}).dropna()
        self.by_qh = df.groupby("qh")["y"].mean().to_dict()
        self.overall = float(df["y"].mean()) if len(df) else 0.0
        self.bands.fit(y, self._point(X))
        return self

    def predict(self, X):
        return self.bands.wrap(self._point(X))


@dataclass
class GradientBoosting(Forecaster):
    """Histogram gradient boosting, one model per quantile, with conformal calibration.

    Quantile models are fitted on the first (1 - calibration) share of the training
    period. The most recent share calibrates the interval (conformalized quantile
    regression): q10/q90 are widened by the amount that makes 80% of calibration
    targets fall inside. Without this the raw intervals are overconfident.
    """

    name: str = "gradient_boosting"
    max_iter: int = 300
    learning_rate: float = 0.05
    calibration: float = 0.15
    models: dict = field(default_factory=dict)
    widen: float = 0.0

    def _fit_quantiles(self, X, y):
        for q in QUANTILES:
            m = HistGradientBoostingRegressor(
                loss="quantile", quantile=q, max_iter=self.max_iter,
                learning_rate=self.learning_rate, max_leaf_nodes=31,
                min_samples_leaf=40, l2_regularization=1.0, random_state=42,
            )
            m.fit(X[FEATURES], y)
            self.models[q] = m

    def _raw(self, X):
        preds = {f"q{int(q * 100)}": self.models[q].predict(X[FEATURES]) for q in QUANTILES}
        # Quantile models are fitted independently; keep them ordered.
        ordered = np.sort(pd.DataFrame(preds)[["q10", "q50", "q90"]].to_numpy(), axis=1)
        return pd.DataFrame(ordered, index=X.index, columns=["q10", "q50", "q90"])

    def fit(self, X, y):
        mask = y.notna().to_numpy()
        X, y = X[mask], y[mask]
        n_cal = int(len(y) * self.calibration)
        if n_cal < 50:  # too little data to calibrate: fit on everything, no widening
            self._fit_quantiles(X, y)
            self.widen = 0.0
            return self
        self._fit_quantiles(X.iloc[:-n_cal], y.iloc[:-n_cal])
        raw = self._raw(X.iloc[-n_cal:])
        y_cal = y.iloc[-n_cal:].to_numpy()
        scores = np.maximum(raw["q10"].to_numpy() - y_cal, y_cal - raw["q90"].to_numpy())
        level = min(1.0, np.ceil((n_cal + 1) * 0.8) / n_cal)
        self.widen = float(np.quantile(scores, level))
        return self

    def predict(self, X):
        out = self._raw(X)
        out["q10"] -= self.widen
        out["q90"] += self.widen
        return out


def all_models() -> list[Forecaster]:
    return [Persistence(), SeasonalNaive(), Climatology(), GradientBoosting()]
