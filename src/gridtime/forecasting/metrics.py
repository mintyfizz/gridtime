"""Scoring forecasts: point accuracy, skill, direction, and interval quality."""

import numpy as np
import pandas as pd


def pinball(y: np.ndarray, q: np.ndarray, tau: float) -> float:
    diff = y - q
    return float(np.mean(np.maximum(tau * diff, (tau - 1) * diff)))


def score(predictions: pd.DataFrame) -> pd.DataFrame:
    """One row per (mode, model, horizon).

    `predictions` needs columns mode, model, horizon, y_true, q10, q50, q90.
    Skill is relative to persistence in the same mode and horizon:
    1 - MAE(model) / MAE(persistence); positive means better than persistence.
    """
    p = predictions.dropna(subset=["y_true", "q50"])
    rows = []
    for (mode, model, horizon), g in p.groupby(["mode", "model", "horizon"], sort=True):
        y, q50 = g["y_true"].to_numpy(), g["q50"].to_numpy()
        err = q50 - y
        rows.append(
            {
                "mode": mode,
                "model": model,
                "horizon": int(horizon),
                "n": len(g),
                "mae": float(np.mean(np.abs(err))),
                "rmse": float(np.sqrt(np.mean(err**2))),
                "bias": float(np.mean(err)),
                "sign_accuracy": float(np.mean(np.sign(q50) == np.sign(y))),
                "pinball": float(np.mean([
                    pinball(y, g["q10"].to_numpy(), 0.1),
                    pinball(y, q50, 0.5),
                    pinball(y, g["q90"].to_numpy(), 0.9),
                ])),
                "coverage_80": float(np.mean((y >= g["q10"]) & (y <= g["q90"]))),
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    ref = out[out["model"] == "persistence"].set_index(["mode", "horizon"])["mae"]
    out["skill_vs_persistence"] = [
        1 - r.mae / ref.get((r.mode, r.horizon), np.nan) for r in out.itertuples()
    ]
    return out
