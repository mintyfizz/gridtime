"""Train, backtest, log to MLflow, and publish forecasts as Parquet.

    python -m gridtime.forecasting.pipeline backtest   # daily: retrain + both backtests
    python -m gridtime.forecasting.pipeline live       # every run: forecast the next QHs

Outputs (data/forecast/, atomic Parquet so the dashboard never reads half a file):
    predictions.parquet   all backtest predictions
    metrics.parquet       scores per mode, model and horizon
    model.joblib          models trained on all data, used for live forecasts
    live.parquet          every live forecast ever made (scored later, for real)
"""

import argparse
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import joblib
import pandas as pd

from gridtime.config import get_settings
from gridtime.forecasting import backtest
from gridtime.forecasting.features import FeatureSource
from gridtime.forecasting.metrics import score
from gridtime.forecasting.series import QH, known_minutes, load_versions, qh_ceil

log = logging.getLogger("gridtime.forecast")
LIVE_MODELS = ("persistence", "gradient_boosting")


def out_dir() -> Path:
    d = get_settings().data_path / "forecast"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def _silver_versions() -> pd.DataFrame:
    from gridtime.storage.tables import silver_table

    return load_versions(silver_table().scan().to_arrow())


def run_backtest(versions: pd.DataFrame | None = None, log_mlflow: bool = True) -> dict:
    versions = _silver_versions() if versions is None else versions
    p = backtest.prepare(versions)
    revised = backtest.revised_backtest(p)
    as_known, info = backtest.as_known_backtest(versions, p)
    predictions = pd.concat([revised, as_known], ignore_index=True)
    metrics = score(predictions)

    # Final models on everything known now, for live forecasting.
    final = backtest.fit_models(p, p.truth.index.max() + QH)
    d = out_dir()
    write_parquet(predictions, d / "predictions.parquet")
    write_parquet(metrics, d / "metrics.parquet")
    joblib.dump({"models": final, "trained_at": datetime.now(UTC)}, d / "model.joblib")

    summary = {
        "history_days": float((p.minutes.index.max() - p.minutes.index.min()).days),
        "revised_decisions": int(revised["decision_time"].nunique()) if len(revised) else 0,
        **{k: v for k, v in info.items() if k != "as_known_start"},
        "as_known_start": str(info.get("as_known_start")),
    }
    if log_mlflow:
        _log_mlflow(metrics, summary, d)
    log.info("Backtest done: %s", summary)
    return {"metrics": metrics, "summary": summary}


def _log_mlflow(metrics: pd.DataFrame, summary: dict, d: Path) -> None:
    import mlflow

    s = get_settings()
    mlflow.set_tracking_uri(f"sqlite:///{s.data_path / 'mlflow.db'}")
    name = "gridtime-backtest"
    if mlflow.get_experiment_by_name(name) is None:
        # Keep artifacts under data/, not ./mlruns in whatever directory we run from.
        mlflow.create_experiment(name, artifact_location=(s.data_path / "mlartifacts").as_uri())
    mlflow.set_experiment(name)
    with mlflow.start_run(run_name=f"backtest-{datetime.now(UTC):%Y%m%dT%H%M}"):
        mlflow.log_params({k: v for k, v in summary.items() if isinstance(v, str)})
        mlflow.log_metrics({k: v for k, v in summary.items() if isinstance(v, int | float)})
        for r in metrics.itertuples():
            prefix = f"{r.mode}.{r.model}.h{r.horizon}"
            for name in ("mae", "rmse", "skill_vs_persistence", "sign_accuracy", "coverage_80"):
                value = getattr(r, name)
                if pd.notna(value):
                    mlflow.log_metric(f"{prefix}.{name}", float(value))
        mlflow.log_artifact(str(d / "metrics.parquet"))


def run_live(versions: pd.DataFrame | None = None, now: datetime | None = None) -> pd.DataFrame:
    """Forecast the next quarter-hours from what is known right now, and log it."""
    d = out_dir()
    model_path = d / "model.joblib"
    if not model_path.exists():
        raise FileNotFoundError("No trained model yet: run the backtest first.")
    bundle = joblib.load(model_path)
    versions = _silver_versions() if versions is None else versions
    now = pd.Timestamp(now or datetime.now(UTC))
    t = qh_ceil(now)  # decision for the next full quarter-hour
    window = versions[versions["valid_from"] >= t - backtest.HISTORY - QH]
    X = pd.DataFrame([FeatureSource(known_minutes(window, known_at=now)).row(t)], index=[t])

    rows = []
    for h, models in bundle["models"].items():
        for m in models:
            if m.name not in LIVE_MODELS:
                continue
            pred = m.predict(X).iloc[0]
            rows.append({
                "made_at": now, "decision_time": t, "horizon": h,
                "target_qh": t + (h - 1) * QH, "model": m.name,
                "q10": pred["q10"], "q50": pred["q50"], "q90": pred["q90"],
                "lag_1_minutes_known": X["lag_1_minutes"].iloc[0],
            })
    new = pd.DataFrame(rows)
    live_path = d / "live.parquet"
    history = pd.read_parquet(live_path) if live_path.exists() else pd.DataFrame()
    write_parquet(pd.concat([history, new], ignore_index=True), live_path)
    log.info("Live forecast for %s: %s", t, new[["model", "horizon", "q50"]].to_dict("records"))
    return new


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["backtest", "live"])
    args = parser.parse_args()
    run_backtest() if args.command == "backtest" else run_live()
