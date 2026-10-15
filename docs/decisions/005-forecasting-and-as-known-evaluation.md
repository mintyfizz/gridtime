# 005 – Quarter-hour imbalance forecasting, evaluated as known at decision time

**Status:** accepted · 2026-10-15

**Context.** The point of keeping every version (decisions 001–004) is to evaluate forecasts
honestly. Most forecasting backtests build features from the final, revised data, which assumes
that at decision time every past minute was already published, complete and final. In reality:

- Elia publishes a minute ~2 minutes after it starts, and GridTime polls every 5 minutes, so at
  a quarter-hour boundary the last few minutes of the previous quarter-hour are not known yet.
- Near-real-time values are later republished in the historical feed, possibly revised.
- History that was backfilled today was not known yesterday.

GridTime started collecting on 2026-10-15, so it has no long as-known history yet.

**Decision.**
- **Target:** mean system imbalance (MW) of the next quarter-hour (h=1) and the one after
  (h=2), decided at each quarter-hour boundary. Quarter-hours are the settlement resolution of
  Belgian imbalance.
- **Models:** persistence, same quarter-hour yesterday, climatology by quarter-hour of day,
  and histogram gradient boosting (scikit-learn) with quantile loss for q10/q50/q90.
  Gradient-boosting intervals are conformally calibrated on the most recent 15% of the
  training period; baselines get intervals from training residuals. scikit-learn over
  LightGBM: same model family, no OpenMP system dependency on macOS.
- **One feature function, two inputs.** Features at decision time t are computed from the
  minute series *as known at t* (Silver, `transaction_time <= t`, `valid_from < t`). The
  revised backtest passes the latest series; the as-known backtest passes `as_of(t)`. A test
  asserts that adding data from after t never changes the features at t.
- **Three evaluation modes:**
  - `revised` – 4-week walk-forward over the backfilled history (retrained per fold).
  - `as_known` – decisions made while GridTime was live and already held enough history,
    features as known at that moment; models trained only on data with targets before the
    window starts.
  - `revised_same_period` – the same decisions with final features. `as_known` vs this mode
    isolates the cost of real data availability.
- **Live forecasts are logged and scored later.** After every Silver update Dagster forecasts
  the next two quarter-hours and appends to `data/forecast/live.parquet`. These are scored once
  their quarter-hour completes: no simulation at all.
- **Backfill stamped honestly.** The 8-week backfill carries the backfill time as its
  transaction time, so as-known evaluation correctly starts only once that history was known.
- **MLflow** (SQLite store under `data/`) logs every backtest run: parameters, summary and
  per-mode/model/horizon metrics.

**First results (revised, 2,640 decisions over 4 weeks).** Gradient boosting: MAE 69 MW at
h=1, 18% better than persistence (85 MW), sign right 82% of the time, 80% intervals covering
81%. At h=2: 13% better than persistence. Same-quarter-hour-yesterday is twice as bad as
persistence: system imbalance is persistent, not daily-seasonal.

**Consequences.** The as-known results start empty and grow by 96 decisions per day; the
revision part of the gap only appears once minutes have passed through both feeds. On
synthetic data with realistic 5-minute polling, the as-known penalty was ~14% of MAE. The live
model is trained on revised history but fed as-known features: that training/serving skew is
exactly what the as-known numbers measure.
