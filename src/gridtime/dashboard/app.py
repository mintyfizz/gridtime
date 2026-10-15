"""GridTime dashboard (reads Silver; Bronze only for pipeline health).

Run:  make dashboard   →  http://localhost:8501
"""

from datetime import UTC, datetime, timedelta

import altair as alt
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import streamlit as st

from gridtime.dashboard import data
from gridtime.storage.tables import bronze_table, silver_table
from gridtime.transform import asof

st.set_page_config(page_title="GridTime", layout="wide")

# Categorical slots 1–2, stepped per theme. Fixed by stage, never by rank.
LIGHT = {"near_real_time": "#2a78d6", "historical": "#eb6834"}
DARK = {"near_real_time": "#3987e5", "historical": "#d95926"}
WINDOWS = {"6 h": 6, "24 h": 24, "48 h": 48, "All": None}
TZ = "Europe/Brussels"


def theme_colors() -> dict[str, str]:
    theme = getattr(getattr(st, "context", None), "theme", None)
    return DARK if getattr(theme, "type", None) == "dark" else LIGHT


@st.cache_data(ttl=60, show_spinner="Reading the lakehouse…")
def load() -> tuple[pa.Table, pa.Table]:
    return silver_table().scan().to_arrow(), bronze_table().scan().to_arrow()


def stage_scale(colors: dict[str, str]) -> alt.Scale:
    return alt.Scale(domain=[data.STAGE_LABELS[k] for k in colors], range=list(colors.values()))


def to_frame(table: pa.Table, hours: int | None) -> pd.DataFrame:
    df = table.to_pandas()
    if hours is not None and not df.empty:
        df = df[df.valid_from >= pd.Timestamp(datetime.now(UTC) - timedelta(hours=hours))]
    df["stage_label"] = df["stage"].map(data.STAGE_LABELS)
    return df


def line_chart(df: pd.DataFrame, y_title: str, colors: dict[str, str]) -> alt.LayerChart:
    """Values per minute, coloured by publication stage, with a crosshair tooltip."""
    hover = alt.selection_point(
        fields=["valid_from"], nearest=True, on="pointerover", clear="pointerout", empty=False
    )
    base = alt.Chart(df).encode(
        x=alt.X("valid_from:T", title=None),
        y=alt.Y("value:Q", title=y_title),
        color=alt.Color(
            "stage_label:N", scale=stage_scale(colors), legend=alt.Legend(orient="top", title=None)
        ),
    )
    lines = base.mark_line(strokeWidth=2, interpolate="step-after")
    points = base.mark_point(size=60, filled=True).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    )
    rule = (
        alt.Chart(df)
        .mark_rule(color="gray", strokeWidth=1)
        .encode(
            x="valid_from:T",
            opacity=alt.condition(hover, alt.value(0.5), alt.value(0)),
            tooltip=[
                alt.Tooltip("valid_from:T", title="Minute", format="%d %b %H:%M"),
                alt.Tooltip("stage_label:N", title="Stage"),
                alt.Tooltip("value:Q", title=y_title, format=",.1f"),
                alt.Tooltip("quality_status:N", title="Elia quality"),
                alt.Tooltip("transaction_time:T", title="Known since", format="%d %b %H:%M:%S"),
            ],
        )
        .add_params(hover)
    )
    zero = alt.Chart().mark_rule(color="gray", strokeDash=[4, 4]).encode(y=alt.datum(0))
    return alt.layer(lines, points, rule, zero, data=df).properties(height=340)


def local(ts) -> str:
    return pd.Timestamp(ts).tz_convert(TZ).strftime("%d %b %H:%M:%S")


# ---------------------------------------------------------------- layout
st.title("GridTime")
st.caption(
    "Belgian grid balancing data from Elia Open Data, kept in every version it was published. "
    "Each value carries the minute it describes and the moment GridTime learned it."
)

silver, bronze = load()
if silver.num_rows == 0:
    st.info("Silver is empty. Run `make ingest` then `make silver`, and refresh.")
    st.stop()

colors = theme_colors()
current = asof.current(silver)

f1, f2, f3 = st.columns([2, 3, 1])
window = f1.radio("Time range", list(WINDOWS), index=1, horizontal=True)
measure = f2.radio(
    "Measure", data.DASHBOARD_MEASURES, format_func=data.label, horizontal=True
)
if f3.button("Refresh", width="stretch"):
    load.clear()
    st.rerun()

label, unit = data.label(measure), data.unit(measure)

# ---- headline numbers (latest known)
cur = current.to_pandas()
nrt = cur[cur.stage == "near_real_time"]
imb = nrt[nrt.measure == "system_imbalance"].dropna(subset=["value"]).tail(1)
price = nrt[nrt.measure == "price_up_marginal"].dropna(subset=["value"]).tail(1)
last_received = pc.max(bronze["transaction_time"]).as_py()
age_min = (datetime.now(UTC) - last_received).total_seconds() / 60

k1, k2, k3, k4 = st.columns(4)
if not imb.empty:
    k1.metric(
        f"System imbalance · {imb.iloc[0].valid_from.tz_convert(TZ):%H:%M}",
        f"{imb.iloc[0].value:,.0f} MW",
        help="Negative = grid short (more consumption than generation). Near real-time value.",
    )
if not price.empty:
    k2.metric("Upward activation price", f"€{price.iloc[0].value:,.0f}/MWh")
k3.metric(
    "Versions in Silver",
    f"{silver.num_rows:,}",
    help="One row per (minute, measure, version). Unchanged re-fetches are not stored.",
)
k4.metric("Last ingest", f"{age_min:,.0f} min ago", help=local(last_received))

# ---- time travel
st.subheader(f"{label} per minute ({unit})")
moments = data.transaction_times(silver)
pick = st.select_slider(
    "As known at",
    options=range(len(moments)),
    value=len(moments) - 1,
    format_func=lambda i: local(moments[i]),
    help="Rewind to any moment GridTime received new data. Values that arrived later are hidden.",
)
known = moments[pick]
rewound = pick != len(moments) - 1

df = to_frame(data.known_at(silver, measure, known), WINDOWS[window]).dropna(subset=["value"])
if df.empty:
    st.info("Nothing known for this measure in the selected window at that moment.")
else:
    st.altair_chart(line_chart(df, f"{label} ({unit})", colors), width="stretch")
    if rewound:
        st.caption(
            f"Showing what GridTime knew at {local(known)}. "
            "Drag the slider to the right end to see the latest values."
        )
    else:
        st.caption(
            "Each minute shows its latest known version. Near real-time values arrive within "
            "minutes; the historical feed republishes them the next day."
        )

# ---- revisions
st.subheader("Revisions: first publication vs historical")
rev = asof.revisions(silver, measure).to_pandas()
if rev.empty:
    st.info(
        "No revised minutes yet. A minute appears here once GridTime has seen it in the near "
        "real-time feed and later in the historical feed, which needs the poller to run across "
        "midnight (`make poll`, then `make silver`)."
    )
else:
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Minutes compared", f"{len(rev):,}")
    r2.metric("Changed", f"{(rev.revision.abs() > 1e-9).mean():.0%}")
    r3.metric("Mean absolute revision", f"{rev.revision.abs().mean():,.1f} {unit}")
    r4.metric("Largest revision", f"{rev.revision.abs().max():,.1f} {unit}")
    rev_chart = (
        alt.Chart(rev)
        .mark_line(strokeWidth=2, color=colors["historical"])
        .encode(
            x=alt.X("valid_from:T", title=None),
            y=alt.Y("revision:Q", title=f"Historical − first published ({unit})"),
            tooltip=[
                alt.Tooltip("valid_from:T", title="Minute", format="%d %b %H:%M"),
                alt.Tooltip("first_value:Q", title="First published", format=",.1f"),
                alt.Tooltip("latest_value:Q", title="Historical", format=",.1f"),
                alt.Tooltip("revision:Q", format="+,.1f"),
                alt.Tooltip("versions:Q", title="Versions"),
            ],
        )
        .properties(height=240)
    )
    zero = alt.Chart().mark_rule(color="gray", strokeDash=[4, 4]).encode(y=alt.datum(0))
    st.altair_chart(alt.layer(rev_chart, zero, data=rev), width="stretch")

# ---- pipeline health
st.subheader("Pipeline")
p1, p2 = st.columns(2)
with p1:
    st.markdown("**Recent polls** (Bronze rows written per feed)")
    batches = data.ingest_batches(bronze).to_pandas()
    batches["feed"] = batches["dataset_id"].map(
        {"ods175": data.STAGE_LABELS["near_real_time"], "ods165": data.STAGE_LABELS["historical"]}
    )
    batches["received"] = batches["transaction_time"].dt.tz_convert(TZ)
    st.dataframe(
        batches.sort_values("received", ascending=False)[["received", "feed", "rows"]].head(12),
        hide_index=True,
        width="stretch",
        column_config={"received": st.column_config.DatetimeColumn(format="D MMM HH:mm:ss")},
    )
with p2:
    st.markdown(f"**Versions per minute** for {label.lower()} (more than 1 = revised)")
    st.dataframe(
        data.version_distribution(silver, measure).to_pandas(), hide_index=True, width="stretch"
    )
    st.markdown("**Elia quality status** (latest version of every minute × measure)")
    quality = data.quality_by_stage(current).to_pandas()
    quality["stage"] = quality["stage"].map(data.STAGE_LABELS)
    quality = quality.rename(columns={"value_count": "values"})
    st.dataframe(quality, hide_index=True, width="stretch")

with st.expander("Version history of one minute"):
    minutes = sorted(cur[cur.measure == measure].valid_from.unique(), reverse=True)
    if minutes:
        pick = st.selectbox("Minute", minutes, format_func=lambda t: local(t)[:-3])
        hist = asof.history(silver, pick.to_pydatetime(), measure).to_pandas()
        st.dataframe(
            hist[["transaction_time", "stage", "value", "quality_status", "source_dataset"]],
            hide_index=True,
            width="stretch",
        )
