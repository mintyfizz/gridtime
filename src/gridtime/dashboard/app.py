"""GridTime dashboard (milestone 1: reads Bronze).

Run:  make dashboard   →  http://localhost:8501
"""

from datetime import UTC, datetime, timedelta

import altair as alt
import pyarrow as pa
import pyarrow.compute as pc
import streamlit as st

from gridtime.dashboard import data
from gridtime.storage.tables import bronze_table

st.set_page_config(page_title="GridTime", layout="wide")

# Categorical slots 1–2, stepped per theme. Fixed by feed, never by rank.
LIGHT = {"ods175": "#2a78d6", "ods165": "#eb6834"}
DARK = {"ods175": "#3987e5", "ods165": "#d95926"}
WINDOWS = {"6 h": 6, "24 h": 24, "48 h": 48, "All": None}


def theme_colors() -> dict[str, str]:
    theme = getattr(getattr(st, "context", None), "theme", None)
    return DARK if getattr(theme, "type", None) == "dark" else LIGHT


@st.cache_data(ttl=60, show_spinner="Reading Bronze…")
def load_bronze() -> pa.Table:
    return bronze_table().scan().to_arrow()


def in_window(table: pa.Table, hours: int | None) -> pa.Table:
    if hours is None or table.num_rows == 0:
        return table
    cutoff = pa.scalar(datetime.now(UTC) - timedelta(hours=hours), pa.timestamp("us", tz="UTC"))
    return table.filter(pc.greater_equal(table["valid_time"], cutoff))


def feed_scale(colors: dict[str, str]) -> alt.Scale:
    return alt.Scale(
        domain=[data.FEEDS[k] for k in colors], range=list(colors.values())
    )


def line_chart(df, y: str, y_title: str, colors: dict[str, str]) -> alt.LayerChart:
    """Lines per feed with a crosshair tooltip on the nearest minute."""
    hover = alt.selection_point(
        fields=["valid_time"], nearest=True, on="pointerover", clear="pointerout", empty=False
    )
    base = alt.Chart(df).encode(
        x=alt.X("valid_time:T", title=None),
        y=alt.Y(f"{y}:Q", title=y_title),
        color=alt.Color(
            "feed:N", scale=feed_scale(colors), legend=alt.Legend(orient="top", title=None)
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
            x="valid_time:T",
            opacity=alt.condition(hover, alt.value(0.5), alt.value(0)),
            tooltip=[
                alt.Tooltip("valid_time:T", title="Minute", format="%d %b %H:%M"),
                alt.Tooltip("feed:N", title="Feed"),
                alt.Tooltip(f"{y}:Q", title=y_title, format=",.1f"),
            ],
        )
        .add_params(hover)
    )
    zero = alt.Chart().mark_rule(color="gray", strokeDash=[4, 4]).encode(y=alt.datum(0))
    return alt.layer(lines, points, rule, zero, data=df).properties(height=340)


# ---------------------------------------------------------------- layout
st.title("GridTime")
st.caption(
    "Belgian grid balancing data from Elia Open Data, kept in every version it was published. "
    "Milestone 1 view over the Bronze layer."
)

bronze = load_bronze()
if bronze.num_rows == 0:
    st.info("Bronze is empty. Run `make ingest` (or `make poll`) and refresh.")
    st.stop()

colors = theme_colors()
current = data.current_view(bronze)

f1, f2, f3 = st.columns([2, 3, 1])
window = f1.radio("Time range", list(WINDOWS), index=1, horizontal=True)
measure = f2.radio(
    "Measure",
    list(data.MEASURES),
    format_func=lambda m: data.MEASURES[m][0],
    horizontal=True,
)
if f3.button("Refresh", width="stretch"):
    load_bronze.clear()
    st.rerun()

label, unit = data.MEASURES[measure]
view = in_window(current, WINDOWS[window])
df = view.to_pandas()
df["feed"] = df["dataset_id"].map(data.FEEDS)

# ---- headline numbers
latest_prelim = df[df.dataset_id == "ods175"].dropna(subset=["systemimbalance"]).tail(1)
latest_price = df[df.dataset_id == "ods175"].dropna(
    subset=["marginalincrementalprice"]
).tail(1)
last_received = pc.max(bronze["transaction_time"]).as_py()
age_min = (datetime.now(UTC) - last_received).total_seconds() / 60

k1, k2, k3, k4 = st.columns(4)
if not latest_prelim.empty:
    row = latest_prelim.iloc[0]
    k1.metric(
        f"System imbalance · {row.valid_time.tz_convert('Europe/Brussels'):%H:%M}",
        f"{row.systemimbalance:,.0f} MW",
        help="Negative = grid short (more consumption than generation). Preliminary value.",
    )
if not latest_price.empty:
    k2.metric(
        "Upward activation price",
        f"€{latest_price.iloc[0].marginalincrementalprice:,.0f}/MWh",
    )
k3.metric("Rows in Bronze", f"{bronze.num_rows:,}")
k4.metric(
    "Last ingest",
    f"{age_min:,.0f} min ago",
    help=f"{last_received.astimezone():%d %b %H:%M:%S}",
)

# ---- main chart
st.subheader(f"{label} per minute ({unit})")
plot_df = df.dropna(subset=[measure])
if plot_df.empty:
    st.info("No values for this measure in the selected window.")
else:
    chart = line_chart(plot_df, measure, f"{label} ({unit})", colors)
    st.altair_chart(chart, width="stretch")
    st.caption(
        "Preliminary values arrive within minutes; validated values replace them the next day. "
        "Where both exist for the same minute, the gap between them is the revision."
    )

# ---- revisions
st.subheader("Revisions: preliminary vs validated")
rev = data.revisions(current, measure).to_pandas()
if rev.empty:
    st.info(
        "No overlap yet. The preliminary feed only covers the current hours and the validated "
        "feed ends at yesterday, so overlap appears once the poller has run across midnight "
        "(`make poll`)."
    )
else:
    r1, r2, r3 = st.columns(3)
    r1.metric("Minutes in both feeds", f"{len(rev):,}")
    r2.metric("Mean absolute revision", f"{rev.revision.abs().mean():,.1f} {unit}")
    r3.metric("Largest revision", f"{rev.revision.abs().max():,.1f} {unit}")
    rev_chart = (
        alt.Chart(rev)
        .mark_line(strokeWidth=2, color=colors["ods175"])
        .encode(
            x=alt.X("valid_time:T", title=None),
            y=alt.Y("revision:Q", title=f"Validated − preliminary ({unit})"),
            tooltip=[
                alt.Tooltip("valid_time:T", title="Minute", format="%d %b %H:%M"),
                alt.Tooltip("preliminary:Q", format=",.1f"),
                alt.Tooltip("validated:Q", format=",.1f"),
                alt.Tooltip("revision:Q", format="+,.1f"),
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
    st.markdown("**Recent polls** (rows written per feed)")
    batches = data.ingest_batches(bronze).to_pandas()
    batches["feed"] = batches["dataset_id"].map(data.FEEDS)
    batches["received"] = batches["transaction_time"].dt.tz_convert("Europe/Brussels")
    st.dataframe(
        batches.sort_values("received", ascending=False)[["received", "feed", "rows"]].head(12),
        hide_index=True,
        width="stretch",
        column_config={"received": st.column_config.DatetimeColumn(format="D MMM HH:mm:ss")},
    )
with p2:
    st.markdown("**Versions per minute** (more than 1 = Elia revised it)")
    versions = data.version_counts(bronze).to_pandas()
    versions["feed"] = versions["dataset_id"].map(data.FEEDS)
    st.dataframe(
        versions[["feed", "versions", "minutes"]], hide_index=True, width="stretch"
    )
    st.markdown("**Elia quality status** (current versions)")
    quality = (
        df.groupby(["feed", "qualitystatus"], dropna=False).size().reset_index(name="minutes")
    )
    st.dataframe(quality, hide_index=True, width="stretch")

with st.expander("Data table (current versions in the selected window)"):
    st.dataframe(
        df[["feed", "valid_time", "transaction_time", "qualitystatus", *data.MEASURES]],
        hide_index=True,
        width="stretch",
    )
