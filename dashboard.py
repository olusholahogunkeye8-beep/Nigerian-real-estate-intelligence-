from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


BASE_DIR = Path(r"C:\Users\USER\Downloads")
DATA_PATH = BASE_DIR / "nigeria_real_estate_analysis_ready.csv"
STATE_SUMMARY_PATH = BASE_DIR / "nigeria_real_estate_state_analysis.csv"


st.set_page_config(
    page_title="Nigeria Real Estate Dashboard",
    page_icon=":material/query_stats:",
    layout="wide",
    initial_sidebar_state="expanded",
)


def naira(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"₦{float(value):,.0f}"


def compact_naira(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    value = float(value)
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"₦{round(value / 1_000_000_000):,}B"
    if abs_value >= 1_000_000:
        return f"₦{round(value / 1_000_000):,}M"
    if abs_value >= 1_000:
        return f"₦{round(value / 1_000):,}K"
    return f"₦{value:,.0f}"


@st.cache_data
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(DATA_PATH)
    state_summary = pd.read_csv(STATE_SUMMARY_PATH)

    for col in ["price", "bedrooms", "bathrooms"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["added_date"] = pd.to_datetime(df["added_date"], format="%d/%m/%Y", errors="coerce")
    df["updated_date"] = pd.to_datetime(df["updated_date"], format="%d/%m/%Y", errors="coerce")
    df["month_posted"] = pd.to_datetime(df["month_posted"], format="%Y-%m", errors="coerce")
    df["price_per_bedroom"] = df["price"] / df["bedrooms"]
    df.loc[df["bedrooms"].isna() | (df["bedrooms"] <= 0), "price_per_bedroom"] = pd.NA

    return df, state_summary


def metric_summary(df: pd.DataFrame) -> dict[str, float]:
    bedroom_df = df[df["bedrooms"].fillna(0) > 0]
    return {
        "listing_count": int(len(df)),
        "median_price": float(df["price"].median()) if not df.empty else 0.0,
        "median_price_per_bedroom": float(bedroom_df["price_per_bedroom"].median()) if not bedroom_df.empty else 0.0,
        "bedroom_valid_count": int(len(bedroom_df)),
        "min_price": float(df["price"].min()) if not df.empty else 0.0,
        "max_price": float(df["price"].max()) if not df.empty else 0.0,
    }


def insufficiency_message(df: pd.DataFrame, state: str | None, property_type: str | None) -> str | None:
    if state and property_type and len(df) < 5:
        return "There are not enough listings for this property type in this state for analysis."
    return None


def make_bar_chart(data: pd.DataFrame, x: str, y: str, title: str = "", horizontal: bool = False) -> alt.Chart:
    if horizontal:
        chart = alt.Chart(data).mark_bar(cornerRadiusEnd=5).encode(
            y=alt.Y(f"{x}:N", sort="-x", title=None),
            x=alt.X(f"{y}:Q", title=None),
            tooltip=[x, alt.Tooltip(y, format=",.2f")],
        )
    else:
        chart = alt.Chart(data).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
            x=alt.X(f"{x}:N", sort="-y", title=None),
            y=alt.Y(f"{y}:Q", title=None),
            tooltip=[x, alt.Tooltip(y, format=",.2f")],
        )
    return chart.properties(height=320, title=title)


def make_line_chart(data: pd.DataFrame, x: str, y: str, title: str = "") -> alt.Chart:
    return (
        alt.Chart(data)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:T", title=None),
            y=alt.Y(f"{y}:Q", title=None),
            tooltip=[alt.Tooltip(f"{x}:T", title="Month"), alt.Tooltip(y, format=",.2f")],
        )
        .properties(height=300, title=title)
    )


df, state_summary = load_data()

st.markdown(
    """
    ### Nigeria Real Estate Intelligence Dashboard
    Explore cleaned real estate listings across Nigeria by state, property type, and area,
    with median-based pricing views built for clearer market analysis.
    """
)


with st.sidebar:
    st.header("Dashboard Filters")
    selected_state = st.selectbox("State", options=["All"] + sorted(df["state"].dropna().unique().tolist()), index=0)
    selected_property_type = st.selectbox(
        "Property type",
        options=["All"] + sorted(df["property_type"].dropna().unique().tolist()),
        index=0,
    )
    selected_area_bucket = st.selectbox(
        "Area",
        options=["All"] + sorted(df["area_bucket"].dropna().unique().tolist()),
        index=0,
    )
    selected_price_category = st.selectbox(
        "Price category",
        options=["All"] + sorted(df["price_category"].dropna().unique().tolist()),
        index=0,
    )
    listing_limit = st.slider("Listings to display", min_value=25, max_value=250, value=100, step=25)
    st.caption(
        "Reliability rule: when a selected state + property type combination has fewer than 5 listings, "
        "the dashboard flags it as insufficient for proper analysis."
    )


state = None if selected_state == "All" else selected_state
property_type = None if selected_property_type == "All" else selected_property_type
area_bucket = None if selected_area_bucket == "All" else selected_area_bucket
price_category = None if selected_price_category == "All" else selected_price_category

filtered = df.copy()
if state:
    filtered = filtered[filtered["state"] == state]
if property_type:
    filtered = filtered[filtered["property_type"] == property_type]
if area_bucket:
    filtered = filtered[filtered["area_bucket"] == area_bucket]
if price_category:
    filtered = filtered[filtered["price_category"] == price_category]

summary = metric_summary(filtered)
warning_text = insufficiency_message(filtered, state, property_type)

monthly = (
    filtered.dropna(subset=["month_posted"])
    .groupby("month_posted", as_index=False)
    .agg(median_price=("price", "median"), listings=("price", "size"))
    .sort_values("month_posted")
)
spark_values = monthly["median_price"].tolist() if not monthly.empty else None

with st.container(horizontal=True):
    st.metric("Available Listings", f"{summary['listing_count']:,}", border=True)
    st.metric(
        "Median Price",
        compact_naira(summary["median_price"]),
        border=True,
        chart_data=spark_values,
        chart_type="line",
    )
    st.metric(
        "Price Range",
        f"{compact_naira(summary['min_price'])} ~ {compact_naira(summary['max_price'])}",
        border=True,
    )
    st.metric(
        "Median Price per Bedroom",
        compact_naira(summary["median_price_per_bedroom"]),
        border=True,
        help=f"Calculated from {summary['bedroom_valid_count']:,} listings with bedrooms > 0.",
    )

if warning_text:
    st.warning(warning_text)


overview_tab, pricing_tab, area_tab, listings_tab = st.tabs(
    ["Overview", "Pricing & Reliability", "Area Intelligence", "Listings"]
)

with overview_tab:
    col1, col2 = st.columns(2)

    state_pricing = (
        filtered.groupby("state", as_index=False)
        .agg(listings=("price", "size"), median_price=("price", "median"))
        .sort_values("median_price", ascending=False)
    )
    property_mix = (
        filtered.groupby("property_type", as_index=False)
        .agg(listings=("price", "size"), median_price=("price", "median"))
        .sort_values("listings", ascending=False)
    )

    with col1:
        with st.container(border=True):
            st.subheader("Median Price by State")
            if state_pricing.empty:
                st.info("No state pricing data is available.")
            else:
                st.altair_chart(
                    make_bar_chart(state_pricing.head(12), "state", "median_price", horizontal=True),
                    use_container_width=True,
                )
                state_display = state_pricing.copy()
                state_display["median_price"] = state_display["median_price"].map(naira)
                st.dataframe(state_display, hide_index=True, use_container_width=True)

    with col2:
        with st.container(border=True):
            st.subheader("Listings by Property Type")
            if property_mix.empty:
                st.info("No property type data is available.")
            else:
                st.altair_chart(
                    make_bar_chart(property_mix, "property_type", "listings"),
                    use_container_width=True,
                )
                property_display = property_mix.copy()
                property_display["median_price"] = property_display["median_price"].map(naira)
                st.dataframe(property_display, hide_index=True, use_container_width=True)

    with st.container(border=True):
        st.subheader("Median Monthly Trend")
        if monthly.empty:
            st.info("No monthly trend is available for the current filters.")
        else:
            st.altair_chart(make_line_chart(monthly, "month_posted", "median_price"), use_container_width=True)
            monthly_display = monthly.copy()
            monthly_display["month_posted"] = monthly_display["month_posted"].dt.strftime("%Y-%m")
            monthly_display["median_price"] = monthly_display["median_price"].map(naira)
            st.dataframe(monthly_display, hide_index=True, use_container_width=True)

with pricing_tab:
    left, right = st.columns([1.1, 0.9])

    with left:
        with st.container(border=True):
            st.subheader("State Pricing Summary")
            state_view = state_summary.copy()
            if state:
                state_view = state_view[state_view["state"] == state]
            if state_view.empty:
                st.info("No state summary rows match the current filters.")
            else:
                display = state_view.copy()
                for col in ["average_price", "median_price", "min_price", "max_price"]:
                    if col in display.columns:
                        display[col] = pd.to_numeric(display[col], errors="coerce").map(naira)
                st.dataframe(display, hide_index=True, use_container_width=True)

    with right:
        with st.container(border=True):
            st.subheader("Median Price Per Bedroom")
            bedroom_df = filtered[filtered["bedrooms"].fillna(0) > 0]
            if bedroom_df.empty:
                st.info("No listings with valid bedroom counts are available for the current filters.")
            else:
                bedroom_summary = (
                    bedroom_df.groupby(["state", "property_type"], as_index=False)
                    .agg(listings=("price", "size"), median_price_per_bedroom=("price_per_bedroom", "median"))
                    .sort_values("median_price_per_bedroom", ascending=False)
                )
                bedroom_display = bedroom_summary.copy()
                bedroom_display["median_price_per_bedroom"] = bedroom_display["median_price_per_bedroom"].map(naira)
                st.dataframe(bedroom_display, hide_index=True, use_container_width=True)

    with st.container(border=True):
        st.subheader("State + Property Type Reliability")
        reliability = (
            filtered.groupby(["state", "property_type"], as_index=False)
            .agg(listings=("price", "size"), median_price=("price", "median"))
            .sort_values(["listings", "state", "property_type"], ascending=[True, True, True])
        )
        if reliability.empty:
            st.info("No reliability data is available for the current filters.")
        else:
            reliability["sufficient_for_analysis"] = reliability["listings"] >= 5
            reliability["message"] = reliability["sufficient_for_analysis"].map(
                lambda ok: "Sufficient" if ok else "There are not enough listings for this property type in this state for analysis."
            )
            reliability["median_price"] = reliability["median_price"].map(naira)
            st.dataframe(reliability, hide_index=True, use_container_width=True)

with area_tab:
    left, right = st.columns([1.15, 0.85])

    area_summary = (
        filtered.groupby(["state", "area_bucket"], as_index=False)
        .agg(
            listings=("price", "size"),
            median_price=("price", "median"),
            min_price=("price", "min"),
            max_price=("price", "max"),
        )
        .sort_values("median_price", ascending=False)
    )

    with left:
        with st.container(border=True):
            st.subheader("Top 10 Most Expensive Areas by Median Price")
            leaderboard = area_summary[area_summary["listings"] >= 5].head(10).copy()
            if leaderboard.empty:
                st.info("No area buckets with at least 5 listings match the current filters.")
            else:
                leaderboard["state_area"] = leaderboard["state"] + " / " + leaderboard["area_bucket"]
                st.altair_chart(
                    make_bar_chart(leaderboard, "state_area", "median_price", horizontal=True),
                    use_container_width=True,
                )
                display = leaderboard.copy()
                for col in ["median_price", "min_price", "max_price"]:
                    display[col] = display[col].map(naira)
                st.dataframe(display, hide_index=True, use_container_width=True)

    with right:
        with st.container(border=True):
            st.subheader("Area Bucket Coverage")
            if area_summary.empty:
                st.info("No area data is available.")
            else:
                coverage = area_summary[["state", "area_bucket", "listings"]].sort_values("listings", ascending=False).head(15)
                st.dataframe(coverage, hide_index=True, use_container_width=True)

with listings_tab:
    with st.container(border=True):
        st.subheader("Listing Explorer")
        listings_df = filtered[
            [
                "title",
                "location",
                "area_bucket",
                "property_type",
                "price",
                "bedrooms",
                "bathrooms",
                "month_posted",
                "state",
                "price_category",
            ]
        ].copy().head(listing_limit)
        if listings_df.empty:
            st.info("No listings are available for the current filters.")
        else:
            listings_df["price"] = listings_df["price"].map(naira)
            listings_df["month_posted"] = listings_df["month_posted"].dt.strftime("%Y-%m")
            st.dataframe(
                listings_df,
                hide_index=True,
                use_container_width=True,
                height=520,
                column_config={
                    "title": st.column_config.TextColumn("Title", width="large"),
                    "location": st.column_config.TextColumn("Location", width="large"),
                    "area_bucket": st.column_config.TextColumn("Area"),
                    "property_type": st.column_config.TextColumn("Property Type"),
                    "price": st.column_config.TextColumn("Price"),
                    "bedrooms": st.column_config.NumberColumn("Bedrooms", format="%d"),
                    "bathrooms": st.column_config.NumberColumn("Bathrooms", format="%d"),
                    "month_posted": st.column_config.TextColumn("Month Posted"),
                    "state": st.column_config.TextColumn("State"),
                    "price_category": st.column_config.TextColumn("Price Category"),
                },
            )