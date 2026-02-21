"""
816039310
app.py: COMP 3610 Assignment 1
NYC Yellow Taxi Trip Dashboard (January 2024)

Run with:  streamlit run app.py
"""

import warnings
from pathlib import Path

import pandas as pd
import polars as pl
import duckdb
import plotly.express as px
import streamlit as st

warnings.filterwarnings("ignore")

RAW_DIR   = Path("data/raw")
TRIP_FILE = RAW_DIR / "yellow_tripdata_2024-01.parquet"
ZONE_FILE = RAW_DIR / "taxi_zone_lookup.csv"

TRIP_URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
ZONE_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DOW_ORDER   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
PAYMENT_MAP = {1:"Credit Card", 2:"Cash", 3:"No Charge", 4:"Dispute", 5:"Unknown"}



def _ensure_files():
    import requests
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for url, dest in [(TRIP_URL, TRIP_FILE), (ZONE_URL, ZONE_FILE)]:
        if dest.exists():
            continue
        st.info(f"Downloading {dest.name}…")
        r = requests.get(url, stream=True, timeout=300)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)



def _process(raw: pl.DataFrame) -> pl.DataFrame:
    CRITICAL = ["tpep_pickup_datetime","tpep_dropoff_datetime",
                "PULocationID","DOLocationID","fare_amount"]
    df = (
        raw
        .drop_nulls(subset=CRITICAL)
        .filter(pl.col("trip_distance") > 0)
        .filter((pl.col("fare_amount") > 0) & (pl.col("fare_amount") <= 500))
        .filter(pl.col("tpep_dropoff_datetime") > pl.col("tpep_pickup_datetime"))
    )
    df = df.with_columns([
        ((pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
         .dt.total_seconds() / 60).alias("trip_duration_minutes"),
        pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
        pl.col("tpep_pickup_datetime").dt.to_string("%A").alias("pickup_day_of_week"),
        pl.col("tpep_pickup_datetime").dt.date().alias("pickup_date"),
    ])
    df = df.with_columns([
        pl.when(pl.col("trip_duration_minutes") > 0)
          .then(pl.col("trip_distance") / (pl.col("trip_duration_minutes") / 60))
          .otherwise(0.0)
          .alias("trip_speed_mph"),
        pl.col("payment_type").map_elements(
            lambda x: PAYMENT_MAP.get(x, "Unknown"),
            return_dtype=pl.String,
        ).alias("payment_label"),
    ])
    return df


@st.cache_data(show_spinner="Loading data…")
def load_data():
    _ensure_files()
    raw    = pl.read_parquet(TRIP_FILE)
    full   = _process(raw)                          # ~3M rows
    sample = _process(raw.sample(n=100_000, seed=42))  # 100k rows
    zones  = pd.read_csv(ZONE_FILE)
    return full, sample, zones

@st.cache_data(show_spinner="Computing full-dataset metrics…")
def get_full_aggs(_full_arrow, _zones_pd):
    con = duckdb.connect()
    con.register("trips", _full_arrow)
    con.register("zones", _zones_pd)

    metrics = con.execute("""
        SELECT
            COUNT(*)                             AS total_trips,
            ROUND(AVG(fare_amount),   2)         AS avg_fare,
            ROUND(SUM(total_amount),  0)         AS total_revenue,
            ROUND(AVG(trip_distance), 2)         AS avg_distance,
            ROUND(AVG(trip_duration_minutes), 1) AS avg_duration
        FROM trips
    """).df()

    date_bounds = con.execute("""
        SELECT MIN(pickup_date) AS min_date, MAX(pickup_date) AS max_date FROM trips
    """).df()

    con.close()
    return metrics, date_bounds

@st.cache_data(show_spinner="Building sample charts…")
def get_sample_chart_data(_sample_arrow,_zones_pd):
    con = duckdb.connect()
    con.register("sample_trips", _sample_arrow)  
    con.register("zones", _zones_pd)
    
    top_zones = con.execute("""
        SELECT z.Zone AS pickup_zone, z.Borough AS borough, COUNT(*) AS total_trips
        FROM sample_trips t
        JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY z.Zone, z.Borough
        ORDER BY total_trips DESC
        LIMIT 10
    """).df()
    fare_by_hour = con.execute("""
        SELECT pickup_hour, ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM sample_trips
        GROUP BY pickup_hour ORDER BY pickup_hour
    """).df()

    payment = con.execute("""
        SELECT payment_label, COUNT(*) AS trips
        FROM sample_trips
        GROUP BY payment_label ORDER BY trips DESC
    """).df()

    dow_hour = con.execute("""
        SELECT pickup_day_of_week, pickup_hour, COUNT(*) AS trips
        FROM sample_trips
        GROUP BY pickup_day_of_week, pickup_hour
    """).df()

    dist = con.execute("SELECT trip_distance FROM sample_trips").df()

    con.close()
    return top_zones, fare_by_hour, payment, dow_hour, dist

def filter_sample(sample_df: pl.DataFrame, start_date, end_date,
                  hour_range, payments):
    payment_in = ", ".join(f"'{p}'" for p in payments)
    con = duckdb.connect()
    con.register("sample_trips", sample_df.to_arrow())
    arrow = con.execute(f"""
        SELECT * FROM sample_trips
        WHERE pickup_date   BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          AND pickup_hour   BETWEEN {hour_range[0]} AND {hour_range[1]}
          AND payment_label IN ({payment_in})
    """).arrow()
    con.close()
    return arrow

def chart_top_zones(df):
    fig = px.bar(
        df, x="total_trips", y="pickup_zone", orientation="h", color="borough",
        text="total_trips",
        labels={"total_trips":"Number of Trips","pickup_zone":"Taxi Zone"},
        title="Top 10 Pickup Zones: 100k Sample",
        color_discrete_sequence=px.colors.qualitative.Bold,
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig.update_layout(yaxis=dict(autorange="reversed"), height=420,
                      legend_title="Borough", margin=dict(l=10, r=90))
    return fig


def chart_fare_by_hour(df):
    fig = px.line(
        df, x="pickup_hour", y="avg_fare", markers=True,
        labels={"pickup_hour":"Hour of Day","avg_fare":"Average Fare ($)"},
        title="Average Fare by Hour of Day: 100k Sample",
        color_discrete_sequence=["steelblue"],
    )
    fig.update_traces(marker_size=7, line_width=2.5)
    fig.update_layout(
        height=400,
        xaxis=dict(
            tickmode="linear", 
            dtick=1,
            range=[0, 23.2],         
            fixedrange=True,       
        ),
    )
    return fig


def chart_distance_hist(df):
    fig = px.histogram(
        df, x="trip_distance",
        range_x=[0, 20.2],
        labels={"trip_distance": "Trip Distance (miles)"},
        title="Trip Distance Distribution: 100k Sample",
        color_discrete_sequence=["steelblue"],
    )
    fig.update_traces(xbins=dict(start=0, end=20, size=0.5))
    fig.update_layout(
        bargap=0.05,
        height=400,
        yaxis_title="Number of Trips",
        xaxis=dict(
            tickmode="linear",
            dtick=2,
        ),
        yaxis=dict(
            range=[0, 20000],
        ),
    )
    return fig


def chart_payment(df):
    fig = px.bar(
        df,
        x="payment_label",
        y="trips",
        title="Payment Type Breakdown: 100k Sample",
        labels={"payment_label": "Payment Type", "trips": "Number of Trips"},
        text="trips",
        color="payment_label",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig.update_layout(
        height=420,
        showlegend=False,
        yaxis=dict(
            range=[0, df["trips"].max() * 1.15],
        ),
        xaxis=dict(showgrid=False),
    )
    return fig


def chart_heatmap(df):
    pivot = (
        df.pivot(index="pickup_day_of_week", columns="pickup_hour", values="trips")
          .reindex(DOW_ORDER)
    )
    fig = px.imshow(
        pivot,
        labels=dict(x="Hour of Day", y="Day of Week", color="Trip Count"),
        title="Trips by Day of Week & Hour: 100k Sample",
        color_continuous_scale="YlOrRd",
        aspect="auto",
    )
    fig.update_layout(height=380)
    return fig

def main():
    st.set_page_config(
        page_title="NYC Yellow Taxi Dashboard",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.title(" NYC Yellow Taxi Trip Dashboard: January 2024")
    st.markdown("""
        This dashboard analyzes **NYC Yellow Taxi trips from January 2024**, exploring patterns
        in pickup demand, fare pricing, trip distances, payment behaviour, and weekly travel trends.
        **Metrics** uses the full ~3M row dataset. All charts use a 100,000-trip sample for performance.
        Click on the tabs to see the different visualizations.       
        *Data: [NYC Taxi and Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)*
    """)
    st.divider()

    full_df, sample_df, zones = load_data()

    metrics_df, date_bounds_df = get_full_aggs(full_df.to_arrow(), zones)
    m = metrics_df.iloc[0]

    top_zones_df, fare_by_hour, payment, dow_hour, dist = get_sample_chart_data(sample_df.to_arrow(), zones)

    st.subheader("Key Metrics: Full Dataset (~3M trips)")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Trips",       f"{int(m['total_trips']):,}")
    c2.metric("Average Fare",      f"${m['avg_fare']:,.2f}")
    c3.metric("Total Revenue",     f"${int(m['total_revenue']):,}")
    c4.metric("Avg Trip Distance", f"{m['avg_distance']} mi")
    c5.metric("Avg Trip Duration", f"{m['avg_duration']} min")
    st.divider()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Top Zones",
        "Fare by Hour",
        "Distance",
        "Payments",
        "Weekly Patterns",
    ])

    with tab1:
        st.plotly_chart(chart_top_zones(top_zones_df), width='stretch')
        st.info(
            "**Insight:**  Midtown Manhattan zones such as Upper East Side and Midtown Center dominate pickup" 
            " activity, whereas zones like Upper West Side South and Lincoln Square East have the least pickups. " 
            "This confirms that business districts and major transit hubs are the primary demand generators for " 
            "yellow taxis."
        )

    with tab2:
        st.plotly_chart(chart_fare_by_hour(fare_by_hour), width='stretch')
        st.info(
            "**Insight:** Fares are highest in the early morning hours (4–6 AM), likely because of the early morning "
            "rush to get to work. The midday dip (11 AM–2 PM) corresponds to shorter cross-town trips during the lunch "
            "window. Evening fares rise again as commuters head home."
        )

    with tab3:
        st.plotly_chart(chart_distance_hist(dist), width='stretch')
        st.info(
            "**Insight:** The distribution is strongly right-skewed; the vast majority of NYC yellow cab trips are under 5 " 
            "miles, reflecting short intra-Manhattan travel. There is a long tail of longer trips ranging from 6 miles to 20 " 
            "miles (likely JFK/LaGuardia airport runs) which pull the mean well above the median. This means the mean fare " 
            "overstates the usual trip cost experienced by most riders. This skew is typical of urban taxi datasets worldwide." 
        )

    with tab4:
        st.plotly_chart(chart_payment(payment), width='stretch')
        st.info(
            "**Insight:** Credit card is by far the dominant payment method, reflecting the near-universal adoption of card " 
            "terminals in NYC taxis. Cash still accounts for a notable minority, suggesting that a segment of riders, " 
            "potentially tourists or older passengers prefer or require cash payment."
        )

    with tab5:
        st.plotly_chart(chart_heatmap(dow_hour), width='stretch')
        st.info(
            "**Insight:** The heatmap reveals two distinct demand patterns. Weekdays show a classic bimodal commuter " 
            "pattern with peaks during morning (7–9 AM) and evening (5–8 PM) rush hours. Weekends (particularly Friday"
            " and Saturday nights) show a very different pattern: demand is concentrated in late evening (10 PM–2 AM), " 
            "corresponding to entertainment and nightlife travel. The early morning hours (3–5 AM) are consistently the" 
            " quietest period across all days." 
        )

    st.divider()
    st.caption(
        "COMP 3610 – made by Fariba Bhaggan"
    )


if __name__ == "__main__":
    main()