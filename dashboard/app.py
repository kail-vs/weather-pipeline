import os
import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "weatherdb"),
    "user": os.getenv("POSTGRES_USER", "weatheruser"),
    "password": os.getenv("POSTGRES_PASSWORD", "weatherpass"),
}

REFRESH_INTERVAL = 5 

def fetch_data():
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT batch_time, city, avg_temp, avg_humidity, reading_count
        FROM weather_agg
        ORDER BY batch_time DESC
        LIMIT 500
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.set_page_config(
    page_title="Real-Time Weather Dashboard",
    # layout="wide",
    initial_sidebar_state="expanded"
)

try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=REFRESH_INTERVAL * 1000, key="refresh_key")
except ImportError:
    st.info(f"Auto-refresh disabled (install `streamlit-autorefresh` to enable)")

st.title("Real-Time Weather Dashboard")
st.caption(f"Updates every {REFRESH_INTERVAL} seconds | Data from `weather_agg` table")

df = fetch_data()

if df.empty:
    st.warning("No data available yet. Waiting for stream updates...")
else:
    cities = ["All"] + sorted(df["city"].unique().tolist())
    selected_city = st.sidebar.selectbox("🏙 Select City", cities)

    if selected_city != "All":
        df = df[df["city"] == selected_city]

    latest_batch_time = df["batch_time"].max()
    latest_df = df[df["batch_time"] == latest_batch_time]

    st.subheader(f"Latest Data (Batch Time: {latest_batch_time})")
    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Temperature", f"{latest_df['avg_temp'].mean():.2f} °C")
    c2.metric("Avg Humidity", f"{latest_df['avg_humidity'].mean():.2f} %")
    c3.metric("Total Readings", int(latest_df['reading_count'].sum()))

    st.markdown("### Temperature Trends")
    fig_temp = px.line(
        df,
        x="batch_time",
        y="avg_temp",
        color="city",
        markers=True,
        title="Average Temperature Over Time",
        labels={"avg_temp": "Avg Temperature (°C)", "batch_time": "Time"},
    )
    fig_temp.update_layout(legend_title_text="City", height=600, width=800)
    st.plotly_chart(fig_temp, use_container_width=True)

    st.markdown("### Humidity Trends")
    fig_hum = px.line(
        df,
        x="batch_time",
        y="avg_humidity",
        color="city",
        markers=True,
        title="Average Humidity Over Time",
        labels={"avg_humidity": "Avg Humidity (%)", "batch_time": "Time"},
    )
    fig_hum.update_layout(legend_title_text="City", height=600, width=800)
    st.plotly_chart(fig_hum, use_container_width=True)

    st.markdown("### Reading Counts")
    fig_counts = px.bar(
        latest_df,
        x="city",
        y="reading_count",
        color="city",
        title="Number of Readings in Latest Batch",
        labels={"reading_count": "Reading Count", "city": "City"},
    )
    fig_counts.update_layout(showlegend=False, height=600, width=800)
    st.plotly_chart(fig_counts, use_container_width=True)

    with st.expander("Show Raw Data"):
        st.dataframe(df.sort_values(by="batch_time", ascending=False), use_container_width=True)
