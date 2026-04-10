import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import warnings

warnings.filterwarnings("ignore")

# ---------------- PAGE CONFIG ----------------
st.set_page_config(page_title="Stock Dashboard", layout="wide")

# ---------------- DATABASE CONNECTION ----------------
conn = psycopg2.connect(
    host="localhost",
    database="stock_db",
    user="postgres",
    password="6671"
)

query = "SELECT * FROM stock_data;"
df = pd.read_sql(query, conn)

# ---------------- DATA CLEANING ----------------
df.columns = df.columns.str.lower()
df['date'] = pd.to_datetime(df['date'])

# ---------------- TITLE ----------------
st.title("📊 Stock Market Dashboard")

# ---------------- SIDEBAR FILTER ----------------
st.sidebar.header("Filter Stocks")

selected_stocks = st.sidebar.multiselect(
    "Select Stocks",
    options=df['stock'].unique(),
    default=df['stock'].unique()
)

# DATE FILTER
st.sidebar.subheader("Filter by Date")
start_date, end_date = st.sidebar.date_input(
    "Select Date Range",
    [df['date'].min(), df['date'].max()]
)

filtered_df = df[
    (df['stock'].isin(selected_stocks)) &
    (df['date'] >= pd.to_datetime(start_date)) &
    (df['date'] <= pd.to_datetime(end_date))
]

# ---------------- KPI SECTION ----------------
st.subheader("📌 Key Metrics")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Stocks", filtered_df['stock'].nunique())

with col2:
    st.metric("Average Price", round(filtered_df['close'].mean(), 2))

with col3:
    st.metric("Max Price", round(filtered_df['close'].max(), 2))

# ---------------- TOP GAINER / LOSER ----------------
st.subheader("📊 Market Insights")

latest_df = filtered_df.sort_values('date').groupby('stock').tail(1)

if not latest_df.empty:
    top_gainer = latest_df.sort_values('close', ascending=False).iloc[0]
    top_loser = latest_df.sort_values('close', ascending=True).iloc[0]

    col4, col5 = st.columns(2)

    with col4:
        st.success(f"🚀 Top Gainer: {top_gainer['stock']} ({round(top_gainer['close'],2)})")

    with col5:
        st.error(f"📉 Top Loser: {top_loser['stock']} ({round(top_loser['close'],2)})")

# ---------------- DATA PREVIEW ----------------
st.subheader("📄 Data Preview")
st.dataframe(filtered_df.tail(20))

# ---------------- DOWNLOAD BUTTON ----------------
st.subheader("⬇️ Download Data")

csv = filtered_df.to_csv(index=False).encode('utf-8')

st.download_button(
    label="Download Filtered Data",
    data=csv,
    file_name="stock_data.csv",
    mime="text/csv"
)

# ---------------- LINE CHART ----------------
st.subheader("📈 Stock Price Trend")

if not filtered_df.empty:
    fig = px.line(
        filtered_df,
        x='date',
        y='close',
        color='stock',
        title="Stock Price Comparison"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data available for selected filters")

# ---------------- COMPANY INFO ----------------
st.subheader("🏢 Company Info")

if all(col in filtered_df.columns for col in ['stock', 'company', 'sector']):
    company_df = filtered_df[['stock', 'company', 'sector']].drop_duplicates()
    st.dataframe(company_df)
else:
    st.warning("Company info not available")

# ---------------- CLOSE CONNECTION ----------------
conn.close()