import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

st.title("Cambridge Crime Trends Dashboard")
st.write("ETL Pipeline Dashboard - Cambridge Police Department Daily Log")

load_dotenv()
database_url = os.getenv("DATABASE_URL")
if database_url:
    engine = create_engine(database_url)
else:
    engine = create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=os.getenv("POSTGRES_USER", "cpd_user"),
            password=os.getenv("POSTGRES_PASSWORD", "cpd_pass"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cpd_db"),
        )
    )

try:
    with engine.connect() as connection:
        df = pd.read_sql(text("SELECT * FROM cpd_incidents"), con=connection)
except SQLAlchemyError as exc:
    st.error(
        "Could not read cpd_incidents from PostgreSQL. Start the Docker Compose "
        "services, run the Airflow DAG, and check DATABASE_URL/.env settings. "
        f"Database error: {exc}"
    )
    st.stop()

if df.empty:
    st.info("No incident rows are available yet. Trigger the Airflow DAG and reload.")
    st.stop()

# simple filter by year
years = sorted(df["year"].dropna().astype(int).unique().tolist())
if not years:
    st.info("No incidents with a valid year are available yet.")
    st.stop()

selected_year = st.selectbox("Select Year", years, index=len(years)-1)
filtered_df = df[df["year"] == selected_year]

# summary counts
st.subheader("Summary")
col1, col2, col3 = st.columns(3)
col1.metric("Total Incidents", len(filtered_df))
col2.metric("Crime Types", filtered_df["type"].nunique())
col3.metric("Unique Streets", filtered_df["location"].nunique())

# chart 1: top 10 crime types
st.subheader("Top 10 Incident Types")
top_types = filtered_df["type"].value_counts().head(10)
st.bar_chart(top_types)

# chart 2: incidents by hour of day
st.subheader("Incidents by Hour of Day")
hourly_counts = filtered_df["hour"].value_counts().sort_index()
st.line_chart(hourly_counts)

# chart 3: incidents by month
st.subheader("Incidents by Month")
monthly_counts = filtered_df["month"].value_counts().sort_index()
st.bar_chart(monthly_counts)

# show table data
st.subheader("Recent Police Log Entries")
st.dataframe(filtered_df.head(100))
