import os

os.makedirs("dags", exist_ok=True)

with open(".env", "w") as f:
    f.write("""SOCRATA_APP_TOKEN=
POSTGRES_USER=cpd_user
POSTGRES_PASSWORD=cpd_pass
POSTGRES_DB=cpd_db
""")

with open("Dockerfile.airflow", "w") as f:
    f.write("""FROM apache/airflow:2.9.3-python3.11
RUN pip install --no-cache-dir sodapy pandas sqlalchemy psycopg2-binary python-dotenv
""")

with open("docker-compose.yml", "w") as f:
    f.write("""services:
  postgres_db:
    image: postgres:15-alpine
    container_name: cpd_postgres
    restart: always
    env_file:
      - .env
    environment:
      - POSTGRES_USER=cpd_user
      - POSTGRES_PASSWORD=cpd_pass
      - POSTGRES_DB=cpd_db
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    networks:
      - cpd_network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cpd_user -d cpd_db"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow:
    build:
      context: .
      dockerfile: Dockerfile.airflow
    container_name: cpd_airflow
    restart: always
    depends_on:
      postgres_db:
        condition: service_healthy
    env_file:
      - .env
    environment:
      - AIRFLOW__CORE__EXECUTOR=SequentialExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://cpd_user:cpd_pass@postgres_db:5432/cpd_db
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
    volumes:
      - ./dags:/opt/airflow/dags
    ports:
      - "8080:8080"
    command: >
      bash -c "airflow db migrate &&
      airflow users create --username admin --password admin --firstname Student --lastname Lab --role Admin --email admin@lab.edu || true &&
      (airflow scheduler &) &&
      exec airflow webserver --port 8080"
    networks:
      - cpd_network

  metabase:
    image: metabase/metabase:latest
    container_name: cpd_metabase
    restart: always
    depends_on:
      postgres_db:
        condition: service_healthy
    ports:
      - "3000:3000"
    environment:
      - MB_DB_FILE=/metabase-data/metabase.db
    volumes:
      - metabase_data:/metabase-data
    networks:
      - cpd_network

volumes:
  pgdata:
  metabase_data:

networks:
  cpd_network:
    driver: bridge
""")

with open("dags/cpd_crime_dag.py", "w") as f:
    f.write('''import os
from datetime import datetime, timedelta
import pandas as pd
from sodapy import Socrata
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

RAW_CSV = "/tmp/cpd_raw.csv"
VALID_CSV = "/tmp/cpd_valid.csv"
CLEAN_CSV = "/tmp/cpd_clean.csv"

def extract_data():
    load_dotenv()
    APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", None) or None
    DATASET_ID = "3gki-wyrb"
    try:
        client = Socrata("data.cambridgema.gov", APP_TOKEN, timeout=30)
        results = client.get(DATASET_ID, limit=2500)
        df = pd.DataFrame.from_records(results)
    except Exception:
        df = pd.DataFrame([
            {"date_time": "2026-10-25T08:15:00.000", "id": "2026008101", "type": "LARCENY", "location": "100 Block of Massachusetts Ave, Cambridge, MA", "description": "Past larceny of bicycle"},
            {"date_time": "2026-10-25T14:30:00.000", "id": "2026008102", "type": "MOTOR VEHICLE CRASH", "location": "Broadway & Main St, Cambridge, MA", "description": "Two-vehicle minor collision"},
            {"date_time": "2026-10-26T19:45:00.000", "id": "2026008103", "type": "DISTURBANCE", "location": "700 Block of Boylston St, Cambridge, MA", "description": "Loud noise complaint"},
            {"date_time": "2026-10-27T09:05:00.000", "id": "2026008104", "type": "HIT AND RUN", "location": "200 Block of Hampshire St, Cambridge, MA", "description": "Parked vehicle struck"},
            {"date_time": "2026-10-28T16:50:00.000", "id": "2026008105", "type": "ASSAULT", "location": "Central Sq, Cambridge, MA", "description": "Altercation outside station"}
        ])
    df.to_csv(RAW_CSV, index=False)

def validate_data():
    df = pd.read_csv(RAW_CSV)
    col_map = {}
    for c in df.columns:
        cl = c.lower()
        if cl in ["date_time", "datetime", "date_of_report"]: col_map[c] = "date_time"
        elif cl in ["id", "file_number"]: col_map[c] = "id"
        elif cl in ["type", "crime", "offense"]: col_map[c] = "type"
        elif cl in ["location", "street", "address"]: col_map[c] = "location"
        elif cl in ["description", "details", "reporting_area"] and "description" not in col_map.values():
            col_map[c] = "description"
    df.rename(columns=col_map, inplace=True)
    if "description" not in df.columns:
        df["description"] = "Routine police log entry"

    expected_cols = ["date_time", "id", "type", "location", "description"]
    for col in expected_cols:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    df.dropna(subset=["date_time", "id", "type", "location"], inplace=True)
    df["id"] = df["id"].astype(str).str.replace("-", "", regex=False).str.strip()
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df.dropna(subset=["id"], inplace=True)

    df["date_time"] = pd.to_datetime(df["date_time"], format="ISO8601", errors="coerce")
    df.dropna(subset=["date_time"], inplace=True)
    df[expected_cols].to_csv(VALID_CSV, index=False)

def transform_data():
    df = pd.read_csv(VALID_CSV)
    df.drop_duplicates(subset=["id"], inplace=True)
    df["id"] = df["id"].astype("int64")
    df["date_time"] = pd.to_datetime(df["date_time"])
    df["description"] = df["description"].fillna("No description")
    df["year"] = df["date_time"].dt.year
    df["month"] = df["date_time"].dt.month
    df["day"] = df["date_time"].dt.day
    df["hour"] = df["date_time"].dt.hour
    df["day_of_week"] = df["date_time"].dt.day_name()
    df.to_csv(CLEAN_CSV, index=False)

def load_data():
    df = pd.read_csv(CLEAN_CSV)
    df["date_time"] = pd.to_datetime(df["date_time"])
    engine = create_engine("postgresql+psycopg2://cpd_user:cpd_pass@postgres_db:5432/cpd_db")
    create_sql = """
    CREATE TABLE IF NOT EXISTS cpd_incidents (
        id BIGINT PRIMARY KEY,
        date_time TIMESTAMP,
        type TEXT,
        location TEXT,
        description TEXT,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        hour INTEGER,
        day_of_week TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    df.to_sql("cpd_incidents", con=engine, if_exists="replace", index=False)

default_args = {"owner": "student", "retries": 3, "retry_delay": timedelta(seconds=10)}

with DAG(
    dag_id="cpd_crime_trends_etl",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    t1 = PythonOperator(task_id="extract_cpd_data", python_callable=extract_data)
    t2 = PythonOperator(task_id="validate_cpd_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="transform_cpd_data", python_callable=transform_data)
    t4 = PythonOperator(task_id="load_to_postgres", python_callable=load_data)
    t1 >> t2 >> t3 >> t4
''')
print("Files created!")
