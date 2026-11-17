# Cambridge Crime Data Engineering Pipeline

An end-to-end Data Engineering project that extracts, validates, transforms, and stores Cambridge Police Department incident data using Apache Airflow, Python, PostgreSQL, and Docker. The project also provides interactive crime-trend analytics through a Streamlit dashboard and Metabase.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Objectives](#objectives)
- [System Architecture](#system-architecture)
- [Technology Stack](#technology-stack)
- [Data Source](#data-source)
- [ETL Pipeline](#etl-pipeline)
- [Database Schema](#database-schema)
- [Dashboard and Analytics](#dashboard-and-analytics)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation and Setup](#installation-and-setup)
- [Running the Pipeline](#running-the-pipeline)
- [Accessing the Applications](#accessing-the-applications)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)
- [Future Improvements](#future-improvements)
- [Author](#author)

---

## Project Overview

The Cambridge Crime Data Engineering Pipeline is an automated ETL (Extract, Transform, Load) system designed to process publicly available police incident records from Cambridge, Massachusetts.

The project demonstrates how raw data from an external public API can be converted into structured, validated, and analytics-ready information through a containerized data engineering architecture.

The pipeline uses Apache Airflow to orchestrate the ETL workflow, Python and Pandas for data processing, and PostgreSQL for persistent storage.

The processed data can subsequently be explored through an interactive Streamlit dashboard and Metabase.

### Key Features

- Automated data extraction from the Socrata Open Data API.
- Data validation and standardization using Pandas.
- Duplicate removal and data transformation.
- Automated date and time feature extraction.
- PostgreSQL-based structured data storage.
- Apache Airflow DAG-based workflow orchestration.
- Docker-based deployment of project services.
- Interactive crime-trend visualization.
- Metabase integration for additional analytical exploration.
- Fallback sample data when API extraction fails.

---

## Objectives

The primary objectives of this project are:

1. Build a complete ETL pipeline using real-world public datasets.
2. Automate data workflows using Apache Airflow.
3. Implement data validation and cleaning techniques.
4. Design a structured relational database using PostgreSQL.
5. Containerize services using Docker Compose.
6. Generate meaningful analytical insights from historical crime records.
7. Develop an interactive dashboard for exploring crime patterns.

---

## System Architecture

The project follows a modular ETL architecture.

```text
       Socrata Open Data API
       Cambridge Police Data
                 |
                 v
        +------------------+
        |  DATA EXTRACTION |
        |      Python      |
        +------------------+
                 |
                 v
        +------------------+
        |  DATA VALIDATION |
        |     Pandas       |
        +------------------+
                 |
                 v
        +------------------+
        | DATA TRANSFORM   |
        | Cleaning and     |
        | Feature Creation |
        +------------------+
                 |
                 v
        +------------------+
        |   DATA LOADING   |
        |    PostgreSQL    |
        +------------------+
                 |
                 v
        +------------------+
        | ANALYTICS LAYER  |
        +------------------+
              /     \
             v       v
        Streamlit  Metabase
        Dashboard  Analytics
```

Apache Airflow orchestrates the extraction, validation, transformation, and loading tasks.

Docker Compose manages the application services and their networking.

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core programming language |
| Pandas | Data manipulation and transformation |
| Apache Airflow 2.9.3 | ETL orchestration |
| PostgreSQL 15 | Relational database |
| SQLAlchemy | Database connectivity and SQL operations |
| Socrata API / sodapy | External data extraction |
| Docker | Application containerization |
| Docker Compose | Multi-container orchestration |
| Streamlit | Interactive analytics dashboard |
| Metabase | Business intelligence and visualization |
| SQLite | Additional database artifact included in the repository |
| Jupyter Notebook | Data engineering experimentation and implementation |

---

## Data Source

The project retrieves police incident records from the official Cambridge Open Data platform.

**Platform:** Cambridge Open Data

**API:** Socrata Open Data API

**Dataset ID:** `3gki-wyrb`

**Domain:** `data.cambridgema.gov`

The extraction process uses the `sodapy` Python library to communicate with the API.

The current extraction implementation requests up to 2,500 records.

The extracted data includes fields such as:

- Incident date and time
- Incident ID
- Incident type
- Incident location
- Incident description

If API extraction fails, the current implementation uses a small predefined sample dataset to allow the pipeline to continue testing its downstream processing stages.

Note: Fallback records are sample data and should not be interpreted as verified live police records.

---

## ETL Pipeline

The ETL workflow is implemented as an Apache Airflow DAG named:

`cpd_crime_trends_etl`

The DAG consists of four sequential tasks.

### 1. Data Extraction

**Task ID:** `extract_cpd_data`

The extraction module:

- Connects to the Cambridge Socrata Open Data API.
- Retrieves police incident records.
- Converts API responses into a Pandas DataFrame.
- Saves the extracted records into a raw CSV file.

Output:

`/tmp/cpd_raw.csv`

### 2. Data Validation

**Task ID:** `validate_cpd_data`

The validation module ensures that incoming records follow a consistent structure.

Operations include:

- Standardizing column names.
- Mapping alternative field names to expected columns.
- Checking the availability of required fields.
- Removing records with missing critical information.
- Normalizing incident IDs.
- Converting incident IDs into numeric values.
- Parsing timestamps into valid datetime objects.
- Removing records containing invalid timestamps.

Expected columns:

- `date_time`
- `id`
- `type`
- `location`
- `description`

Output:

`/tmp/cpd_valid.csv`

### 3. Data Transformation

**Task ID:** `transform_cpd_data`

The transformation module converts validated records into an analytics-ready format.

Operations include:

- Removing duplicate incidents using incident IDs.
- Converting incident IDs into integer format.
- Handling missing descriptions.
- Extracting year from timestamps.
- Extracting month from timestamps.
- Extracting day from timestamps.
- Extracting hour from timestamps.
- Extracting the day of the week.

Additional analytical columns:

| Column | Description |
|---|---|
| year | Year of the incident |
| month | Month of the incident |
| day | Day of the month |
| hour | Hour of occurrence |
| day_of_week | Name of the weekday |

Output:

`/tmp/cpd_clean.csv`

### 4. Data Loading

**Task ID:** `load_to_postgres`

The final stage loads the transformed records into PostgreSQL.

The pipeline:

- Establishes a connection using SQLAlchemy.
- Creates the `cpd_incidents` table if necessary.
- Loads the transformed dataset into PostgreSQL.
- Stores incident information and derived analytical attributes.

Target table:

`cpd_incidents`

The DAG executes these tasks sequentially:

```python
extract_cpd_data
        >>
validate_cpd_data
        >>
transform_cpd_data
        >>
load_to_postgres
```

The configured schedule is daily (`@daily`).

The DAG also uses three retries with a 10-second retry delay.

---

## Database Schema

The PostgreSQL database is named:

`cpd_db`

The primary analytical table is:

`cpd_incidents`

| Column | Data Type | Description |
|---|---|---|
| id | BIGINT | Unique incident identifier |
| date_time | TIMESTAMP | Incident date and time |
| type | TEXT | Incident classification |
| location | TEXT | Incident location |
| description | TEXT | Incident description |
| year | INTEGER | Incident year |
| month | INTEGER | Incident month |
| day | INTEGER | Incident day |
| hour | INTEGER | Incident hour |
| day_of_week | TEXT | Weekday name |

The incident ID acts as the primary key in the table definition.

The database is persisted using a Docker volume to retain data between container restarts.

---

## Dashboard and Analytics

The project includes a Streamlit dashboard that retrieves processed incident data from PostgreSQL.

### Dashboard Features

**1. Year Selection**

Users can select a year to explore incidents from a particular period.

**2. Summary Statistics**

The dashboard displays:

- Total number of incidents.
- Number of distinct incident types.
- Number of unique incident locations.

**3. Top 10 Incident Types**

A bar chart displays the ten most frequently occurring incident categories.

**4. Incidents by Hour**

A line chart illustrates the distribution of incidents throughout the day.

**5. Incidents by Month**

A bar chart displays monthly incident frequencies.

**6. Recent Police Log Entries**

A tabular view provides access to up to 100 incident records for the selected year.

### Metabase

Metabase is also included in the Docker Compose configuration to provide an additional business intelligence interface.

It can connect to the PostgreSQL database and be used to build custom queries, charts, and analytical dashboards.

---

## Project Structure

```text
DE/
│
├── DE_PIPELINE_2.ipynb
│
├── setup_project.py
│
├── Dockerfile.airflow
│
├── docker-compose.yml
│
├── dashboard.py
│
├── cpd_db.db
│
├── .env
│
├── .gitattributes
│
└── dags/
    │
    └── cpd_crime_dag.py
```

**File descriptions:**

| File | Description |
|---|---|
| `DE_PIPELINE_2.ipynb` | Jupyter notebook containing the project's notebook-based work |
| `setup_project.py` | Generates the Airflow DAG, Docker configuration, and environment file |
| `Dockerfile.airflow` | Defines the custom Airflow image and Python dependencies |
| `docker-compose.yml` | Configures PostgreSQL, Airflow, and Metabase |
| `dashboard.py` | Implements the Streamlit analytics dashboard |
| `cpd_db.db` | SQLite database artifact |
| `.env` | Stores environment configuration |
| `dags/cpd_crime_dag.py` | Defines the automated ETL workflow |

Note: The `dags/` directory and its DAG file are generated by `setup_project.py`. They may not exist in a fresh checkout until the setup script is executed.

---

## Prerequisites

Before running the project, install:

- Python 3.11
- Docker Desktop or Docker Engine
- Docker Compose
- Git

Recommended minimum system configuration:

- 8 GB RAM
- 10 GB available storage
- Internet connectivity for API access and Docker image downloads

---

## Installation and Setup

### Step 1: Clone the Repository

```bash
git clone https://github.com/prakhart056-student/DE.git

cd DE
```

### Step 2: Generate Project Configuration

Execute:

```bash
python setup_project.py
```

This script generates:

- The `dags/` directory.
- The Airflow DAG.
- The `.env` configuration file.
- The Airflow Dockerfile.
- The Docker Compose configuration.

Important: The setup script rewrites these generated configuration files. Run it before making custom changes to them.

### Step 3: Configure Environment Variables

Open `.env` and configure the Socrata API token if available.

```env
SOCRATA_APP_TOKEN=your_socrata_app_token

POSTGRES_USER=cpd_user
POSTGRES_PASSWORD=cpd_pass
POSTGRES_DB=cpd_db
```

The API token is optional in the current implementation.

For production use, replace the default database credentials with secure values.

### Step 4: Build and Start Docker Services

```bash
docker compose up --build -d
```

This starts:

- PostgreSQL database.
- Apache Airflow.
- Metabase.

Check the running containers:

```bash
docker compose ps
```

### Step 5: Verify Container Logs

```bash
docker compose logs -f
```

To inspect an individual service:

```bash
docker compose logs airflow

docker compose logs postgres_db

docker compose logs metabase
```

---

## Running the Pipeline

### Step 1: Open Airflow

Navigate to:

`http://localhost:8080`

The current Docker configuration creates the following default login:

```text
Username: admin
Password: admin
```

These credentials are intended for local development only.

### Step 2: Locate the DAG

Find:

`cpd_crime_trends_etl`

### Step 3: Trigger the Workflow

Enable the DAG if necessary and trigger it manually using the Airflow interface.

### Step 4: Monitor Execution

Monitor the four tasks:

1. `extract_cpd_data`
2. `validate_cpd_data`
3. `transform_cpd_data`
4. `load_to_postgres`

A successful run should complete all four tasks.

### Step 5: Verify Database Records

Connect to PostgreSQL:

```bash
docker compose exec postgres_db \
psql -U cpd_user -d cpd_db
```

Execute:

```sql
SELECT *
FROM cpd_incidents
LIMIT 10;
```

Check total records:

```sql
SELECT COUNT(*)
FROM cpd_incidents;
```

Explore incident categories:

```sql
SELECT type, COUNT(*) AS total
FROM cpd_incidents
GROUP BY type
ORDER BY total DESC;
```

---

## Accessing the Applications

Once the Docker services are running:

| Application | URL |
|---|---|
| Apache Airflow | http://20.192.9.103:8080/home |
| Metabase | http://20.192.9.103:3000/auto/dashboard/table/20?date_time=&day_of_week=&type= |
| PostgreSQL | localhost:5432 |
| Streamlit Dashboard | https://cpd-dash-81406745.azurewebsites.net/ |

### Starting the Streamlit Dashboard

The dashboard is a separate Python application.

Install its dependencies:

```bash
pip install pandas streamlit sqlalchemy psycopg2-binary python-dotenv
```

Ensure that `.env` contains the correct PostgreSQL connection settings.

Then execute:

```bash
streamlit run dashboard.py
```

Open:

`http://localhost:8501`

The dashboard expects the PostgreSQL `cpd_incidents` table to contain processed records.

### Configuring Metabase

Open:

`http://localhost:3000`

Complete the initial Metabase setup and add a PostgreSQL data source.

Use the following connection details:

```text
Host: postgres_db
Port: 5432
Database: cpd_db
Username: cpd_user
Password: cpd_pass
```

When connecting from Metabase, use `postgres_db` as the hostname because both services operate within the Docker network.

---

## Environment Variables

| Variable | Purpose |
|---|---|
| SOCRATA_APP_TOKEN | Optional Socrata API authentication token |
| POSTGRES_USER | PostgreSQL username |
| POSTGRES_PASSWORD | PostgreSQL password |
| POSTGRES_DB | PostgreSQL database name |
| POSTGRES_HOST | PostgreSQL host used by the dashboard |
| POSTGRES_PORT | PostgreSQL port |
| DATABASE_URL | Optional complete SQLAlchemy database URL |

The Airflow service uses its configured PostgreSQL connection string through Docker Compose.

---

## Troubleshooting

### 1. Airflow DAG Is Not Visible

Check whether the DAG file exists:

```bash
ls dags/
```

If it does not exist, execute:

```bash
python setup_project.py
```

Restart the services:

```bash
docker compose restart airflow
```

### 2. PostgreSQL Connection Failed

Check the database container:

```bash
docker compose ps
```

Inspect database logs:

```bash
docker compose logs postgres_db
```

Verify the database credentials and connection hostname.

### 3. No Records Available in Dashboard

The dashboard requires the `cpd_incidents` table to contain records.

Trigger the Airflow DAG and verify that the loading task completes successfully.

### 4. API Extraction Failed

Check internet connectivity and the Socrata API configuration.

The current pipeline contains fallback sample records for extraction failures.

### 5. Rebuild the Containers

```bash
docker compose down

docker compose up --build -d
```

To remove containers and persistent volumes:

```bash
docker compose down -v
```

Warning: Removing volumes permanently deletes the persisted PostgreSQL and Metabase data.

---

## Future Improvements

Potential enhancements to make the pipeline more scalable and production-ready include:

- Implement incremental data extraction instead of repeatedly retrieving a fixed number of records.
- Introduce proper logging and monitoring.
- Add automated data quality checks.
- Implement structured error handling and failure notifications.
- Add database indexing for faster analytical queries.
- Separate Airflow metadata storage from the analytical PostgreSQL database.
- Introduce CI/CD automation using GitHub Actions.
- Add historical crime trend comparisons.
- Implement geospatial analysis of incident locations.
- Introduce advanced analytical visualizations.
- Add automated testing for ETL tasks.
- Improve security by using Docker secrets or dedicated secret management.

---

## Author

**Prakhar Tiwari**

GitHub: [@prakhart056-student](https://github.com/prakhart056-student)

Project Repository: [Cambridge Crime Data Engineering Pipeline](https://github.com/prakhart056-student/DE)

---

## License

This project is intended for educational and demonstration purposes.

A formal open-source license can be added to the repository to specify usage, modification, and distribution permissions.
