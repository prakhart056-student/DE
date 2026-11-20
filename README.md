# Cambridge Crime Data Engineering Pipeline

An end-to-end Data Engineering project that extracts, validates, transforms, and stores Cambridge Police Department incident data using Apache Airflow, Python, PostgreSQL, and Docker. The project also provides interactive crime-trend analytics through a Streamlit dashboard and Metabase, and is deployed on Microsoft Azure.

---

## Table of Contents

- [Live Demo](#live-demo)
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
- [Cloud Deployment on Azure](#cloud-deployment-on-azure)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)
- [Future Improvements](#future-improvements)
- [Author](#author)

---

## Live Demo

The project is deployed on Microsoft Azure.

| Service | URL | Access |
|---|---|---|
| Streamlit Dashboard | https://cpd-dash-81406745.azurewebsites.net/ | Public, read-only |
| Apache Airflow | http://20.192.9.103:8080 | Login required |
| Metabase | http://20.192.9.103:3000 | Login required |

The dashboard reads live data from the same PostgreSQL database that Airflow loads and Metabase queries, so it shows the result of the most recent pipeline run.

> The hosted demo runs on Azure student credits and may be switched off after evaluation. Credentials for Airflow and Metabase are not published. To run everything yourself, see [Installation and Setup](#installation-and-setup) (local) or [Cloud Deployment on Azure](#cloud-deployment-on-azure).

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
- Cloud deployment on Azure: the Docker Compose stack on a virtual machine and the Streamlit dashboard on App Service, reading live data through a read-only database user.

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
8. Deploy the system to the cloud and expose it through public URLs.

---

## System Architecture

The project follows a modular ETL architecture.

<img width="1024" height="572" alt="image" src="https://github.com/user-attachments/assets/65dc649e-61d0-4459-b021-cc9527479e73" />

Apache Airflow orchestrates the extraction, validation, transformation, and loading tasks.

Docker Compose manages the application services and their networking.

The Azure deployment of this architecture is described in [Cloud Deployment on Azure](#cloud-deployment-on-azure).

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
| Microsoft Azure | Cloud hosting (Virtual Machine, App Service, Network Security Groups) |
| Azure CLI | Provisioning and configuring the Azure resources |
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

Note: Fallback records are sample data and should not be interpreted as verified live police records. If the dashboard suddenly shows only five incidents, the last pipeline run probably used the fallback data; check the `extract_cpd_data` task log in Airflow.

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

Each run replaces the contents of `cpd_incidents` (full refresh), so the table, and therefore the dashboard and Metabase, always reflects the most recent successful run.

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

The DAG's `CREATE TABLE` statement declares the incident ID as the primary key. Note that the load step then replaces the table with pandas' `to_sql(..., if_exists="replace")`, which recreates it with pandas-inferred column types and without the primary key constraint. Using an upsert instead is listed under [Future Improvements](#future-improvements).

The database is persisted using a Docker volume to retain data between container restarts.

The same database also holds Airflow's own metadata tables, which is why the dashboard's read-only database user (see [Cloud Deployment on Azure](#cloud-deployment-on-azure)) is granted access to `cpd_incidents` only.

The repository also contains `cpd_db.db`, a SQLite snapshot of incident data (12,344 rows covering 2025 and 2026). The Docker pipeline itself does not use it.

---

## Dashboard and Analytics

The project includes a Streamlit dashboard that retrieves processed incident data from PostgreSQL. It queries the `cpd_incidents` table on every page load and interaction, so it always shows the data from the most recent pipeline run.

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
├── requirements.txt
│
├── cpd_db.db
│
├── .env
│
├── .gitattributes
│
├── README.md
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
| `requirements.txt` | Python dependencies of the dashboard (used locally and on Azure App Service) |
| `cpd_db.db` | SQLite snapshot of incident data |
| `.env` | Stores environment configuration |
| `dags/cpd_crime_dag.py` | Defines the automated ETL workflow |

Note: The `dags/` directory and its DAG file are generated by `setup_project.py`. They may not exist in a fresh checkout until the setup script is executed.

---

## Prerequisites

Before running the project locally, install:

- Python 3.11
- Docker Desktop or Docker Engine
- Docker Compose
- Git

Recommended minimum system configuration:

- 8 GB RAM
- 10 GB available storage
- Internet connectivity for API access and Docker image downloads

To deploy to Azure you additionally need an Azure subscription and the Azure CLI (or Azure Cloud Shell).

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

These credentials are intended for local development only. Change them before exposing Airflow on any server.

### Step 2: Locate the DAG

Find:

`cpd_crime_trends_etl`

### Step 3: Trigger the Workflow

The DAG is created unpaused. Trigger it manually using the Airflow interface to load data immediately instead of waiting for the daily schedule.

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

Check how recent the data is:

```sql
SELECT MAX(date_time) AS newest_incident
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

### Hosted demo

The URLs of the Azure deployment are listed in [Live Demo](#live-demo).

### Local (Docker Compose)

Once the Docker services are running:

| Application | URL |
|---|---|
| Apache Airflow | http://localhost:8080 |
| Metabase | http://localhost:3000 |
| PostgreSQL | localhost:5432 |
| Streamlit Dashboard | http://localhost:8501 |

### Starting the Streamlit Dashboard

The dashboard is a separate Python application.

Install its dependencies:

```bash
pip install -r requirements.txt
```

Ensure that `.env` contains the correct PostgreSQL connection settings.

Then execute:

```bash
streamlit run dashboard.py
```

Open:

`http://localhost:8501`

The dashboard expects the PostgreSQL `cpd_incidents` table to contain processed records.

To point the dashboard at a different database, set `DATABASE_URL` (see [Environment Variables](#environment-variables)).

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

Complete the Metabase setup wizard immediately after the first start; until an administrator account exists, anyone who can reach the page can create it.

---

## Cloud Deployment on Azure

The hosted demo uses two Azure resources:

| Component | Azure service | Runs |
|---|---|---|
| Data platform | Linux virtual machine | Docker Compose stack: PostgreSQL, Airflow, Metabase |
| Dashboard | App Service (Linux, Python 3.11) | Streamlit dashboard (`dashboard.py`) |

### Cloud Architecture

```text
 Cambridge Open Data (Socrata API)
             |
             v
 +--------------------------------------------------+
 | Azure VM (Ubuntu, Docker Compose)                |
 |                                                  |
 |   Airflow  :8080 --- ETL ---> PostgreSQL :5432   |
 |   Metabase :3000 ------------> PostgreSQL        |
 +--------------------------------------------------+
             ^
             |  PostgreSQL protocol, read-only user "dash_ro"
             |  (port 5432 allowed only from the App Service IPs)
             |
 +--------------------------------------+
 | Azure App Service                    | <--- HTTPS --- Browser
 |   Streamlit dashboard (dashboard.py) |
 +--------------------------------------+
```

### Step 1: Create the VM and start the stack

Create a VM with at least 4 GB of RAM (8 GB is more comfortable for Airflow plus Metabase) and open the Airflow and Metabase ports:

```bash
az group create -n <RESOURCE_GROUP> -l <REGION>

az vm create -g <RESOURCE_GROUP> -n cpd-vm --image Ubuntu2204 --size Standard_B2ms \
  --admin-username azureuser --generate-ssh-keys --public-ip-sku Standard

az vm open-port -g <RESOURCE_GROUP> -n cpd-vm --port 8080 --priority 1010
az vm open-port -g <RESOURCE_GROUP> -n cpd-vm --port 3000 --priority 1020
```

Connect to the VM with SSH and start the stack:

```bash
curl -fsSL https://get.docker.com | sudo sh

git clone https://github.com/prakhart056-student/DE.git
cd DE
python3 setup_project.py

# Replace the default Airflow password (--password admin) in docker-compose.yml first.
sudo docker compose up -d --build
```

Open Airflow on port 8080 and trigger `cpd_crime_trends_etl` once so that `cpd_incidents` is populated.

### Step 2: Create a read-only database user for the dashboard

The dashboard never needs to write, so it gets its own login that can only read `cpd_incidents`:

```bash
sudo docker compose exec -T postgres_db psql -U cpd_user -d cpd_db <<'SQL'
CREATE ROLE dash_ro LOGIN PASSWORD '<LONG_RANDOM_PASSWORD>' CONNECTION LIMIT 10;
ALTER ROLE dash_ro SET default_transaction_read_only = on;
ALTER ROLE dash_ro SET statement_timeout = '30s';
GRANT CONNECT ON DATABASE cpd_db TO dash_ro;
GRANT USAGE ON SCHEMA public TO dash_ro;
GRANT SELECT ON TABLE public.cpd_incidents TO dash_ro;
-- The DAG recreates the table on every run, so also grant on future tables:
ALTER DEFAULT PRIVILEGES FOR ROLE cpd_user IN SCHEMA public GRANT SELECT ON TABLES TO dash_ro;
SQL
```

### Step 3: Deploy the dashboard to App Service

```bash
az appservice plan create -g <RESOURCE_GROUP> -n cpd-plan --is-linux --sku B1

az webapp create -g <RESOURCE_GROUP> -p cpd-plan -n <APP_NAME> --runtime "PYTHON:3.11"

az webapp config appsettings set -g <RESOURCE_GROUP> -n <APP_NAME> --settings \
  SCM_DO_BUILD_DURING_DEPLOYMENT=true \
  WEBSITES_PORT=8000 \
  "DATABASE_URL=postgresql+psycopg2://dash_ro:<LONG_RANDOM_PASSWORD>@<VM_PUBLIC_IP>:5432/cpd_db?connect_timeout=5"

az webapp config set -g <RESOURCE_GROUP> -n <APP_NAME> --web-sockets-enabled true --always-on true \
  --startup-file "python -m streamlit run dashboard.py --server.port=8000 --server.address=0.0.0.0 --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false"

python3 -m zipfile -c app.zip dashboard.py requirements.txt
az webapp deploy -g <RESOURCE_GROUP> -n <APP_NAME> --src-path app.zip --type zip --async true
```

The first build installs the packages from `requirements.txt` and can take several minutes. `--async true` keeps the deployment tool from failing with a gateway timeout while it waits.

### Step 4: Allow only the dashboard to reach PostgreSQL

Docker Compose publishes PostgreSQL on port 5432 of the VM. Do not open this port to the whole internet. Allow it only from the App Service's outbound IP addresses:

```bash
az webapp show -g <RESOURCE_GROUP> -n <APP_NAME> --query possibleOutboundIpAddresses -o tsv

az network nsg rule create -g <RESOURCE_GROUP> --nsg-name <VM_NSG_NAME> -n allow-pg-appservice \
  --priority 1100 --direction Inbound --access Allow --protocol Tcp \
  --destination-port-ranges 5432 --source-address-prefixes <IP_1> <IP_2> <IP_3> ...
```

Because the default `cpd_user` password is public in this README, also make PostgreSQL itself refuse every login except `dash_ro` from those addresses. Add two lines per App Service IP at the top of PostgreSQL's `pg_hba.conf` (find it with `SHOW hba_file;`), then run `SELECT pg_reload_conf();`:

```text
host cpd_db dash_ro <IP>/32 scram-sha-256
host all    all     <IP>/32 reject
```

Existing rules stay untouched, so Airflow and Metabase keep working.

### Step 5: Verify

- The Airflow DAG run is green and `SELECT COUNT(*), MAX(date_time) FROM cpd_incidents;` returns the expected numbers.
- The dashboard's *Total Incidents* for a year matches the same count in Metabase.
- Port 5432 does **not** answer from a machine outside the allow-list.

### Data freshness

The dashboard reads PostgreSQL on every interaction, so new data appears as soon as a DAG run finishes. The DAG runs daily; trigger it from the Airflow UI for an immediate refresh.

### Security notes

- Use strong, unique passwords for Airflow and the database users; never commit them to the repository.
- Airflow and Metabase are served over plain HTTP. For anything beyond a demo, place them behind a reverse proxy with HTTPS.
- Keep the dashboard on the read-only `dash_ro` user. It cannot modify data and cannot see Airflow's metadata tables.
- Complete the Metabase setup wizard immediately after the first start.

### Cost and cleanup

Both the VM and the App Service plan are billed while they exist. When the demo is over, remove everything in the resource groups you created:

```bash
az group delete -n <RESOURCE_GROUP> --yes
```

---

## Environment Variables

| Variable | Purpose |
|---|---|
| SOCRATA_APP_TOKEN | Optional Socrata API authentication token |
| POSTGRES_USER | PostgreSQL username |
| POSTGRES_PASSWORD | PostgreSQL password |
| POSTGRES_DB | PostgreSQL database name |
| POSTGRES_HOST | PostgreSQL host used by the dashboard (default `localhost`) |
| POSTGRES_PORT | PostgreSQL port |
| DATABASE_URL | Optional complete SQLAlchemy database URL. When set, it takes priority over the `POSTGRES_*` variables. The hosted dashboard uses it to reach the VM's database. |
| WEBSITES_PORT | Azure App Service only: the port the dashboard listens on (`8000`) |
| SCM_DO_BUILD_DURING_DEPLOYMENT | Azure App Service only: install `requirements.txt` while deploying |

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

### 6. Hosted Dashboard Shows "Could Not Read cpd_incidents"

The dashboard cannot reach the VM's database. Check, in this order:

1. The `DATABASE_URL` app setting (host, port, user, password).
2. The network security group rule for port 5432. App Service outbound IP addresses can change when the plan is scaled or moved; re-run `az webapp show ... --query possibleOutboundIpAddresses` and update the rule and the `pg_hba.conf` lines.
3. That the Airflow DAG has run at least once, so the table exists.
4. That the `psycopg2-binary` package is installed (it is listed in `requirements.txt`).

### 7. Hosted Dashboard Is Slow on the First Request

App Service unloads idle apps. Enable *Always On* (`az webapp config set ... --always-on true`).

### 8. Airflow or Metabase Is Not Reachable on the VM

Check that the VM's network security group allows ports 8080 and 3000, and that the containers are running (`sudo docker compose ps`). On a VM with little memory, containers can be killed under load; use a larger VM size or add a swap file.

---

## Future Improvements

Potential enhancements to make the pipeline more scalable and production-ready include:

- Implement incremental data extraction instead of repeatedly retrieving a fixed number of records.
- Request the newest incidents explicitly (for example `order="date_time DESC"` in the Socrata query) so the 2,500-record limit always covers the most recent data.
- Load with an upsert (`INSERT ... ON CONFLICT`) instead of replacing the table, so the primary key is preserved and history accumulates.
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
- Serve Airflow and Metabase over HTTPS behind a reverse proxy.
- Replace the IP allow-list with App Service VNet integration or a managed PostgreSQL service.
- Improve security by using Docker secrets or dedicated secret management such as Azure Key Vault.

---

## Author

**Prakhar Tiwari**

GitHub: [@prakhart056-student](https://github.com/prakhart056-student)

Project Repository: [Cambridge Crime Data Engineering Pipeline](https://github.com/prakhart056-student/DE)

---

## License

This project is intended for educational and demonstration purposes.

A formal open-source license can be added to the repository to specify usage, modification, and distribution permissions.
