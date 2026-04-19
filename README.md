# 🚗 Egypt Ride-Hailing Data Engineering Pipeline

An end-to-end data engineering project simulating a production-grade pipeline
for ride-hailing companies operating in Egypt (similar to Uber Egypt and inDrive Egypt).
The pipeline covers batch ingestion, real-time streaming, transformation, orchestration,
and analytics dashboards.


## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────────────────┐
│                         BATCH LANE (Nightly)                            │
│                                                                         │
│  ┌────────────┐                                                         │
│  │ PostgreSQL │──┐                                                      │
│  │  (Trips)   │  │    ┌─────────┐    ┌──────────┐    ┌─────────────┐  │
│  └────────────┘  ├───►│ Airflow │───►│  MSSQL   │───►│     dbt     │  │
│                  │    │  (DAG)  │    │ Staging  │    │(Transforms) │  │
│  ┌────────────┐  │    └─────────┘    └──────────┘    └──────┬──────┘  │
│  │  MongoDB   │──┘         │                                │          │
│  │ (Payments) │            │         ┌──────────────────────▼──────┐   │
│  └────────────┘            │         │      MSSQL Gold Layer       │   │
│                            │         │  fct_trips │ dim_drivers    │   │
│  ┌────────────┐            │         └──────────────────┬──────────┘   │
│  │    SSIS    │────────────┘                            │              │
│  │  (Manual)  │                                         │              │
│  └────────────┘                                         │              │
└─────────────────────────────────────────────────────────┼──────────────┘
                                                          │
┌─────────────────────────────────────────────────────────┼──────────────┐
│                    STREAMING LANE (Real-Time)            │              │
│                                                          │              │
│  ┌────────────┐    ┌───────┐    ┌─────────┐    ┌───────▼──────────┐   │
│  │ Driver App │───►│ Kafka │───►│ PySpark │───►│  MSSQL           │   │
│  │ Simulator  │    │Topics │    │Streaming│    │  streaming_      │   │
│  └────────────┘    └───────┘    └─────────┘    │  trip_agg        │   │
│                                                 └───────┬──────────┘   │
└─────────────────────────────────────────────────────────┼──────────────┘
                                                          │
                                                 ┌────────▼────────┐
                                                 │    Power BI     │
                                                 │   Dashboards    │
                                                 │  (4 Pages)      │
                                                 └─────────────────┘
```
### Two Lanes:
- **Batch Lane:** Airflow orchestrates nightly extraction from PostgreSQL and MongoDB
  → MSSQL staging → dbt transforms → Gold layer
- **Streaming Lane:** Kafka producer simulates live driver GPS events →
  PySpark Structured Streaming aggregates → MSSQL real-time table

---

## 🛠️ Technology Stack

| Tool | Version | Role |
|------|---------|------|
| Python | 3.12 | Scripting, data generation, pipeline logic |
| PostgreSQL | 15.6 | OLTP source database (trips, drivers, riders) |
| MS SQL Server | 2022 | Data warehouse (staging + gold schemas) |
| MongoDB | 7.0 | NoSQL source (payment records) |
| dbt | 1.8.9 | SQL transformations (staging → gold) |
| SSIS | 2022 | Manual batch ETL demonstration |
| Apache Airflow | 2.9.2 | Pipeline orchestration and scheduling |
| Apache Kafka | 3.6 (CP 7.6) | Real-time event streaming |
| PySpark | 3.5.1 | Structured streaming processing |
| Docker | Latest | Infrastructure containerization |
| Power BI | Desktop | Analytics dashboards |

---

## 📁 Project Structure
```
rides-pipeline/
├── docker-compose.yml              # All infrastructure services
├── .env                            # Secrets (never committed)
├── requirements.txt                # Python dependencies
├── README.md                       
│
├── sql/
│   ├── init_postgres.sql           # PostgreSQL schema DDL
│   ├── init_mssql.sql              # MSSQL staging schema DDL
│   └── check_data.sql              # Checking that data exists in MSSQL
│
├── data_sources/
│   ├── generate_trips.py           # Seeds PostgreSQL with 50k trips
│   ├── generate_payments.py        # Seeds MongoDB with 50k payments
│   └── kafka_producer.py           # Streams live GPS events to Kafka
│
├── airflow/
│   └── dags/
│       ├── batch_pipeline_dag.py   # Nightly batch pipeline DAG
│       └── streaming_monitor_dag.py # Streaming health monitor DAG
│
├── ssis/
│   └── extract_pg_to_mssql.py     # Python script called by SSIS
│                                   # (SSIS package saved on Windows)
│
├── rh_dbt/                         # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                # Bronze → Silver views
│       │   ├── sources.yml
│       │   ├── mart_stg_trips.sql
│       │   ├── mart_stg_drivers.sql
│       │   └── mart_stg_payments.sql
│       └── marts/                  # Silver → Gold tables
│           ├── fct_trips.sql
│           └── dim_drivers.sql
│
├── spark/
│   └── streaming_job.py            # PySpark Kafka → MSSQL streaming
│
└── mongo_data/                     # MongoDB bind mount (auto-created)
```

## ⚙️ Prerequisites

### On Windows:
- Docker Desktop with WSL2 Integration enabled
- SSMS (SQL Server Management Studio)
- Visual Studio 2022 with SSIS extension (for SSIS demo)
- Power BI Desktop

### On WSL Ubuntu:
- Python 3.12
- ODBC Driver 18 for SQL Server
- dbt-sqlserver

## 🚀 Setup & Execution Order

### 1. Clone the Repository
```
bash
git clone https://github.com/abdelrahmansmm/rides-pipeline.git
cd rides-pipeline
```

### 2. Configure Environment Variables
Edit .env with your credentials

### 3. Create Python Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Install ODBC Driver (WSL)
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | \
  sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt update
sudo ACCEPT_EULA=Y apt install -y msodbcsql18 unixodbc-dev
```

### 5. Start Docker Infrastructure
```bash
docker compose up -d
# Wait ~60 seconds for all services to be healthy
docker compose ps
```

### 6. Initialize MSSQL Schema
Connect to `localhost,1433` in SSMS with `sa` credentials and run `sql/init_mssql.sql`

### 7. Seed Source Databases
```bash
source .venv/bin/activate
python data_sources/generate_trips.py
python data_sources/generate_payments.py
```

### 8. Start Kafka Producer
```bash
nohup python data_sources/kafka_producer.py > logs/kafka_producer.log 2>&1 &
```

### 9. Trigger Airflow DAG
Toggle rh_batch_pipeline ON
→ Click Trigger DAG

### 10. Run dbt (also triggered by Airflow)
```bash
cd rh_dbt
dbt run
dbt test
```

### 11. Start PySpark Streaming Job
```bash
docker exec -it rp_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11 \
  /opt/spark/work-dir/streaming_job.py
```

### 12. Connect Power BI
```
Open Power BI Desktop
→ Get Data → SQL Server
→ Server: localhost,1433
→ Database: RidesHailingDW
→ Load: gold.fct_trips, gold.dim_drivers, dbo.streaming_trip_agg
```
---

## 📊 Power BI Dashboard Pages

| Page | Description |
|------|-------------|
| Executive Overview | KPI cards, revenue trend, trips by governorate |
| Trip Analytics | Vehicle type, payment method, peak hours analysis |
| Driver Performance | Driver tiers, ratings, geographic distribution |
| Real-Time Monitor | Live streaming data from Kafka → PySpark pipeline |

---

## 🗄️ Data Model

### Star Schema (Gold Layer)
```
fct_trips (50,000 rows)
├── driver_id → dim_drivers
├── pickup_zone_id
├── dropoff_zone_id
└── trip_date, trip_hour, surge_fare, distance_km...
dim_drivers (500 rows)
├── driver_tier (Gold/Silver/Bronze)
├── vehicle_type
└── governorate, rating...
streaming_trip_agg (real-time)
├── window_start, window_end (5-min tumbling windows)
├── zone_id
└── trip_count, total_revenue_egp...
```
---

## 📝 License
MIT License — feel free to use this project as a reference.
