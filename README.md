## -logs-monitoring-pipeline
This repository contains the source code for the automated pipeline used to extract, parse, transform and store backend log data from an automated document software. The solution is built using Python scripts orchestrated via Apache Airflow and stores the resulting structured data in a PostgreSQL database for analysis and dashboard integration.


## Objectives
- Automate real-time backend log processing  
- Structure unstructured log data for analysis  
- Centralize log storage in PostgreSQL  
- Facilitate the creation of monitoring dashboards and reports  
- Ensure the quality and consistency of processed data  


## System Architecture
┌─────────────────┐ ┌──────────────────┐ ┌─────────────────┐
│ Log Sources │───▶│ Apache Airflow │───▶│ PostgreSQL │
│ (Backend Apps) │ │ ETL Pipeline │ │ Database │
└─────────────────┘ └──────────────────┘ └─────────────────┘
│
▼
┌──────────────────┐
│ Dashboards │
│ & Analytics │
└──────────────────┘



## Features
- Automated extraction of logs from multiple sources
- Intelligent parsing of different log formats
- Data transformation into standardized structures
- Quality validation of processed data
- Structured storage in PostgreSQL
- Pipeline monitoring via Airflow UI
- Error handling and automatic recovery
- Alerts for processing failures

## Project Structure
logs-monitoring-pipeline/
│
├── dags/                          # Airflow DAGs
│   ├── log_processing_dag.py      # Main DAG
│   └── maintenance_dag.py         # Maintenance DAG
│
├── src/                           # Source code
│   ├── extractors/                # Extraction modules
│   │   ├── __init__.py
│   │   └── log_extractor.py
│   ├── parsers/                   # Log parsers
│   │   ├── __init__.py
│   │   └── log_parser.py
│   ├── transformers/              # Data transformations
│   │   ├── __init__.py
│   │   └── data_transformer.py
│   └── loaders/                   # Data loading
│       ├── __init__.py
│       └── db_loader.py
│
├── sql/                           # SQL scripts
│   ├── create_tables.sql
│   └── indexes.sql
│
├── config/                        # Configuration files
│   ├── airflow.cfg
│   ├── database.yaml
│   └── logging.conf
│
├── tests/                         # Automated tests
│   ├── test_extractors.py
│   ├── test_parsers.py
│   └── test_transformers.py
│
├── docs/                          # Documentation
│   ├── architecture.md
│   └── data_dictionary.md
│
├── requirements.txt               # Python dependencies
├── docker-compose.yml             # Docker configuration
├── Dockerfile                     # Docker image
└── README.md                      # This file




---

## Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/Martacorrei/-logs-monitoring-pipeline.git
cd -logs-monitoring-pipeline
```

### 2. Create a virtual environment 

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Intall Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configurations

Airflow Connections:
You need to configure Airflow connections for:
  - PostgreSQL database
This can be done through the Airflow UI or using CLI commands.

### 5. Environment Variables

Create a .env file or set environment variables manually. Example:


POSTGRES_HOST=
POSTGRES_USER=
POSTGRES_PASSWORD=


## How to Run

### Start Airflow

```bash
airflow db init
airflow webserver --port 8080
airflow scheduler
```

### Execute the DAG

The DAG docgenius_dag.py will be automatically detected by Airflow if placed in the configured DAGs folder.
You can trigger it manually or wait for a scheduled run via the Airflow UI at http://localhost:8080

### Monitor Execution

Check logs for detailed information about each task
Access the PostgreSQL database to query processed and structured log data
Visualize results in your preferred BI tool (e.g., Power BI, Tableau)





