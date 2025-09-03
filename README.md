# logs-monitoring-pipeline

This repository contains the source code for an automated pipeline used to extract, parse, transform, and store backend log data from an automated document software. The solution is built using Python scripts orchestrated via Apache Airflow and stores the resulting structured data in a PostgreSQL database for analysis and dashboard integration.

---

## Objectives

- Automate real-time backend log processing  
- Structure unstructured log data for analysis  
- Centralize log storage in PostgreSQL  
- Facilitate the creation of monitoring dashboards and reports  
- Ensure the quality and consistency of processed data  

---

## System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Log Sources   │───▶│  Apache Airflow  │───▶│   PostgreSQL    │
│  (Backend Apps) │    │   ETL Pipeline   │    │    Database     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   Dashboards     │
                       │   & Analytics    │
                       └──────────────────┘
```

---

## Features

- Automated extraction of logs from multiple sources  
- Intelligent parsing of different log formats  
- Data transformation into standardized structures  
- Quality validation of processed data  
- Structured storage in PostgreSQL  
- Pipeline monitoring via Airflow UI  
- Error handling and automatic recovery  
- Alerts for processing failures  

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
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate     # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configuration

#### Airflow Connections

You need to configure Airflow connections for:

- PostgreSQL database

This can be done via the **Airflow UI** or using the **Airflow CLI**.

### 5. Environment Variables

Create a `.env` file or manually export the environment variables. Example:

```env
POSTGRES_HOST=localhost
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

---

## How to Run

### Start Airflow

```bash
airflow db init
airflow webserver --port 8080
airflow scheduler
```

### Execute the DAG

The DAG `docgenius_dag.py` will be automatically detected by Airflow if placed in the configured `dags/` folder.

You can trigger it manually or wait for a scheduled run via the **Airflow UI**:

[http://localhost:8080](http://localhost:8080)

### Monitor Execution

- Check **task logs** for detailed execution information  
- Access the **PostgreSQL database** to query processed data  
- Visualize the results in BI tools like **Power BI** or **Tableau**



