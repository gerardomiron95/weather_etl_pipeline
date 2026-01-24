## Overview
This project is an **Airflow-based ETL pipeline** that ingests weather data for 10 U.S. cities from the OpenWeatherMap API, stages it, and populates both historical and fact tables in a PostgreSQL database.  

Key features:  
- Load current weather data into a staging table.  
- Insert new records into a historical table without overwriting existing data.  
- Transform and populate fact and dimension tables.  
- Perform staging and fact table quality checks.  
- Maintain an audit log of ETL runs.  

---

## Project Structure
```
weather-etl-pipeline/
├── dags/
│ └── weather_etl_dag.py
├── scripts/
│ ├── load_staging_data.py
│ ├── load_historical_data.py
│ ├── stage_quality_checks.py
│ ├── fact_quality_checks.py
│ └── write_audit_log.py
├── sql/
│ └── transform/
│ ├── load_date_dim_table.sql
│ └── load_fact_table.sql
├── setup/
│ ├── create_tables.sql
│ └── load_cities.sql
├── .env
├── requirements.txt
└── README.md
```

---

## Prerequisites
- Python 3.12+  
- Docker and Docker Compose  
- Apache Airflow 2.9+  
- PostgreSQL (running in container or host)  
- OpenWeatherMap API key  

---

## Setup

1. **Clone the repository**  
- ```git clone https://github.com/yourusername/weather-etl-pipeline.git```
- ```cd weather-etl-pipeline```

2. **Create a virtual environment and install libraries**
- ```python3 -m venv .venv```
- ```pip install -r requirements.txt```

3. **Create a .env file (or set Airflow Variables**)
   - API_KEY=<your_openweathermap_api_key>

4. **Run create_tables.sql in setup folder to create schema**

5. **Run dags/scripts/load_cities.py to populate staging_cities table**
   - Run ```load_cities.sql``` in setup folder to populate dim_city table.
   - https://bulk.openweathermap.org/sample/ # Cities list comes from Openweather
   
6. **Start Airflow and Postgres using Docker Compose**
   - ```docker-compose up -d```
     
7. **Set Airflow connection**
   - Airflow should have a weather connection configured (host, port, username, password, database).

8. **Run the DAG**
   - Trigger weather_etl_pipeline manually in the Airflow UI, or let it run on its daily schedule.
  
---

## Tables
- stage_cities: Staging table for raw cities data.
- dim_city: Dimension table for cities with unique city_id.
- staging_weather: Staging table for raw weather data.
- historical_weather: Historical table (appends new data only).
- dim_datetime: Dimension table for dates.
- fact_weather: Fact table for processed weather metrics.
- weather_load_audit: ETL audit logs.

## Logging & Monitoring
- Uses Airflow logging for detailed ETL logs.
- Each DAG run logs the number of cities processed, records inserted, and status in weather_load_audit.

## Quality Checks
- Staging checks: ensure valid JSON structure, unique cities, non-empty data.
- Fact checks: duplicates, temperature/humidity ranges, orphan records.

## Dependencies
- psycopg2
- requests
- apache-airflow
- python-dotenv

---

## You can run the scripts locally outside of Airflow for testing
- python scripts/load_staging_data.py
- python scripts/load_historical_data.py
- python scripts/stage_quality_checks.py
- python scripts/fact_quality_checks.py
