from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from scripts.load_staging_data import load_staging_weather
from scripts.load_historical_data import load_historical_weather
from scripts.stage_quality_checks import stage_checks
from scripts.fact_quality_checks import fact_checks
from scripts.write_audit_log import write_weather_audit

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily weather ETL pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    load_weather_stage = PythonOperator(
        task_id="load_staging_data",
        python_callable=load_staging_weather,
    )

    load_weather_history = PythonOperator(
        task_id="load_historical_data",
        python_callable=load_historical_weather,
    )

    staging_quality_checks = PythonOperator(
        task_id="staging_quality_checks",
        python_callable=stage_checks
    )

    load_dim_datetime = PostgresOperator(
        task_id="load_dim_datetime",
        postgres_conn_id="weather",
        sql="sql/transform/load_date_dim_table.sql",
    )

    load_fact_weather = PostgresOperator(
        task_id="load_fact_weather",
        postgres_conn_id="weather",
        sql="sql/transform/load_fact_table.sql",
    )

    fact_quality_checks = PythonOperator(
        task_id="fact_quality_checks",
        python_callable=fact_checks
    )

    weather_audit_log = PythonOperator(
    task_id="write_weather_audit",
    python_callable=write_weather_audit,
    )

    (
        load_weather_stage
        >> load_weather_history
        >> staging_quality_checks
        >> load_dim_datetime
        >> load_fact_weather
        >> fact_quality_checks
        >> weather_audit_log
    )
