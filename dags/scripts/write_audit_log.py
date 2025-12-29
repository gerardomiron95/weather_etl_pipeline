import logging
import psycopg2
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

def write_weather_audit(ti):
    conn_info = BaseHook.get_connection("weather")

    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password
    )
    cur = conn.cursor()

    staging_metrics = ti.xcom_pull(task_ids="load_staging_data")
    history_metrics = ti.xcom_pull(task_ids="load_historical_data")

    city_count = staging_metrics["city_count"]
    records_inserted = history_metrics["records_inserted"]

    cur.execute(
        """
        INSERT INTO weather_load_audit (city_count, records_inserted, status)
        VALUES (%s, %s, %s)
        """,
        (city_count, records_inserted, "SUCCESS"),
    )

    conn.commit()
    cur.close()
    conn.close()

    log.info(
        "Audit written: city_count=%s records_inserted=%s",
        city_count,
        records_inserted
    )
