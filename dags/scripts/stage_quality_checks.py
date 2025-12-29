import psycopg2
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


def stage_checks():
    log = LoggingMixin().log
    log.info("Running staging quality checks")

    conn_info = BaseHook.get_connection("weather")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM staging_weather;")
    row_count = cur.fetchone()[0]
    if row_count == 0:
        raise ValueError("staging_weather is empty")

    cur.execute("SELECT COUNT(DISTINCT city_id) FROM staging_weather;")
    cities_loaded = cur.fetchone()[0]
    if cities_loaded == 0:
        raise ValueError("No cities loaded")

    cur.execute("SELECT MAX(retrieved_at) FROM staging_weather;")
    log.info("Latest retrieval time: %s", cur.fetchone()[0])

    cur.execute("""
        SELECT COUNT(*) FROM staging_weather
        WHERE weather_json->'main' IS NULL
           OR weather_json->'wind' IS NULL
           OR weather_json->'weather' IS NULL;
    """)
    bad_records = cur.fetchone()[0]
    if bad_records > 0:
        raise ValueError(f"{bad_records} bad staging records found")

    cur.close()
    conn.close()

    log.info("Staging quality checks passed")
