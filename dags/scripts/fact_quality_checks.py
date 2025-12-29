import psycopg2
import logging
from airflow.hooks.base import BaseHook

logger = logging.getLogger("airflow.task")

def fact_checks():
    logger.info("Starting fact table quality checks")

    # Get connection info from Airflow
    conn_info = BaseHook.get_connection("weather")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password
    )
    cur = conn.cursor()

    # Define all checks
    checks = [
        (
            "Duplicate records",
            """
            SELECT COUNT(*) - COUNT(*) FILTER (WHERE rn = 1) AS duplicates
            FROM (
                SELECT city_id, datetime_id,
                       ROW_NUMBER() OVER (PARTITION BY city_id, datetime_id ORDER BY city_id) AS rn
                FROM fact_weather
            ) t;
            """
        ),
        (
            "Temperature out of range",
            """
            SELECT COUNT(*) FROM fact_weather
            WHERE temperature < -80 OR temperature > 150;
            """
        ),
        (
            "Humidity out of range",
            """
            SELECT COUNT(*) FROM fact_weather
            WHERE humidity < 0 OR humidity > 100;
            """
        ),
        (
            "Orphan records",
            """
            SELECT COUNT(*) FROM fact_weather f
            LEFT JOIN dim_city c ON f.city_id = c.city_id
            LEFT JOIN dim_datetime d ON f.datetime_id = d.datetime_id
            WHERE c.city_id IS NULL OR d.datetime_id IS NULL;
            """
        ),
    ]

    # Execute checks
    for name, sql in checks:
        cur.execute(sql)
        count = cur.fetchone()[0]
        if count > 0:
            logger.error(f"{name} check failed: {count}")
            raise Exception(f"{name} check failed: {count}")
        logger.info(f"{name} check passed")

    # Clean up
    cur.close()
    conn.close()

    logger.info("All fact quality checks passed")