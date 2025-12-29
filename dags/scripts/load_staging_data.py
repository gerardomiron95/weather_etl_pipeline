import psycopg2
import requests
import json
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

def load_staging_weather():
    log.info("Starting staging weather load")

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

    # Get API key from Airflow Variables
    API_KEY = Variable.get("WEATHER_API_KEY")

    CITIES = [
        {"id": 4174757, "name": "Tampa", "state": "FL"},
        {"id": 4259418, "name": "Indianapolis", "state": "IN"},
        {"id": 4460243, "name": "Charlotte", "state": "NC"},
        {"id": 4699066, "name": "Houston", "state": "TX"},
        {"id": 4887398, "name": "Chicago", "state": "IL"},
        {"id": 4259640, "name": "Jasper", "state": "IN"},
        {"id": 4671654, "name": "Austin", "state": "TX"},
        {"id": 4164138, "name": "Miami", "state": "FL"},
        {"id": 4487042, "name": "Raleigh", "state": "NC"},
        {"id": 4890864, "name": "Elgin", "state": "IL"}
    ]

    records_processed = 0

    for city in CITIES:
        city_id = city["id"]
        city_name = city["name"]
        state = city["state"]

        log.info(f"Fetching weather for {city_name}, {state}")
        url = f"https://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={API_KEY}&units=imperial"
        response = requests.get(url)

        if response.status_code != 200:
            log.error(f"Failed for {city_name}: {response.text}")
            continue

        weather_json = response.json()
        now = datetime.now()  # local timestamp

        # Insert or update record in staging table
        cur.execute(
            """
            INSERT INTO staging_weather (
                city_id, city_name, state, country, lat, lon, weather_json, retrieved_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id) DO UPDATE
            SET weather_json = EXCLUDED.weather_json,
                retrieved_at = EXCLUDED.retrieved_at
            """,
            (
                city_id,
                city_name,
                state,
                weather_json.get("sys", {}).get("country"),
                weather_json.get("coord", {}).get("lat"),
                weather_json.get("coord", {}).get("lon"),
                json.dumps(weather_json),
                now
            )
        )

        records_processed += 1
        log.info(f"Inserted/Updated {city_name} at {now}")

    conn.commit()
    cur.close()
    conn.close()
    log.info("Staging weather load complete. Total records processed: %s", records_processed)

    return {"city_count": records_processed, "records_inserted": records_processed}