import psycopg2
import requests
import json
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

def load_historical_weather():
    log.info("Starting historical weather load")

    conn_info = BaseHook.get_connection("weather")
    conn = psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password
    )
    cur = conn.cursor()

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

    records_inserted = 0

    for city in CITIES:
        city_id = city["id"]
        city_name = city["name"]
        state = city["state"]

        log.info(f"Fetching weather for {city_name}, {state}")

        url = f"https://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={API_KEY}&units=imperial"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            log.error(f"HTTP request failed for {city_name}: {e}")
            continue

        weather_json = response.json()

        try:
            cur.execute(
                """
                INSERT INTO historical_weather (
                    city_id, city_name, state, country, lat, lon, weather_json, retrieved_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    city_id,
                    city_name,
                    state,
                    weather_json.get("sys", {}).get("country"),
                    weather_json.get("coord", {}).get("lat"),
                    weather_json.get("coord", {}).get("lon"),
                    json.dumps(weather_json),
                    datetime.now()  # timestamp for this insertion
                )
            )
            records_inserted += 1
            log.info(f"Inserted historical weather for {city_name}")
        except Exception as e:
            log.error(f"DB insert failed for {city_name}: {e}")
            conn.rollback()
        else:
            conn.commit()

    cur.close()
    conn.close()

    log.info(f"Historical weather load complete. Total records inserted: {records_inserted}")
    return {"records_inserted": records_inserted}
