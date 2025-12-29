# One time run to populate staging_cities table
import json
import psycopg2
from airflow.hooks.base import BaseHook

conn_info = BaseHook.get_connection("weather")  # "weather" is the Airflow connection ID

conn = psycopg2.connect(
    host=conn_info.host,
    port=conn_info.port,
    dbname=conn_info.schema,  # schema in Airflow connection is DB name
    user=conn_info.login,
    password=conn_info.password
)

cur = conn.cursor()

with open("city_list.json", "r", encoding="utf-8") as f:
    cities = json.load(f)

insert_sql = """
INSERT INTO staging_cities (
    city_id, city_name, state, country, longitude, latitude
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (city_id) DO NOTHING;
"""

for c in cities:
    cur.execute(
        insert_sql,
        (
            c["id"],
            c["name"],
            c.get("state", ""),
            c["country"],
            c["coord"]["lon"],
            c["coord"]["lat"]
        )
    )

conn.commit()
cur.close()
conn.close()

print("Cities loaded into staging_cities")