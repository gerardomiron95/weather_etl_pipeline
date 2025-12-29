-- One time run to load data into dim_city table
CREATE TABLE IF NOT EXISTS staging_cities (
    city_id     INT PRIMARY KEY,
    city_name   TEXT,
    state       TEXT,
    country     TEXT,
    longitude   NUMERIC,
    latitude    NUMERIC
);

-- Run after load_cities.py
INSERT INTO dim_city (
    city_id,
    city_name,
    state,
    country,
    longitude,
    latitude
)
SELECT
    city_id,
    city_name,
    state,
    country,
    longitude,
    latitude
FROM staging_cities
ON CONFLICT (city_id) DO NOTHING;
