/*
Cities from openweather's list https://bulk.openweathermap.org/sample/
*/
CREATE TABLE IF NOT EXISTS dim_city (
    city_id INT PRIMARY KEY,
    city_name TEXT NOT NULL,
    state TEXT,
    country TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id BIGINT PRIMARY KEY,
    observed_at TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    hour INT NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_weather (
    city_id INT PRIMARY KEY,
    city_name TEXT NOT NULL,
    state TEXT,
    country TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    weather_json JSONB NOT NULL,
    retrieved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS historical_weather (
    city_id INT NOT NULL,
    city_name TEXT NOT NULL,
    state TEXT,
    country TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    weather_json JSONB NOT NULL,
    retrieved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city_id, retrieved_at)
);


CREATE TABLE IF NOT EXISTS fact_weather (
    city_id INT REFERENCES dim_city(city_id),
    datetime_id BIGINT REFERENCES dim_datetime(datetime_id),

    temperature DOUBLE PRECISION,
    feels_like DOUBLE PRECISION,
    temp_min DOUBLE PRECISION,
    temp_max DOUBLE PRECISION,

    pressure INT,
    humidity INT,
    visibility INT,

    wind_speed DOUBLE PRECISION,
    wind_deg INT,
    wind_gust DOUBLE PRECISION,

    cloud_coverage INT,

    weather_id INT,
    weather_main TEXT,
    weather_description TEXT,

    PRIMARY KEY (city_id, datetime_id)
);

CREATE TABLE IF NOT EXISTS weather_load_audit (
    load_id SERIAL PRIMARY KEY,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    city_count INT,
    records_inserted INT,
    status TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_weather_datetime
    ON fact_weather (datetime_id);

CREATE INDEX IF NOT EXISTS idx_fact_weather_city
    ON fact_weather (city_id);
