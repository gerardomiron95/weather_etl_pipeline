INSERT INTO fact_weather (
    city_id,
    datetime_id,

    temperature,
    feels_like,
    temp_min,
    temp_max,

    pressure,
    humidity,
    visibility,

    wind_speed,
    wind_deg,
    wind_gust,

    cloud_coverage,

    weather_id,
    weather_main,
    weather_description
)
SELECT
    sw.city_id,
    (sw.weather_json->>'dt')::BIGINT AS datetime_id,

    (sw.weather_json->'main'->>'temp')::DOUBLE PRECISION,
    (sw.weather_json->'main'->>'feels_like')::DOUBLE PRECISION,
    (sw.weather_json->'main'->>'temp_min')::DOUBLE PRECISION,
    (sw.weather_json->'main'->>'temp_max')::DOUBLE PRECISION,

    (sw.weather_json->'main'->>'pressure')::INT,
    (sw.weather_json->'main'->>'humidity')::INT,
    (sw.weather_json->>'visibility')::INT,

    (sw.weather_json->'wind'->>'speed')::DOUBLE PRECISION,
    (sw.weather_json->'wind'->>'deg')::INT,
    (sw.weather_json->'wind'->>'gust')::DOUBLE PRECISION,

    (sw.weather_json->'clouds'->>'all')::INT,

    (sw.weather_json->'weather'->0->>'id')::INT,
    sw.weather_json->'weather'->0->>'main',
    sw.weather_json->'weather'->0->>'description'
FROM staging_weather sw
ON CONFLICT (city_id, datetime_id) DO NOTHING;
