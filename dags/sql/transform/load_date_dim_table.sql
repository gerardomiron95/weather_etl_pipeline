INSERT INTO dim_datetime (
    datetime_id,
    observed_at,
    date,
    hour,
    day,
    month,
    year
)
SELECT DISTINCT
    (weather_json->>'dt')::BIGINT                     AS datetime_id,
    to_timestamp((weather_json->>'dt')::BIGINT)       AS observed_at,
    DATE(to_timestamp((weather_json->>'dt')::BIGINT)) AS date,
    EXTRACT(HOUR  FROM to_timestamp((weather_json->>'dt')::BIGINT))::INT AS hour,
    EXTRACT(DAY   FROM to_timestamp((weather_json->>'dt')::BIGINT))::INT AS day,
    EXTRACT(MONTH FROM to_timestamp((weather_json->>'dt')::BIGINT))::INT AS month,
    EXTRACT(YEAR  FROM to_timestamp((weather_json->>'dt')::BIGINT))::INT AS year
FROM staging_weather
ON CONFLICT (datetime_id) DO NOTHING;
