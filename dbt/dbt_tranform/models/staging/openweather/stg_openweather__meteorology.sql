{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('openweather', 'raw_openweather_meteorology') }}
),

normalization AS (
    SELECT * FROM {{ ref('province_normalization') }}
),

deduplicated AS (
    SELECT
        r.source,
        COALESCE(n.target_name, r.province) as province,
        r.latitude,
        r.longitude,
        toStartOfHour(r.timestamp_utc) as timestamp_utc,
        r.temp,
        r.feels_like,
        r.humidity,
        r.pressure,
        r.wind_speed,
        r.wind_deg,
        r.clouds_all,
        r.ingest_time,
        -- Keep most recent ingest for each normalized province and hour
        ROW_NUMBER() OVER (PARTITION BY COALESCE(n.target_name, r.province), toStartOfHour(r.timestamp_utc) ORDER BY r.ingest_time DESC) as rn
    FROM raw_data r
    LEFT JOIN normalization n ON r.province = n.raw_name
)

SELECT
    source,
    province,
    latitude,
    longitude,
    timestamp_utc,
    temp,
    feels_like,
    humidity,
    pressure,
    wind_speed,
    wind_deg,
    clouds_all,
    now() as dbt_updated_at
FROM deduplicated
WHERE rn = 1
