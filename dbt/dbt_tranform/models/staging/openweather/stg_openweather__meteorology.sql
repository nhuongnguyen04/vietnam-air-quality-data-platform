{{ config(
    materialized='view'
) }}

WITH raw_data AS (
    SELECT * FROM {{ source('openweather', 'raw_openweather_meteorology') }}
),

ingestion_points AS (
    SELECT * FROM {{ ref('openweather_ingestion_points') }}
),

province_norm AS (
    SELECT * FROM {{ ref('province_normalization') }}
),

joined AS (
    SELECT
        r.source,
        COALESCE(p.province, r.province) AS raw_province,
        COALESCE(p.district, 'Unknown') AS district,
        r.latitude,
        r.longitude,
        toStartOfHour(r.timestamp_utc) AS hourly_timestamp,
        r.temp,
        r.feels_like,
        r.humidity,
        r.pressure,
        r.wind_speed,
        r.wind_deg,
        r.clouds_all,
        r.ingest_time
    FROM raw_data r
    LEFT JOIN ingestion_points p ON 
        toDecimal32(r.latitude, 4) = toDecimal32(p.latitude, 4) AND 
        toDecimal32(r.longitude, 4) = toDecimal32(p.longitude, 4)
),

normalized AS (
    SELECT
        j.*,
        COALESCE(pn.target_name, j.raw_province) AS normalized_province
    FROM joined j
    LEFT JOIN province_norm pn ON j.raw_province = pn.raw_name
    WHERE normalized_province IS NOT NULL AND normalized_province != ''
),

deduplicated AS (
    SELECT
        *,
        -- Keeping the latest ingest per (province, district, hour)
        row_number() OVER (
            PARTITION BY normalized_province, district, hourly_timestamp
            ORDER BY ingest_time DESC
        ) AS rn
    FROM normalized
)

SELECT
    source,
    normalized_province AS province,
    district,
    latitude,
    longitude,
    hourly_timestamp AS timestamp_utc,
    temp,
    feels_like,
    humidity,
    pressure,
    wind_speed,
    wind_deg,
    clouds_all,
    now() AS dbt_updated_at
FROM deduplicated
WHERE rn = 1
