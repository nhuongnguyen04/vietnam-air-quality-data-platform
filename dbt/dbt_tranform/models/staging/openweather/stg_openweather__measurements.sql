{{ config(materialized='view') }}

select
    'openweather'                                        AS source,
    concat('OPENWEATHER_', upper(city_name))              AS station_id,
    city_name                                            AS station_name,
    latitude,
    longitude,
    timestamp_utc,
    {{ standardize_pollutant_name('parameter') }}         AS parameter,
    value,
    unit,
    quality_flag,
    aqi_reported,
    ingest_time,
    raw_payload
from {{ source('openweather', 'raw_openweather_measurements') }}
