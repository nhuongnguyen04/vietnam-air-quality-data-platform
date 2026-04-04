{{ config(materialized='view') }}

-- OpenWeather raw station_id format: openweather:Da Nang:16.1:108.2
-- Using raw station_id to match dim_locations JOIN key in fact tables.
-- city_name is preserved as station_name for display in dashboards.
select
    'openweather'                                        AS source,
    station_id,                                          -- raw format: openweather:Da Nang:16.1:108.2
    city_name                                            AS station_name,
    city_name                                            AS city,
    city_name                                            AS province,
    latitude,
    longitude,
    timestamp_utc,
    {{ standardize_pollutant_name('parameter') }}        AS parameter,
    value,
    case
        when lower(parameter) in ('pm25', 'pm2.5', 'pm2_5', 'pm10', 'pm_10') then 'µg/m³'
        when lower(parameter) in ('o3', 'ozone', 'no2', 'nitrogen_dioxide', 'so2', 'sulfur_dioxide', 'nh3', 'no') then 'ppb'
        when lower(parameter) = 'co' then 'µg/m³'
        else 'unknown'
    end                                                  AS unit,
    quality_flag,
    aqi_reported,
    ingest_time,
    raw_payload,
    city_name
from {{ source('openweather', 'raw_openweather_measurements') }}
