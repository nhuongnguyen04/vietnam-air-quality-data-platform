{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(station_name, datetime_hour)',
    order_by='(province, datetime_hour, station_name)',
    partition_by='toYYYYMM(date)'
) }}

WITH aqi AS (
    SELECT 
        toStartOfHour(timestamp_utc) as datetime_hour,
        toDate(timestamp_utc) as date,
        province,
        district,
        station_name,
        -- Use the coordinates passed through from intermediate models
        any(latitude) as latitude,
        any(longitude) as longitude,
        max(aqi_us) as aqi_us,
        max(aqi_vn) as aqi_vn,
        maxIf(value, parameter = 'pm25') as pm25,
        maxIf(value, parameter = 'pm10') as pm10,
        maxIf(value, parameter = 'co') as co
    FROM {{ ref('int_aqi__calculations') }}
    {% if is_incremental() %}
    WHERE timestamp_utc >= (SELECT max(datetime_hour) FROM {{ this }}) - interval 3 hour
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5
),

weather_district AS (
    -- Staging weather is already district-level
    SELECT * FROM {{ ref('stg_openweather__meteorology') }}
),

weather_province AS (
    -- Average by province for points without district meteorology
    SELECT 
        province,
        timestamp_utc,
        avg(temp) as temp,
        avg(humidity) as humidity,
        avg(wind_speed) as wind_speed,
        avg(wind_deg) as wind_deg,
        avg(pressure) as pressure
    FROM {{ ref('stg_openweather__meteorology') }}
    GROUP BY province, timestamp_utc
),

traffic AS (
    -- TomTom traffic flow at station centroids
    SELECT 
        station_name,
        toStartOfHour(timestamp_utc) as datetime_hour,
        avg(value) as value,
        any(quality_flag) as quality_flag
    FROM {{ ref('stg_tomtom__flow') }}
    GROUP BY station_name, datetime_hour
),

pop AS (
    -- 2026 population projections
    SELECT * FROM {{ ref('stg_core__population') }}
)

SELECT
    a.datetime_hour as datetime_hour,
    a.date as date,
    a.province as province,
    a.district as district,
    a.station_name as station_name,
    -- Reliability: Prefer intermediate coordinates passed from upstream
    a.latitude as station_latitude,
    a.longitude as station_longitude,
    
    -- Air Quality metrics
    a.aqi_us as aqi_us,
    a.aqi_vn as aqi_vn,
    a.pm25 as pm25,
    a.pm10 as pm10,
    a.co as co,
    
    -- Meteorology (Coalesce District -> Province fallback)
    -- Using nullIf to handle ClickHouse's 0.0 default for non-matching joins
    coalesce(nullIf(wd.temp, 0), wp.temp) as temp,
    coalesce(nullIf(wd.humidity, 0), wp.humidity) as humidity,
    coalesce(nullIf(wd.wind_speed, 0), wp.wind_speed) as wind_speed,
    coalesce(nullIf(wd.wind_deg, 0), wp.wind_deg) as wind_deg,
    coalesce(nullIf(wd.pressure, 0), wp.pressure) as pressure,
    
    -- Traffic impact
    t.value as congestion_index,
    t.quality_flag as traffic_data_type,
    
    -- Environmental indicators
    case when coalesce(nullIf(wd.humidity, 0), wp.humidity) > 80 then 1 else 0 end as is_high_humidity_suppression,
    case when coalesce(nullIf(wd.wind_speed, 0), wp.wind_speed) < 1.0 then 1 else 0 end as is_stagnant_air_risk,
    
    -- Demographics and Exposure
    p.total_population as provincial_population,
    CAST((a.pm25 * p.total_population) / 1000000.0 AS Float32) as population_exposure_score,
    
    -- Traffic Correlation
    CAST(if(t.value > 0, a.pm25 / t.value, 0) AS Float32) as traffic_pollution_ratio,

    now() as dbt_updated_at

FROM aqi a
LEFT JOIN weather_district wd ON 
    a.province = wd.province AND 
    a.district = wd.district AND 
    a.datetime_hour = wd.timestamp_utc
LEFT JOIN weather_province wp ON
    a.province = wp.province AND
    a.datetime_hour = wp.timestamp_utc
LEFT JOIN traffic t ON a.station_name = t.station_name AND a.datetime_hour = t.datetime_hour
LEFT JOIN pop p ON a.province = p.location_name
