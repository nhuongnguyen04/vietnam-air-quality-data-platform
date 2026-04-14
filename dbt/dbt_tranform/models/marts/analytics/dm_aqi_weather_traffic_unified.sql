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

weather AS (
    SELECT * FROM {{ ref('stg_openweather__meteorology') }}
),

traffic AS (
    SELECT 
        station_name,
        toStartOfHour(timestamp_utc) as datetime_hour,
        avg(value) as value,
        any(quality_flag) as quality_flag
    FROM {{ ref('stg_tomtom__flow') }}
    GROUP BY station_name, datetime_hour
),

station_metadata AS (
    -- New high-precision metadata (from TomTom Search API)
    SELECT station_name, latitude, longitude FROM {{ ref('unified_stations_metadata') }}
),

pop AS (
    -- Updated 2026 population projections
    SELECT * FROM {{ ref('stg_core__population') }}
)

SELECT
    a.datetime_hour as datetime_hour,
    a.date as date,
    a.province as province,
    a.district as district,
    a.station_name as station_name,
    -- Fallback to extracting coordinates from station_name if metadata join fails
    coalesce(nullIf(m.latitude, 0), toFloat64OrNull(splitByChar(':', a.station_name)[3])) as station_latitude,
    coalesce(nullIf(m.longitude, 0), toFloat64OrNull(splitByChar(':', a.station_name)[4])) as station_longitude,
    
    -- Air Quality metrics
    a.aqi_us as aqi_us,
    a.aqi_vn as aqi_vn,
    a.pm25 as pm25,
    a.pm10 as pm10,
    a.co as co,
    
    -- Meteorology
    w.temp as temp,
    w.humidity as humidity,
    w.wind_speed as wind_speed,
    w.wind_deg as wind_deg,
    w.pressure as pressure,
    
    -- Traffic
    t.value as congestion_index,
    t.quality_flag as traffic_data_type,
    
    -- Weather Indicators
    case when w.humidity > 80 then 1 else 0 end as is_high_humidity_suppression,
    case when w.wind_speed < 1.0 then 1 else 0 end as is_stagnant_air_risk,
    
    -- Demographics
    p.total_population as provincial_population,
    
    -- Advanced Calculated Metrics
    -- Exposure Score: PM2.5 intensity weighted by population
    CAST((a.pm25 * p.total_population) / 1000000.0 AS Float32) as population_exposure_score,
    
    -- Traffic Impact Ratio: How much pollution per unit of congestion
    -- Avoid division by zero
    CAST(if(t.value > 0, a.pm25 / t.value, 0) AS Float32) as traffic_pollution_ratio,

    now() as dbt_updated_at

FROM aqi a
LEFT JOIN station_metadata m ON a.station_name = m.station_name
LEFT JOIN weather w ON a.province = w.province AND a.datetime_hour = w.timestamp_utc
LEFT JOIN traffic t ON a.station_name = t.station_name AND a.datetime_hour = t.datetime_hour
LEFT JOIN pop p ON a.province = p.location_name
