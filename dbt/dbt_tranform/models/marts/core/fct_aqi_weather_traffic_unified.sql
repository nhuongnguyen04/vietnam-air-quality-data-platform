-- depends_on: {{ ref('fct_air_quality_ward_level_hourly') }}
{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(ward_code, datetime_hour)',
    order_by='(province, datetime_hour, ward_code)',
    partition_by='toYYYYMM(date)',
    overwrite=true
) }}

WITH aqi AS (
    SELECT 
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        latitude,
        longitude,
        hourly_avg_aqi_us as aqi_us,
        hourly_avg_aqi_vn as aqi_vn,
        pm25_hourly_avg as pm25,
        pm10_hourly_avg as pm10,
        co_hourly_avg as co,
        main_pollutant
    FROM {{ ref('fct_air_quality_ward_level_hourly') }}
    {% if is_incremental() %}
    WHERE datetime_hour >= (SELECT max(datetime_hour) FROM {{ this }}) - interval 3 hour
    {% endif %}
),

weather_ward AS (
    SELECT * FROM {{ ref('stg_openweather__meteorology') }}
),

weather_province AS (
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
    SELECT 
        ward_code,
        toStartOfHour(timestamp_utc) as datetime_hour,
        avg(value) as value,
        any(quality_flag) as quality_flag
    FROM {{ ref('stg_tomtom__flow') }}
    GROUP BY ward_code, datetime_hour
),

pop AS (
    SELECT * FROM {{ ref('stg_core__population') }}
)

SELECT
    a.datetime_hour AS datetime_hour,
    a.date AS date,
    assumeNotNull(a.province) as province,
    assumeNotNull(a.ward_code) as ward_code,
    a.region_3 AS region_3,
    a.region_8 AS region_8,
    a.latitude AS latitude,
    a.longitude AS longitude,
    
    -- Air Quality metrics
    a.aqi_us AS aqi_us,
    a.aqi_vn AS aqi_vn,
    a.pm25 AS pm25,
    a.pm10 AS pm10,
    a.co AS co,
    a.main_pollutant AS main_pollutant,
    
    -- Meteorology (Coalesce Ward -> Province fallback)
    coalesce(nullIf(ww.temp, 0), wp.temp) as temp,
    coalesce(nullIf(ww.humidity, 0), wp.humidity) as humidity,
    coalesce(nullIf(ww.wind_speed, 0), wp.wind_speed) as wind_speed,
    coalesce(nullIf(ww.wind_deg, 0), wp.wind_deg) as wind_deg,
    coalesce(nullIf(ww.pressure, 0), wp.pressure) as pressure,
    
    -- Traffic impact
    t.value as congestion_index,
    t.quality_flag as traffic_data_type,
    
    -- Environmental indicators
    case when coalesce(nullIf(ww.humidity, 0), wp.humidity) > 80 then 1 else 0 end as is_high_humidity_suppression,
    case when coalesce(nullIf(ww.wind_speed, 0), wp.wind_speed) < 1.0 then 1 else 0 end as is_stagnant_air_risk,
    
    -- Demographics and Exposure
    p.total_population as provincial_population,
    CAST((a.pm25 * p.total_population) / 1000000.0 AS Float32) as population_exposure_score,
    
    -- Traffic Correlation
    CAST(if(t.value > 0, a.pm25 / t.value, 0) AS Float32) as traffic_pollution_ratio,

    now() as dbt_updated_at

FROM aqi a
LEFT JOIN weather_ward ww ON 
    a.ward_code = ww.ward_code AND 
    a.datetime_hour = ww.timestamp_utc
LEFT JOIN weather_province wp ON
    a.province = wp.province AND
    a.datetime_hour = wp.timestamp_utc
LEFT JOIN traffic t ON a.ward_code = t.ward_code AND a.datetime_hour = t.datetime_hour
LEFT JOIN pop p ON a.province = p.location_name
