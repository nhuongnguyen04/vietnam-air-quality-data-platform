{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(province, date, assumeNotNull(ward_code), datetime_hour)',
    partition_by='toYYYYMM(date)',
    query_settings={
        'join_use_nulls': 1
    }
) }}

with
aqi_base AS (
    SELECT 
        datetime_hour,
        province,
        ward_code,
        argMax(date, last_ingested_at) as date,
        argMax(region_3, last_ingested_at) as region_3,
        argMax(region_8, last_ingested_at) as region_8,
        argMax(avg_aqi_us, last_ingested_at) as aqi_us,
        argMax(avg_aqi_vn, last_ingested_at) as aqi_vn,
        argMax(pm25_avg, last_ingested_at) as pm25,
        argMax(pm10_avg, last_ingested_at) as pm10,
        argMax(co_avg, last_ingested_at) as co,
        argMax(main_pollutant, last_ingested_at) as main_pollutant,
        argMax(source_mix, last_ingested_at) as source_mix,
        argMax(confidence_score, last_ingested_at) as confidence_score,
        argMax(confidence_level, last_ingested_at) as confidence_level,
        sum(aqiin_observation_count) as aqiin_observation_count,
        sum(openweather_observation_count) as openweather_observation_count,
        max(last_ingested_at) as max_last_ingested_at,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
    FROM {{ ref('fct_air_quality_ward_level_hourly') }}
    GROUP BY
        datetime_hour,
        province,
        ward_code
),

aqi AS (
    SELECT
        ab.datetime_hour,
        ab.province,
        ab.ward_code,
        ab.date,
        ab.region_3,
        ab.region_8,
        ab.aqi_us,
        ab.aqi_vn,
        ab.pm25,
        ab.pm10,
        ab.co,
        ab.main_pollutant,
        ab.source_mix,
        ab.confidence_score,
        ab.confidence_level,
        ab.aqiin_observation_count,
        ab.openweather_observation_count,
        ab.max_last_ingested_at as last_ingested_at,
        ab.max_raw_loaded_at as raw_loaded_at,
        ab.latest_raw_sync_run_id as raw_sync_run_id,
        ab.latest_raw_sync_started_at as raw_sync_started_at,
        adm.lat as latitude,
        adm.lon as longitude
    FROM aqi_base ab
    LEFT JOIN {{ ref('stg_core__administrative_units') }} adm ON ab.ward_code = adm.ward_code
),

weather AS (
    SELECT
        province,
        ward_code,
        datetime_hour,
        temperature,
        humidity,
        wind_speed,
        wind_direction,
        pressure,
        province_temperature,
        province_humidity,
        province_wind_speed,
        province_wind_direction,
        province_pressure,
        is_high_humidity_suppression,
        is_stagnant_air_risk,
        stagnant_air_probability,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    FROM {{ ref('fct_weather_ward_hourly') }}
),

traffic AS (
    SELECT 
        ward_code,
        datetime_hour,
        congestion_index,
        traffic_data_type,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    FROM {{ ref('fct_traffic_ward_hourly') }}
),

pop AS (
    SELECT
        location_name,
        total_population
    FROM {{ ref('stg_core__population') }}
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
    a.source_mix AS source_mix,
    a.confidence_score AS confidence_score,
    a.confidence_level AS confidence_level,
    a.aqiin_observation_count AS aqiin_observation_count,
    a.openweather_observation_count AS openweather_observation_count,
    
    -- Meteorology (Ward-level first, province fallback, explicitly Nullable)
    cast(coalesce(w.temperature, w.province_temperature) AS Nullable(Float64)) as temperature,
    cast(coalesce(w.humidity, w.province_humidity) AS Nullable(Float64)) as humidity,
    cast(coalesce(w.wind_speed, w.province_wind_speed) AS Nullable(Float64)) as wind_speed,
    cast(coalesce(w.wind_direction, w.province_wind_direction) AS Nullable(Float64)) as wind_direction,
    cast(coalesce(w.pressure, w.province_pressure) AS Nullable(Float64)) as pressure,
    
    -- Traffic impact
    cast(t.congestion_index AS Nullable(Float64)) as congestion_index,
    cast(t.traffic_data_type AS Nullable(String)) as traffic_data_type,
    
    -- Environmental indicators
    case when coalesce(w.humidity, w.province_humidity) > 80 then 1 else 0 end as is_high_humidity_suppression,
    case when coalesce(w.wind_speed, w.province_wind_speed) < 1.0 then 1 else 0 end as is_stagnant_air_risk,
    CAST(if(coalesce(w.wind_speed, w.province_wind_speed) < 1.0, 1.0, 0.0) AS Float32) as stagnant_air_probability,
    CAST(0.0 AS Float32) as weather_influence_pct,
    
    -- Demographics and Exposure
    cast(p.total_population AS Nullable(Int64)) as provincial_population,
    cast((a.pm25 * p.total_population) / 1000000.0 AS Nullable(Float32)) as population_exposure_score,
    
    -- Traffic Correlation
    cast(a.pm25 * t.congestion_index AS Nullable(Float32)) as traffic_pollution_impact_score,
    CAST(0.0 AS Float32) as traffic_contribution_pct,
    cast(if(t.congestion_index > 0, a.pm25 / t.congestion_index, 0) AS Nullable(Float32)) as traffic_pollution_ratio,

    now() as dbt_updated_at,

    greatest(
        coalesce(a.raw_loaded_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(w.raw_loaded_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(t.raw_loaded_at, toDateTime('1970-01-01 00:00:00'))
    ) as raw_loaded_at,
    coalesce(t.raw_sync_run_id, w.raw_sync_run_id, a.raw_sync_run_id, '') as raw_sync_run_id,
    greatest(
        coalesce(a.raw_sync_started_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(w.raw_sync_started_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(t.raw_sync_started_at, toDateTime('1970-01-01 00:00:00'))
    ) as raw_sync_started_at

FROM aqi a
LEFT JOIN weather w ON
    a.province = w.province AND
    a.ward_code = w.ward_code AND
    a.datetime_hour = w.datetime_hour
LEFT JOIN traffic t ON 
    a.ward_code = t.ward_code AND 
    a.datetime_hour = t.datetime_hour
LEFT JOIN pop p ON a.province = p.location_name
