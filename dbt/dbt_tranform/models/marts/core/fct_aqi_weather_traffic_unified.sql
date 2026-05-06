-- depends_on: {{ ref('fct_air_quality_ward_level_hourly') }}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree(raw_loaded_at)',
    unique_key='(ward_code, datetime_hour)',
    order_by='(province, datetime_hour, ward_code)',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 2,
        'max_bytes_before_external_group_by': 2147483648
    }
) }}

with
aqi_incremental AS (
    SELECT 
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        avg_aqi_us as aqi_us,
        avg_aqi_vn as aqi_vn,
        pm25_avg as pm25,
        pm10_avg as pm10,
        co_avg as co,
        main_pollutant,
        last_ingested_at,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    FROM {{ ref('fct_air_quality_ward_level_hourly') }}
    WHERE {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

aqi_base AS (
    -- Normalize append-overlap duplicates to the intended ward-hour grain.
    SELECT
        datetime_hour,
        argMax(date, last_ingested_at) as date,
        province,
        ward_code,
        argMax(region_3, last_ingested_at) as region_3,
        argMax(region_8, last_ingested_at) as region_8,
        argMax(aqi_us, last_ingested_at) as aqi_us,
        argMax(aqi_vn, last_ingested_at) as aqi_vn,
        argMax(pm25, last_ingested_at) as pm25,
        argMax(pm10, last_ingested_at) as pm10,
        argMax(co, last_ingested_at) as co,
        argMax(main_pollutant, last_ingested_at) as main_pollutant,
        max(last_ingested_at) as max_last_ingested_at,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
    FROM aqi_incremental
    GROUP BY
        datetime_hour,
        province,
        ward_code
),

aqi AS (
    SELECT 
        ab.datetime_hour,
        ab.date,
        ab.province,
        ab.ward_code,
        ab.region_3,
        ab.region_8,
        ab.aqi_us,
        ab.aqi_vn,
        ab.pm25,
        ab.pm10,
        ab.co,
        ab.main_pollutant,
        ab.max_last_ingested_at as last_ingested_at,
        ab.max_raw_loaded_at as raw_loaded_at,
        ab.latest_raw_sync_run_id as raw_sync_run_id,
        ab.latest_raw_sync_started_at as raw_sync_started_at,
        adm.lat as latitude,
        adm.lon as longitude
    FROM aqi_base ab
    LEFT JOIN {{ ref('stg_core__administrative_units') }} adm ON ab.ward_code = adm.ward_code
),

weather_incremental AS (
    SELECT
        province,
        ward_code,
        timestamp_utc,
        temp,
        humidity,
        wind_speed,
        wind_deg,
        pressure,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    FROM {{ ref('stg_openweather__meteorology') }}
    WHERE {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

weather_ward AS (
    SELECT
        province,
        ward_code,
        timestamp_utc,
        argMax(temp, ingest_time) as temp,
        argMax(humidity, ingest_time) as humidity,
        argMax(wind_speed, ingest_time) as wind_speed,
        argMax(wind_deg, ingest_time) as wind_deg,
        argMax(pressure, ingest_time) as pressure,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
    FROM weather_incremental
    GROUP BY
        province,
        ward_code,
        timestamp_utc
),

weather AS (
    SELECT
        province,
        ward_code,
        timestamp_utc,
        temp,
        humidity,
        wind_speed,
        wind_deg,
        pressure,
        max_raw_loaded_at as raw_loaded_at,
        latest_raw_sync_run_id as raw_sync_run_id,
        latest_raw_sync_started_at as raw_sync_started_at,
        avg(temp) over (partition by province, timestamp_utc) as province_temp,
        avg(humidity) over (partition by province, timestamp_utc) as province_humidity,
        avg(wind_speed) over (partition by province, timestamp_utc) as province_wind_speed,
        avg(wind_deg) over (partition by province, timestamp_utc) as province_wind_deg,
        avg(pressure) over (partition by province, timestamp_utc) as province_pressure
    FROM weather_ward
),

traffic AS (
    SELECT 
        ward_code,
        toStartOfHour(timestamp_utc) as datetime_hour,
        avg(value) as value,
        any(quality_flag) as quality_flag,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
    FROM {{ ref('stg_tomtom__flow') }}
    WHERE {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
    GROUP BY ward_code, datetime_hour
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
    
    -- Meteorology (Ward-level first, province fallback)
    coalesce(nullIf(w.temp, 0), w.province_temp) as temperature,
    coalesce(nullIf(w.humidity, 0), w.province_humidity) as humidity,
    coalesce(nullIf(w.wind_speed, 0), w.province_wind_speed) as wind_speed,
    coalesce(nullIf(w.wind_deg, 0), w.province_wind_deg) as wind_direction,
    coalesce(nullIf(w.pressure, 0), w.province_pressure) as pressure,
    
    -- Traffic impact
    t.value as congestion_index,
    t.quality_flag as traffic_data_type,
    
    -- Environmental indicators
    case when coalesce(nullIf(w.humidity, 0), w.province_humidity) > 80 then 1 else 0 end as is_high_humidity_suppression,
    case when coalesce(nullIf(w.wind_speed, 0), w.province_wind_speed) < 1.0 then 1 else 0 end as is_stagnant_air_risk,
    CAST(if(coalesce(nullIf(w.wind_speed, 0), w.province_wind_speed) < 1.0, 1.0, 0.0) AS Float32) as stagnant_air_probability,
    CAST(0.0 AS Float32) as weather_influence_pct,
    
    -- Demographics and Exposure
    p.total_population as provincial_population,
    CAST((a.pm25 * p.total_population) / 1000000.0 AS Float32) as population_exposure_score,
    
    -- Traffic Correlation
    CAST(a.pm25 * t.value AS Float32) as traffic_pollution_impact_score,
    CAST(0.0 AS Float32) as traffic_contribution_pct,
    CAST(if(t.value > 0, a.pm25 / t.value, 0) AS Float32) as traffic_pollution_ratio,

    now() as dbt_updated_at,

    greatest(
        coalesce(a.raw_loaded_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(w.raw_loaded_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(t.max_raw_loaded_at, toDateTime('1970-01-01 00:00:00'))
    ) as raw_loaded_at,
    coalesce(t.latest_raw_sync_run_id, w.raw_sync_run_id, a.raw_sync_run_id, '') as raw_sync_run_id,
    greatest(
        coalesce(a.raw_sync_started_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(w.raw_sync_started_at, toDateTime('1970-01-01 00:00:00')),
        coalesce(t.latest_raw_sync_started_at, toDateTime('1970-01-01 00:00:00'))
    ) as raw_sync_started_at

FROM aqi a
LEFT JOIN weather w ON
    a.province = w.province AND
    a.ward_code = w.ward_code AND
    a.datetime_hour = w.timestamp_utc
LEFT JOIN (
    select
        ward_code,
        datetime_hour,
        value,
        quality_flag,
        max_raw_loaded_at,
        latest_raw_sync_run_id,
        latest_raw_sync_started_at
    from traffic
) t ON a.ward_code = t.ward_code AND a.datetime_hour = t.datetime_hour
LEFT JOIN pop p ON a.province = p.location_name
