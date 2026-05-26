{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete_insert',
    engine='ReplacingMergeTree(raw_loaded_at)',
    unique_key=['ward_code', 'datetime_hour'],
    order_by='(province, date, assumeNotNull(ward_code), datetime_hour)',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 2,
        'join_use_nulls': 1
    }
) }}

with weather_incremental as (
    select
        province,
        ward_code,
        timestamp_utc as datetime_hour,
        toDate(timestamp_utc) as date,
        temp,
        humidity,
        wind_speed,
        wind_deg,
        pressure,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ ref('stg_openweather__meteorology') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

weather_dedup as (
    select
        province,
        ward_code,
        datetime_hour,
        date,
        argMax(temp, ingest_time) as temp,
        argMax(humidity, ingest_time) as humidity,
        argMax(wind_speed, ingest_time) as wind_speed,
        argMax(wind_deg, ingest_time) as wind_deg,
        argMax(pressure, ingest_time) as pressure,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as raw_sync_started_at
    from weather_incremental
    group by
        province,
        ward_code,
        datetime_hour,
        date
),

weather_with_fallbacks as (
    select
        province,
        ward_code,
        datetime_hour,
        date,
        cast(temp as Nullable(Float64)) as temperature,
        cast(humidity as Nullable(Float64)) as humidity,
        cast(wind_speed as Nullable(Float64)) as wind_speed,
        cast(wind_deg as Nullable(Float64)) as wind_direction,
        cast(pressure as Nullable(Float64)) as pressure,
        
        -- Province-level fallbacks (correctly ignoring nulls in averages)
        cast(avg(temp) over (partition by province, datetime_hour) as Nullable(Float64)) as province_temperature,
        cast(avg(humidity) over (partition by province, datetime_hour) as Nullable(Float64)) as province_humidity,
        cast(avg(wind_speed) over (partition by province, datetime_hour) as Nullable(Float64)) as province_wind_speed,
        cast(avg(wind_deg) over (partition by province, datetime_hour) as Nullable(Float64)) as province_wind_direction,
        cast(avg(pressure) over (partition by province, datetime_hour) as Nullable(Float64)) as province_pressure,

        max_raw_loaded_at as raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from weather_dedup
)

select
    datetime_hour,
    date,
    assumeNotNull(province) as province,
    assumeNotNull(ward_code) as ward_code,
    
    -- Main metrics
    temperature,
    humidity,
    wind_speed,
    wind_direction,
    pressure,
    
    -- Fallback metrics
    province_temperature,
    province_humidity,
    province_wind_speed,
    province_wind_direction,
    province_pressure,
    
    -- Custom indices and indicators
    case when coalesce(humidity, province_humidity) > 80 then 1 else 0 end as is_high_humidity_suppression,
    case when coalesce(wind_speed, province_wind_speed) < 1.0 then 1 else 0 end as is_stagnant_air_risk,
    cast(if(coalesce(wind_speed, province_wind_speed) < 1.0, 1.0, 0.0) as Float32) as stagnant_air_probability,

    now() as dbt_updated_at,
    raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at
from weather_with_fallbacks
where ward_code != ''
