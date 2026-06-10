{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(raw_loaded_at)',
    incremental_strategy='append',
    unique_key='(ward_code, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    tags=['pipeline_v2'],
    query_settings={
        'max_threads': 2
    }
) }}

with waqi_raw as (
    select 
        m.source as source,
        s.station_name as station_name,
        s.ward_code as ward_code,
        s.province as province,
        s.latitude as latitude,
        s.longitude as longitude,
        m.timestamp_utc as timestamp_utc,
        m.parameter as parameter,
        m.value as value,
        m.aqi_reported as aqi_reported,
        m.quality_flag as quality_flag,
        m.ingest_time as ingest_time,
        m.raw_loaded_at as raw_loaded_at,
        m.raw_sync_run_id as raw_sync_run_id,
        m.raw_sync_started_at as raw_sync_started_at
    from {{ ref('stg_waqi__measurements') }} m
    join {{ ref('stg_core__stations') }} s 
      on m.station_name = s.station_name 
     and s.station_source = 'waqi'
    where {{ downstream_incremental_predicate('m.raw_sync_run_id', 'm.raw_loaded_at') }}
),

normalized_for_us as (
    select 
        source,
        station_name,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        quality_flag,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        -- Convert units for US AQI macros (which expect ppm for CO and ppb for others)
        -- Raw values are assumed to be µg/m³
        case
            when parameter = 'co' then value / 1145.0
            when parameter = 'no2' then value * 0.532
            when parameter = 'so2' then value * 0.382
            when parameter = 'o3' then value * 0.510
            else value
        end as value_us_standard
    from waqi_raw
),

with_aqi as (
    select
        source,
        station_name,
        ward_code,
        province,
        latitude,
        longitude,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        quality_flag,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        value_us_standard,
        {{ calculate_aqi_vn('parameter', 'value') }} as aqi_vn,
        {{ calculate_aqi('parameter', 'value_us_standard') }} as aqi_us,
        {{ get_vietnam_region_3('province') }} as region_3,
        {{ get_vietnam_region_8('province') }} as region_8
    from normalized_for_us
)

select * from with_aqi
