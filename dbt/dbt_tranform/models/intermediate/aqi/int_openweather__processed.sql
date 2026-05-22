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

with ow_raw as (
    select
        source,
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
        raw_sync_started_at
    from {{ ref('stg_openweather__measurements') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

normalized_for_us as (
    select *,
        -- Convert units for US AQI macros (which expect ppm for CO and ppb for others)
        -- Raw values are assumed to be µg/m³
        case
            when parameter = 'co' then value / 1145.0
            when parameter = 'no2' then value * 0.532
            when parameter = 'so2' then value * 0.382
            when parameter = 'o3' then value * 0.510
            else value
        end as value_us_standard
    from ow_raw
),

with_aqi as (
    select
        source,
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
