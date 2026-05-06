{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(raw_loaded_at)',
    incremental_strategy='append',
    unique_key='(source, ward_code, timestamp_utc, parameter)',
    order_by='(province, timestamp_utc, ward_code, source, parameter)',
    partition_by='toYYYYMM(timestamp_utc)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864
    }
) }}

with measurements as (
    select
        source,
        ward_code,
        province,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        region_3,
        region_8
    from {{ ref('int_core__measurements_unified') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

aqi_measurements as (
    select
        source,
        ward_code,
        province,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        region_3,
        region_8
    from measurements
    where parameter in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
),

normalized_for_us as (
    select
        source,
        ward_code,
        province,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        region_3,
        region_8,
        -- Convert units for US AQI macros (which expect ppm for CO and ppb for others)
        -- Raw values are assumed to be µg/m³
        case
            when parameter = 'co' then value / 1145.0
            when parameter = 'no2' then value * 0.532
            when parameter = 'so2' then value * 0.382
            when parameter = 'o3' then value * 0.510
            else value
        end as value_us_standard
    from aqi_measurements
),

aqi_rows as (
    select
        source,
        ward_code,
        province,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        region_3,
        region_8,
        value_us_standard,
        {{ calculate_aqi('parameter', 'value_us_standard') }} as aqi_us,
        {{ calculate_aqi_vn('parameter', 'value') }}           as aqi_vn
    from normalized_for_us
),

aqi_final as (
    select
        source,
        ward_code,
        province,
        timestamp_utc,
        parameter,
        value,
        source_weight,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        region_3,
        region_8,
        value_us_standard,
        aqi_us,
        aqi_vn,
        cast(null, 'Nullable(Float64)') as max_aqi_us_in_hour,
        cast(null, 'Nullable(Float64)') as max_aqi_vn_in_hour,
        false as is_dominant_us,
        false as is_dominant_vn
    from aqi_rows
)

select
    source,
    ward_code,
    province,
    timestamp_utc,
    parameter,
    value,
    source_weight,
    ingest_time,
    raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at,
    region_3,
    region_8,
    value_us_standard,
    aqi_us,
    aqi_vn,
    max_aqi_us_in_hour,
    max_aqi_vn_in_hour,
    is_dominant_us,
    is_dominant_vn
from aqi_final
