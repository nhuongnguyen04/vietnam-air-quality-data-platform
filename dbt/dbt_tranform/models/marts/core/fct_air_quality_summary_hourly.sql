{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(raw_loaded_at)',
    incremental_strategy='delete_insert',
    unique_key=['province', 'ward_code', 'datetime_hour', 'source'],
    order_by='(province, ward_code, datetime_hour, source)',
    partition_by='toYYYYMM(datetime_hour)',
    query_settings={
        'max_threads': 1,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864
    }
) }}

with calculations as (
    select
        timestamp_utc,
        province,
        ward_code,
        region_3,
        region_8,
        source,
        source_weight,
        parameter,
        value,
        aqi_us,
        aqi_vn,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ ref('int_aqi__calculations') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

pivoted as (
    select
        toStartOfHour(timestamp_utc) as datetime_hour,
        any(toDate(timestamp_utc)) as date,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        source,
        any(source_weight) as source_weight,
        
        -- Overall AQIs
        max(aqi_us) as final_aqi_us,
        max(aqi_vn) as final_aqi_vn,
        
        -- Concentrations
        nullIf(avgIf(value, parameter = 'pm25'), 0/0) as pm25_value,
        nullIf(avgIf(value, parameter = 'pm10'), 0/0) as pm10_value,
        nullIf(avgIf(value, parameter = 'co'),   0/0) as co_value,
        nullIf(avgIf(value, parameter = 'no2'),  0/0) as no2_value,
        nullIf(avgIf(value, parameter = 'so2'),  0/0) as so2_value,
        nullIf(avgIf(value, parameter = 'o3'),   0/0) as o3_value,

        -- Sub-AQI Indices (Vietnam Standard)
        nullIf(avgIf(aqi_vn, parameter = 'pm25'), 0/0) as pm25_aqi,
        nullIf(avgIf(aqi_vn, parameter = 'pm10'), 0/0) as pm10_aqi,
        nullIf(avgIf(aqi_vn, parameter = 'co'),   0/0) as co_aqi,
        nullIf(avgIf(aqi_vn, parameter = 'no2'),  0/0) as no2_aqi,
        nullIf(avgIf(aqi_vn, parameter = 'so2'),  0/0) as so2_aqi,
        nullIf(avgIf(aqi_vn, parameter = 'o3'),   0/0) as o3_aqi,
        
        -- Dominant Pollutants
        argMax(parameter, aqi_us) as dominant_pollutant_us,
        argMax(parameter, aqi_vn) as dominant_pollutant_vn,
        max(ingest_time) as ingest_time,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
        
    from calculations
    where parameter in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
    group by 
        datetime_hour, 
        province, 
        ward_code, 
        source
)

select
    datetime_hour,
    date,
    province,
    ward_code,
    region_3,
    region_8,
    source,
    source_weight,
    final_aqi_us,
    final_aqi_vn,
    pm25_value,
    pm10_value,
    co_value,
    no2_value,
    so2_value,
    o3_value,
    pm25_aqi,
    pm10_aqi,
    co_aqi,
    no2_aqi,
    so2_aqi,
    o3_aqi,
    dominant_pollutant_us,
    dominant_pollutant_vn,
    ingest_time,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at
from pivoted
