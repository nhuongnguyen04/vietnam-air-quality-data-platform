{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    engine='ReplacingMergeTree(ingest_time)',
    unique_key=['province', 'ward_code'],
    order_by='(province, assumeNotNull(ward_code))',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864,
        'max_memory_usage': 4294967296
    }
) }}

with hourly_aqi as (
    select * from {{ ref('fct_air_quality_ward_level_hourly') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

admin_units as (
    select 
        ward_code,
        latitude,
        longitude
    from {{ ref('dim_administrative_units') }}
),

latest_hourly as (
    select
        h.province,
        h.ward_code,
        argMax(h.region_3, h.datetime_hour) as region_3,
        argMax(h.region_8, h.datetime_hour) as region_8,
        argMax(h.datetime_hour, h.datetime_hour) as latest_hour,
        argMax(h.avg_aqi_us, h.datetime_hour) as current_aqi_us,
        argMax(h.avg_aqi_vn, h.datetime_hour) as current_aqi_vn,
        argMax(h.pm25_avg, h.datetime_hour) as pm25,
        argMax(h.pm10_avg, h.datetime_hour) as pm10,
        argMax(h.main_pollutant, h.datetime_hour) as main_pollutant,

        'consolidated' as data_source,
        argMax(h.last_ingested_at, h.datetime_hour) as ingest_time
    from hourly_aqi h
    group by
        h.province,
        h.ward_code
),

latest_records as (
    select
        h.province,
        h.ward_code,
        h.region_3,
        h.region_8,
        any(a.latitude) as latitude,
        any(a.longitude) as longitude,
        h.latest_hour,
        h.current_aqi_us,
        h.current_aqi_vn,
        h.pm25,
        h.pm10,
        h.main_pollutant,
        h.data_source,
        h.ingest_time
    from latest_hourly h
    left join admin_units a on h.ward_code = a.ward_code
    group by
        h.province,
        h.ward_code,
        h.region_3,
        h.region_8,
        h.latest_hour,
        h.current_aqi_us,
        h.current_aqi_vn,
        h.pm25,
        h.pm10,
        h.main_pollutant,
        h.data_source,
        h.ingest_time
)

select * from latest_records
