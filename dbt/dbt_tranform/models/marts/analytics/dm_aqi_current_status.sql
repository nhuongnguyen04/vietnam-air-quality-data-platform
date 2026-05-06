{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code)',
    order_by='(province, assumeNotNull(ward_code))'
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

latest_records as (
    select
        h.province,
        h.ward_code,
        any(h.region_3) as region_3,
        any(h.region_8) as region_8,
        any(a.latitude) as latitude,
        any(a.longitude) as longitude,
        argMax(h.datetime_hour, h.datetime_hour) as latest_hour,
        argMax(h.avg_aqi_us, h.datetime_hour) as current_aqi_us,
        argMax(h.avg_aqi_vn, h.datetime_hour) as current_aqi_vn,
        argMax(h.pm25_avg, h.datetime_hour) as pm25,
        argMax(h.pm10_avg, h.datetime_hour) as pm10,
        argMax(h.main_pollutant, h.datetime_hour) as main_pollutant,

        'consolidated' as data_source,
        argMax(h.last_ingested_at, h.datetime_hour) as ingest_time
    from hourly_aqi h
    left join admin_units a on h.ward_code = a.ward_code
    group by
        h.province,
        h.ward_code
)

select * from latest_records
