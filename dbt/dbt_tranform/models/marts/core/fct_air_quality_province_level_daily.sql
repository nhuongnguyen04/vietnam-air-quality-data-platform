{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, date)',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with province_hourly as (
    select * from {{ ref('fct_air_quality_province_level_hourly') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

daily_agg as (
    select
        date,
        province,
        any(region_3) as region_3,
        any(region_8) as region_8,
        
        avg(avg_aqi_us) as _avg_aqi_us,
        max(avg_aqi_us) as _max_aqi_us,
        min(avg_aqi_us) as _min_aqi_us,
        
        avg(avg_aqi_vn) as _avg_aqi_vn,
        max(avg_aqi_vn) as _max_aqi_vn,
        min(avg_aqi_vn) as _min_aqi_vn,
        
        -- Concentrations
        avg(pm25_avg) as _pm25_avg,
        avg(pm10_avg) as _pm10_avg,
        avg(co_avg)   as _co_avg,
        avg(no2_avg)  as _no2_avg,
        avg(so2_avg)  as _so2_avg,
        avg(o3_avg)   as _o3_avg,

        -- Daily average sub-AQIs
        avg(pm25_aqi) as _pm25_aqi,
        avg(pm10_aqi) as _pm10_aqi,
        avg(co_aqi)   as _co_aqi,
        avg(no2_aqi)  as _no2_aqi,
        avg(so2_aqi)  as _so2_aqi,
        avg(o3_aqi)   as _o3_aqi,
        
        max(last_ingested_at) as last_ingested_at,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
        
    from province_hourly
    group by date, province
),

final as (
    select
        date, province, region_3, region_8, last_ingested_at,
        max_raw_loaded_at as raw_loaded_at,
        latest_raw_sync_run_id as raw_sync_run_id,
        latest_raw_sync_started_at as raw_sync_started_at,
        _avg_aqi_us as avg_aqi_us,
        _max_aqi_us as max_aqi_us,
        _min_aqi_us as min_aqi_us,
        _avg_aqi_vn as avg_aqi_vn,
        _max_aqi_vn as max_aqi_vn,
        _min_aqi_vn as min_aqi_vn,
        _pm25_avg as pm25_avg,
        _pm10_avg as pm10_avg,
        _co_avg as co_avg,
        _no2_avg as no2_avg,
        _so2_avg as so2_avg,
        _o3_avg as o3_avg,
        _pm25_aqi as pm25_aqi,
        _pm10_aqi as pm10_aqi,
        _co_aqi as co_aqi,
        _no2_aqi as no2_aqi,
        _so2_aqi as so2_aqi,
        _o3_aqi as o3_aqi,
        -- Use macro for daily dominant pollutant
        {{ get_main_pollutant('_pm25_aqi', '_pm10_aqi', '_co_aqi', '_no2_aqi', '_so2_aqi', '_o3_aqi') }} as main_pollutant
    from daily_agg
)

select * from final
