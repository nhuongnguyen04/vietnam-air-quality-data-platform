{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)',
    tags=['pipeline_v2']
) }}

with daily_agg as (
    select
        date,
        province,
        ward_code,
        region_3,
        region_8,
        source_mix,
        any(confidence_score) as confidence_score,
        any(confidence_level) as confidence_level,

        avg(avg_aqi_us) as avg_aqi_us_agg,
        max(avg_aqi_us) as max_aqi_us_agg,
        min(avg_aqi_us) as min_aqi_us_agg,

        avg(avg_aqi_vn) as avg_aqi_vn_agg,
        max(avg_aqi_vn) as max_aqi_vn_agg,
        min(avg_aqi_vn) as min_aqi_vn_agg,

        -- Concentrations
        avg(pm25_avg) as pm25_avg_agg,
        avg(pm10_avg) as pm10_avg_agg,
        avg(co_avg)   as co_avg_agg,
        avg(no2_avg)  as no2_avg_agg,
        avg(so2_avg)  as so2_avg_agg,
        avg(o3_avg)   as o3_avg_agg,

        -- Daily average sub-AQIs
        avg(pm25_aqi) as pm25_aqi_agg,
        avg(pm10_aqi) as pm10_aqi_agg,
        avg(co_aqi)   as co_aqi_agg,
        avg(no2_aqi)  as no2_aqi_agg,
        avg(so2_aqi)  as so2_aqi_agg,
        avg(o3_aqi)   as o3_aqi_agg,

        sum(aqiin_observation_count) as aqiin_observation_count_agg,
        sum(openweather_observation_count) as openweather_observation_count_agg,

        max(last_ingested_at)                       as last_ingested_at_agg,
        max(raw_loaded_at)                          as raw_loaded_at_agg,
        argMax(raw_sync_run_id, raw_loaded_at)      as raw_sync_run_id_agg,
        argMax(raw_sync_started_at, raw_loaded_at)  as raw_sync_started_at_agg

    from {{ ref('fct_air_quality_ward_level_hourly') }}
    group by
        date,
        province,
        ward_code,
        region_3,
        region_8,
        source_mix
)
select
    date,
    province,
    ward_code,
    region_3,
    region_8,
    avg_aqi_us_agg as avg_aqi_us,
    max_aqi_us_agg as max_aqi_us,
    min_aqi_us_agg as min_aqi_us,
    avg_aqi_vn_agg as avg_aqi_vn,
    max_aqi_vn_agg as max_aqi_vn,
    min_aqi_vn_agg as min_aqi_vn,
    pm25_avg_agg as pm25_avg,
    pm10_avg_agg as pm10_avg,
    co_avg_agg as co_avg,
    no2_avg_agg as no2_avg,
    so2_avg_agg as so2_avg,
    o3_avg_agg as o3_avg,
    pm25_aqi_agg as pm25_aqi,
    pm10_aqi_agg as pm10_aqi,
    co_aqi_agg as co_aqi,
    no2_aqi_agg as no2_aqi,
    so2_aqi_agg as so2_aqi,
    o3_aqi_agg as o3_aqi,
    aqiin_observation_count_agg as aqiin_observation_count,
    openweather_observation_count_agg as openweather_observation_count,
    source_mix,
    confidence_score,
    confidence_level,
    last_ingested_at_agg as last_ingested_at,
    raw_loaded_at_agg as raw_loaded_at,
    raw_sync_run_id_agg as raw_sync_run_id,
    raw_sync_started_at_agg as raw_sync_started_at,
    {{ get_main_pollutant('pm25_aqi_agg', 'pm10_aqi_agg', 'co_aqi_agg', 'no2_aqi_agg', 'so2_aqi_agg', 'o3_aqi_agg') }} as main_pollutant
from daily_agg

