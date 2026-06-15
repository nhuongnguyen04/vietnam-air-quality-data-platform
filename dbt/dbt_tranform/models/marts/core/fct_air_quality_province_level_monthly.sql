{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(province, month)',
    partition_by='toYYYYMM(month)',
    tags=['pipeline_v2']
) }}

with province_monthly as (
    select
        month,
        province,
        region_3,
        region_8,
        source_mix,
        any(confidence_score) as confidence_score,
        any(confidence_level) as confidence_level,

        -- Provincial average
        avg(avg_aqi_us) as avg_aqi_us_agg,
        max(max_aqi_us) as max_aqi_us_agg,
        min(min_aqi_us) as min_aqi_us_agg,
        avg(avg_aqi_vn) as avg_aqi_vn_agg,
        max(max_aqi_vn) as max_aqi_vn_agg,
        min(min_aqi_vn) as min_aqi_vn_agg,

        -- Concentrations
        avg(pm25_avg) as pm25_avg_agg,
        avg(pm10_avg) as pm10_avg_agg,
        avg(co_avg)   as co_avg_agg,
        avg(no2_avg)  as no2_avg_agg,
        avg(so2_avg)  as so2_avg_agg,
        avg(o3_avg)   as o3_avg_agg,

        -- Sub-AQIs
        avg(pm25_aqi) as pm25_aqi_agg,
        avg(pm10_aqi) as pm10_aqi_agg,
        avg(co_aqi)   as co_aqi_agg,
        avg(no2_aqi)  as no2_aqi_agg,
        avg(so2_aqi)  as so2_aqi_agg,
        avg(o3_aqi)   as o3_aqi_agg,

        count(distinct ward_code) as total_ward_count_agg,
        sum(aqiin_observation_count + openweather_observation_count) as observation_count_agg,

        max(last_ingested_at)                       as last_ingested_at_agg,
        max(raw_loaded_at)                          as raw_loaded_at_agg,
        argMax(raw_sync_run_id, raw_loaded_at)      as raw_sync_run_id_agg,
        argMax(raw_sync_started_at, raw_loaded_at)  as raw_sync_started_at_agg

    from {{ ref('fct_air_quality_ward_level_monthly') }}
    group by
        month,
        province,
        region_3,
        region_8,
        source_mix
)
select
    month,
    province,
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
    total_ward_count_agg as total_ward_count,
    
    -- Ward/Observation counts mapped per source mix
    if(source_mix = 'observed', total_ward_count_agg, toUInt64(0)) as aqiin_ward_count,
    if(source_mix = 'modeled', total_ward_count_agg, toUInt64(0)) as openweather_ward_count,
    if(source_mix = 'observed', observation_count_agg, toUInt64(0)) as aqiin_observation_count,
    if(source_mix = 'modeled', observation_count_agg, toUInt64(0)) as openweather_observation_count,
    
    source_mix,
    confidence_score,
    confidence_level,
    last_ingested_at_agg as last_ingested_at,
    raw_loaded_at_agg as raw_loaded_at,
    raw_sync_run_id_agg as raw_sync_run_id,
    raw_sync_started_at_agg as raw_sync_started_at,
    {{ get_main_pollutant('pm25_aqi_agg', 'pm10_aqi_agg', 'co_aqi_agg', 'no2_aqi_agg', 'so2_aqi_agg', 'o3_aqi_agg') }} as main_pollutant
from province_monthly

