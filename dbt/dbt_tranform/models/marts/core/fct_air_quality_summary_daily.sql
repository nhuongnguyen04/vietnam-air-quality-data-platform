{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

select
    date,
    province,
    ward_code,
    region_3,
    region_8,
    if(source_mix = 'observed', 'aqiin', 'openweather') as source,
    avg_aqi_us as daily_avg_aqi_us,
    avg_aqi_vn as daily_avg_aqi_vn,
    max_aqi_us as daily_max_aqi_us,
    max_aqi_vn as daily_max_aqi_vn,
    pm25_avg as pm25_daily_avg,
    pm10_avg as pm10_daily_avg,
    co_avg as co_daily_avg,
    no2_avg as no2_daily_avg,
    so2_avg as so2_daily_avg,
    o3_avg as o3_daily_avg,
    pm25_aqi as pm25_daily_aqi,
    pm10_aqi as pm10_daily_aqi,
    co_aqi as co_daily_aqi,
    no2_aqi as no2_daily_aqi,
    so2_aqi as so2_daily_aqi,
    o3_aqi as o3_daily_aqi,
    pm25_avg as pm25_daily_max,
    pm10_avg as pm10_daily_max,
    main_pollutant as dominant_pollutant_us,
    main_pollutant as dominant_pollutant_vn,
    if(source_mix = 'observed', aqiin_observation_count, openweather_observation_count) as hourly_count,
    last_ingested_at as ingest_time,
    raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at
from {{ ref('fct_air_quality_ward_level_daily') }}

