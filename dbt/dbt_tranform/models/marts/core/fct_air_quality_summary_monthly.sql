{{ config(
    materialized='view',
    tags=['pipeline_v2']
) }}

select
    month,
    province,
    ward_code,
    region_3,
    region_8,
    'aqiin' as source,
    avg_aqi_us as monthly_avg_aqi_us,
    avg_aqi_vn as monthly_avg_aqi_vn,
    max_aqi_us as monthly_max_aqi_us,
    min_aqi_us as monthly_min_aqi_us,
    pm25_avg as pm25_monthly_avg,
    pm10_avg as pm10_monthly_avg,
    co_avg as co_monthly_avg,
    no2_avg as no2_monthly_avg,
    so2_avg as so2_monthly_avg,
    o3_avg as o3_monthly_avg,
    main_pollutant as dominant_pollutant_vn,
    last_ingested_at as ingest_time,
    raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at
from {{ ref('int_aqiin__ward_monthly') }}

union all

select
    month,
    province,
    ward_code,
    region_3,
    region_8,
    'openweather' as source,
    avg_aqi_us as monthly_avg_aqi_us,
    avg_aqi_vn as monthly_avg_aqi_vn,
    max_aqi_us as monthly_max_aqi_us,
    min_aqi_us as monthly_min_aqi_us,
    pm25_avg as pm25_monthly_avg,
    pm10_avg as pm10_monthly_avg,
    co_avg as co_monthly_avg,
    no2_avg as no2_monthly_avg,
    so2_avg as so2_monthly_avg,
    o3_avg as o3_monthly_avg,
    main_pollutant as dominant_pollutant_vn,
    last_ingested_at as ingest_time,
    raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at
from {{ ref('int_ow__ward_monthly') }}
