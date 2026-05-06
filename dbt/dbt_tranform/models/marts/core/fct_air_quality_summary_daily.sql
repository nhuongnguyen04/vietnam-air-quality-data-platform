{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, date)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864
    }
) }}

with
{% if is_incremental() %}
{{ affected_dates_cte(ref('fct_air_quality_summary_hourly')) }},
{% endif %}
hourly_summary as (
    select
        h.date,
        h.province,
        h.ward_code,
        h.region_3,
        h.region_8,
        h.source,
        h.final_aqi_us,
        h.final_aqi_vn,
        h.pm25_value,
        h.pm10_value,
        h.co_value,
        h.no2_value,
        h.so2_value,
        h.o3_value,
        h.pm25_aqi,
        h.pm10_aqi,
        h.co_aqi,
        h.no2_aqi,
        h.so2_aqi,
        h.o3_aqi,
        h.dominant_pollutant_us,
        h.dominant_pollutant_vn,
        h.ingest_time,
        h.raw_loaded_at,
        h.raw_sync_run_id,
        h.raw_sync_started_at
    from {{ ref('fct_air_quality_summary_hourly') }} h
    {% if is_incremental() %}
    inner join affected_dates d on h.date = d.affected_date
    {% endif %}
),

daily_aggregated as (
    select
        date,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        source,
        
        -- Overall AQIs (Avg and Max of the day)
        round(avg(final_aqi_us), 2) as daily_avg_aqi_us,
        round(avg(final_aqi_vn), 2) as daily_avg_aqi_vn,
        max(final_aqi_us) as daily_max_aqi_us,
        max(final_aqi_vn) as daily_max_aqi_vn,
        
        -- Concentrations (Daily Averages)
        round(avg(pm25_value), 2) as pm25_daily_avg,
        round(avg(pm10_value), 2) as pm10_daily_avg,
        round(avg(co_value), 2)   as co_daily_avg,
        round(avg(no2_value), 2)  as no2_daily_avg,
        round(avg(so2_value), 2)  as so2_daily_avg,
        round(avg(o3_value), 2)   as o3_daily_avg,
        
        -- Sub-AQI Indices (Daily Averages)
        round(avg(pm25_aqi), 1)  as pm25_daily_aqi,
        round(avg(pm10_aqi), 1)  as pm10_daily_aqi,
        round(avg(co_aqi), 1)    as co_daily_aqi,
        round(avg(no2_aqi), 1)   as no2_daily_aqi,
        round(avg(so2_aqi), 1)   as so2_daily_aqi,
        round(avg(o3_aqi), 1)    as o3_daily_aqi,
        
        -- Max Concentrations
        max(pm25_value) as pm25_daily_max,
        max(pm10_value) as pm10_daily_max,
        
        -- Dominant Pollutant for the day (based on highest hourly AQI)
        argMax(dominant_pollutant_us, final_aqi_us) as dominant_pollutant_us,
        argMax(dominant_pollutant_vn, final_aqi_vn) as dominant_pollutant_vn,
        
        count(*) as hourly_count,
        max(ingest_time) as ingest_time,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
        
    from hourly_summary
    group by date, province, ward_code, source
)

select
    date,
    province,
    ward_code,
    region_3,
    region_8,
    source,
    daily_avg_aqi_us,
    daily_avg_aqi_vn,
    daily_max_aqi_us,
    daily_max_aqi_vn,
    pm25_daily_avg,
    pm10_daily_avg,
    co_daily_avg,
    no2_daily_avg,
    so2_daily_avg,
    o3_daily_avg,
    pm25_daily_aqi,
    pm10_daily_aqi,
    co_daily_aqi,
    no2_daily_aqi,
    so2_daily_aqi,
    o3_daily_aqi,
    pm25_daily_max,
    pm10_daily_max,
    dominant_pollutant_us,
    dominant_pollutant_vn,
    hourly_count,
    ingest_time,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at
from daily_aggregated
