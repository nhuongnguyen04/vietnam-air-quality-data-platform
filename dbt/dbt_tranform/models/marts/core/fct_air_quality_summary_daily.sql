{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, date)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with hourly_summary as (
    select 
        date,
        province,
        ward_code,
        region_3,
        region_8,
        source,
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
        dominant_pollutant_vn
    from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    where date >= (select max(date) - interval 2 day from {{ this }})
    {% endif %}
)

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
    
    count(*) as hourly_count
    
from hourly_summary
group by date, province, ward_code, source
