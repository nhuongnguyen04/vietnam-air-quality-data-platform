{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, datetime_hour)',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with ward_hourly as (
    select * from {{ ref('fct_air_quality_ward_level_hourly') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
),

province_hourly as (
    select
        datetime_hour,
        date,
        province,
        region_3,
        region_8,
        
        -- Provincial average
        avg(avg_aqi_us) as avg_aqi_us,
        avg(avg_aqi_vn) as avg_aqi_vn,
        
        -- Concentrations
        avg(pm25_avg) as pm25_avg,
        avg(pm10_avg) as pm10_avg,
        avg(co_avg)   as co_avg,
        avg(no2_avg)  as no2_avg,
        avg(so2_avg)  as so2_avg,
        avg(o3_avg)   as o3_avg,

        -- Sub-AQIs
        avg(pm25_aqi) as pm25_aqi,
        avg(pm10_aqi) as pm10_aqi,
        avg(co_aqi)   as co_aqi,
        avg(no2_aqi)  as no2_aqi,
        avg(so2_aqi)  as so2_aqi,
        avg(o3_aqi)   as o3_aqi,
        
        max(last_ingested_at) as last_ingested_at
        
    from ward_hourly
    group by
        datetime_hour,
        date,
        province,
        region_3,
        region_8
),

final as (
    select
        *,
        -- Provincial main pollutant based on averaged sub-AQIs
        {{ get_main_pollutant('pm25_aqi', 'pm10_aqi', 'co_aqi', 'no2_aqi', 'so2_aqi', 'o3_aqi') }} as main_pollutant
    from province_hourly
)

select * from final
