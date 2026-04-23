{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, month)',
    order_by='(province, month, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(month)'
) }}

with ward_daily as (
    select * from {{ ref('fct_air_quality_ward_level_daily') }}
    {% if is_incremental() %}
    where date >= toStartOfMonth(today()) - interval 1 month
    {% endif %}
),

monthly_agg as (
    select
        toStartOfMonth(date) as month,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        
        avg(avg_aqi_us) as _avg_aqi_us,
        max(max_aqi_us) as _max_aqi_us,
        min(min_aqi_us) as _min_aqi_us,
        
        avg(avg_aqi_vn) as _avg_aqi_vn,
        max(max_aqi_vn) as _max_aqi_vn,
        min(min_aqi_vn) as _min_aqi_vn,
        
        -- Concentrations
        avg(pm25_avg) as _pm25_avg,
        avg(pm10_avg) as _pm10_avg,
        avg(co_avg)   as _co_avg,
        avg(no2_avg)  as _no2_avg,
        avg(so2_avg)  as _so2_avg,
        avg(o3_avg)   as _o3_avg,

        -- Monthly average sub-AQIs
        avg(pm25_aqi) as _pm25_aqi,
        avg(pm10_aqi) as _pm10_aqi,
        avg(co_aqi)   as _co_aqi,
        avg(no2_aqi)  as _no2_aqi,
        avg(so2_aqi)  as _so2_aqi,
        avg(o3_aqi)   as _o3_aqi,
        
        count(*) as samples_count,
        max(last_ingested_at) as last_ingested_at
        
    from ward_daily
    group by month, province, ward_code
),

final as (
    select
        month, province, ward_code, region_3, region_8, samples_count, last_ingested_at,
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
        -- Monthly main pollutant
        {{ get_main_pollutant('_pm25_aqi', '_pm10_aqi', '_co_aqi', '_no2_aqi', '_so2_aqi', '_o3_aqi') }} as main_pollutant
    from monthly_agg
)

select * from final
