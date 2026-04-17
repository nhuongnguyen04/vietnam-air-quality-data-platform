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
        region_3,
        region_8,
        
        avg(daily_avg_aqi_us) as monthly_avg_aqi_us,
        max(daily_max_aqi_us) as monthly_max_aqi_us,
        min(daily_min_aqi_us) as monthly_min_aqi_us,
        
        avg(daily_avg_aqi_vn) as monthly_avg_aqi_vn,
        max(daily_max_aqi_vn) as monthly_max_aqi_vn,
        min(daily_min_aqi_vn) as monthly_min_aqi_vn,
        
        avg(pm25_daily_avg) as pm25_monthly_avg,
        avg(pm10_daily_avg) as pm10_monthly_avg,
        avg(co_daily_avg)   as co_monthly_avg,
        avg(no2_daily_avg)  as no2_monthly_avg,
        avg(so2_daily_avg)  as so2_monthly_avg,
        avg(o3_daily_avg)   as o3_monthly_avg,

        -- Monthly average sub-AQIs
        avg(pm25_daily_aqi) as pm25_monthly_aqi,
        avg(pm10_daily_aqi) as pm10_monthly_aqi,
        avg(co_daily_aqi)   as co_monthly_aqi,
        avg(no2_daily_aqi)  as no2_monthly_aqi,
        avg(so2_daily_aqi)  as so2_monthly_aqi,
        avg(o3_daily_aqi)   as o3_monthly_aqi,
        
        count(*) as samples_count,
        max(last_ingested_at) as last_ingested_at
        
    from ward_daily
    group by 1, 2, 3, 4, 5
),

final as (
    select
        *,
        -- Monthly main pollutant
        case 
            when pm25_monthly_aqi >= pm10_monthly_aqi and pm25_monthly_aqi >= co_monthly_aqi and pm25_monthly_aqi >= no2_monthly_aqi and pm25_monthly_aqi >= so2_monthly_aqi and pm25_monthly_aqi >= o3_monthly_aqi then 'pm25'
            when pm10_monthly_aqi >= co_monthly_aqi and pm10_monthly_aqi >= no2_monthly_aqi and pm10_monthly_aqi >= so2_monthly_aqi and pm10_monthly_aqi >= o3_monthly_aqi then 'pm10'
            when co_monthly_aqi >= no2_monthly_aqi and co_monthly_aqi >= so2_monthly_aqi and co_monthly_aqi >= o3_monthly_aqi then 'co'
            when no2_monthly_aqi >= so2_monthly_aqi and no2_monthly_aqi >= so2_monthly_aqi and no2_monthly_aqi >= o3_monthly_aqi then 'no2'
            when so2_monthly_aqi >= o3_monthly_aqi then 'so2'
            else 'o3'
        end as main_pollutant
    from monthly_agg
)

select * from final
