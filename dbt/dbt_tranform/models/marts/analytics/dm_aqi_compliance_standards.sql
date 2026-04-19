{{ config(
    materialized='incremental',
    engine='MergeTree',
    order_by='(province, date)',
    partition_by='toYYYYMM(date)'
) }}

with daily_data as (
    select
        date,
        province,
        region_3,
        region_8,
        avg_aqi_us,
        avg_aqi_vn,
        pm25_avg,
        pm10_avg,
        last_ingested_at as ingest_time
    from {{ ref('fct_air_quality_province_level_daily') }}
    {% if is_incremental() %}
    -- Simple incremental for daily data
    where last_ingested_at > (select max(ingest_time) from {{ this }})
    {% endif %}
),

compliance as (
    select
        date,
        province,
        region_3,
        region_8,
        pm25_avg,
        pm10_avg,
        
        -- WHO Standards (Daily Limits: PM2.5=15, PM10=45)
        if(pm25_avg > 15, 1, 0) as who_pm25_breach,
        if(pm10_avg > 45, 1, 0) as who_pm10_breach,
        
        -- Vietnam National Standards (TCVN 05:2023 - Daily Limits: PM2.5=50, PM10=100)
        if(pm25_avg > 50, 1, 0) as tcvn_pm25_breach,
        if(pm10_avg > 100, 1, 0) as tcvn_pm10_breach,
        
        case
            when pm25_avg > 50 then 'Unhealthy (TCVN Breach)'
            when pm25_avg > 15 then 'Warning (WHO Breach)'
            else 'Good/Safe'
        end as compliance_status,

        ingest_time
    from daily_data
)

select * from compliance
