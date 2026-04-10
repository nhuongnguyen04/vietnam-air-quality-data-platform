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
        district,
        pm25_avg,
        pm10_avg,
        hourly_count
    from {{ ref('fct_air_quality_summary_daily') }}
    {% if is_incremental() %}
    -- Simple incremental for daily data
    where date >= (select max(date) from {{ this }})
    {% endif %}
),

compliance as (
    select
        date,
        province,
        district,
        pm25_avg,
        pm10_avg,
        
        -- WHO Standards (Daily)
        if(pm25_avg > 15, 1, 0) as who_pm25_breach,
        if(pm10_avg > 45, 1, 0) as who_pm10_breach,
        
        -- Vietnam National Standards (TCVN 05:2023 - Daily)
        if(pm25_avg > 50, 1, 0) as tcvn_pm25_breach,
        if(pm10_avg > 100, 1, 0) as tcvn_pm10_breach,
        
        case
            when pm25_avg > 50 then 'Unhealthy (TCVN Breach)'
            when pm25_avg > 15 then 'Warning (WHO Breach)'
            else 'Good/Safe'
        end as compliance_status
    from daily_data
)

select * from compliance
