{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    engine='ReplacingMergeTree(ingest_time)',
    unique_key=['province', 'date'],
    order_by='(province, date)',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864,
        'optimize_aggregation_in_order': 1
    }
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
        last_ingested_at as ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ ref('fct_air_quality_province_level_daily') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

compliance as (
    select
        date,
        province,
        region_3,
        region_8,
        pm25_avg,
        pm10_avg,
        
        -- WHO Standards (Daily Limits: PM2.5=15 µg/m³, PM10=45 µg/m³)
        -- Ref: WHO Global Air Quality Guidelines 2021
        -- Note: WHO annual guideline is 5 µg/m³ (not checked here — daily only)
        if(pm25_avg > 15, 1, 0) as who_pm25_breach,
        if(pm10_avg > 45, 1, 0) as who_pm10_breach,
        
        -- Vietnam National Standards (QCVN 05:2023/BTNMT - Daily Limits)
        -- PM2.5 daily: 50 µg/Nm³ | PM10 daily: 100 µg/Nm³
        -- Note: From 01/01/2026, annual PM2.5 limit tightened to 45 µg/Nm³
        --        (but this model checks daily thresholds only)
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
