{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with summary as (
    select * from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
),

consolidated as (
    select
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        
        -- Weighted Average logic (Ground=5, Model=1)
        sum(final_aqi_us * source_weight) / sum(source_weight) as hourly_avg_aqi_us,
        sum(final_aqi_vn * source_weight) / sum(source_weight) as hourly_avg_aqi_vn,
        
        -- Concentrations (Weighted)
        sum(pm25_value * source_weight) / sum(source_weight) as pm25_hourly_avg,
        sum(pm10_value * source_weight) / sum(source_weight) as pm10_hourly_avg,
        sum(co_value   * source_weight) / sum(source_weight) as co_hourly_avg,
        sum(no2_value  * source_weight) / sum(source_weight) as no2_hourly_avg,
        sum(so2_value  * source_weight) / sum(source_weight) as so2_hourly_avg,
        sum(o3_value   * source_weight) / sum(source_weight) as o3_hourly_avg,

        -- Sub-AQIs (Weighted)
        sum(pm25_aqi * source_weight) / sum(source_weight) as pm25_hourly_aqi,
        sum(pm10_aqi * source_weight) / sum(source_weight) as pm10_hourly_aqi,
        sum(co_aqi   * source_weight) / sum(source_weight) as co_hourly_aqi,
        sum(no2_aqi  * source_weight) / sum(source_weight) as no2_hourly_aqi,
        sum(so2_aqi  * source_weight) / sum(source_weight) as so2_hourly_aqi,
        sum(o3_aqi   * source_weight) / sum(source_weight) as o3_hourly_aqi,
        
        max(ingest_time) as last_ingested_at
        
    from summary
    group by
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8
)

select * from consolidated
