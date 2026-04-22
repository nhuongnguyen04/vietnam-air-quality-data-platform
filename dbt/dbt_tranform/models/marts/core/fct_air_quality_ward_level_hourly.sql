{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with summary as (
    select
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8,
        source,
        source_weight,
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
        ingest_time
    from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 24 hour from {{ this }})
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
        
        -- Standardized Weighted Average logic
        sum(final_aqi_us * source_weight) / nullIf(sum(source_weight), 0) as avg_aqi_us,
        sum(final_aqi_vn * source_weight) / nullIf(sum(source_weight), 0) as avg_aqi_vn,
        
        -- Concentrations (Standardized Names)
        sum(pm25_value * source_weight) / nullIf(sum(source_weight), 0) as pm25_avg,
        sum(pm10_value * source_weight) / nullIf(sum(source_weight), 0) as pm10_avg,
        sum(co_value   * source_weight) / nullIf(sum(source_weight), 0) as co_avg,
        sum(no2_value  * source_weight) / nullIf(sum(source_weight), 0) as no2_avg,
        sum(so2_value  * source_weight) / nullIf(sum(source_weight), 0) as so2_avg,
        sum(o3_value   * source_weight) / nullIf(sum(source_weight), 0) as o3_avg,

        -- Sub-AQIs (Standardized Names)
        sum(pm25_aqi * source_weight) / nullIf(sum(source_weight), 0) as pm25_aqi,
        sum(pm10_aqi * source_weight) / nullIf(sum(source_weight), 0) as pm10_aqi,
        sum(co_aqi   * source_weight) / nullIf(sum(source_weight), 0) as co_aqi,
        sum(no2_aqi  * source_weight) / nullIf(sum(source_weight), 0) as no2_aqi,
        sum(so2_aqi  * source_weight) / nullIf(sum(source_weight), 0) as so2_aqi,
        sum(o3_aqi   * source_weight) / nullIf(sum(source_weight), 0) as o3_aqi,
        
        max(ingest_time) as last_ingested_at
        
    from summary
    group by
        datetime_hour,
        date,
        province,
        ward_code,
        region_3,
        region_8
),

final as (
    select
        c.*,
        -- Use macro for dominant pollutant
        {{ get_main_pollutant('pm25_aqi', 'pm10_aqi', 'co_aqi', 'no2_aqi', 'so2_aqi', 'o3_aqi') }} as main_pollutant
    from consolidated c
)

select * from final
