{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864,
        'optimize_aggregation_in_order': 1
    }
) }}

with
{% if is_incremental() %}
{{ affected_hours_cte(ref('fct_air_quality_summary_hourly')) }},
{% endif %}
summary as (
    select
        s.datetime_hour,
        s.date,
        s.province,
        s.ward_code,
        s.region_3,
        s.region_8,
        s.source,
        s.source_weight,
        s.final_aqi_us,
        s.final_aqi_vn,
        s.pm25_value,
        s.pm10_value,
        s.co_value,
        s.no2_value,
        s.so2_value,
        s.o3_value,
        s.pm25_aqi,
        s.pm10_aqi,
        s.co_aqi,
        s.no2_aqi,
        s.so2_aqi,
        s.o3_aqi,
        s.ingest_time,
        s.raw_loaded_at,
        s.raw_sync_run_id,
        s.raw_sync_started_at
    from {{ ref('fct_air_quality_summary_hourly') }} s
    {% if is_incremental() %}
    inner join affected_hours h on s.datetime_hour = h.affected_hour
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
        
        max(ingest_time) as last_ingested_at,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
        
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
        c.datetime_hour,
        c.date,
        c.province,
        c.ward_code,
        c.region_3,
        c.region_8,
        c.avg_aqi_us,
        c.avg_aqi_vn,
        c.pm25_avg,
        c.pm10_avg,
        c.co_avg,
        c.no2_avg,
        c.so2_avg,
        c.o3_avg,
        c.pm25_aqi,
        c.pm10_aqi,
        c.co_aqi,
        c.no2_aqi,
        c.so2_aqi,
        c.o3_aqi,
        c.last_ingested_at,
        c.max_raw_loaded_at as raw_loaded_at,
        c.latest_raw_sync_run_id as raw_sync_run_id,
        c.latest_raw_sync_started_at as raw_sync_started_at,
        -- Use macro for dominant pollutant
        {{ get_main_pollutant('pm25_aqi', 'pm10_aqi', 'co_aqi', 'no2_aqi', 'so2_aqi', 'o3_aqi') }} as main_pollutant
    from consolidated c
)

select * from final
