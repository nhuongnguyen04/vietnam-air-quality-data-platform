{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, district, datetime_hour)',
    order_by='(province, date, assumeNotNull(district))',
    partition_by='toYYYYMM(date)'
) }}

with summary as (
    select * from {{ ref('fct_air_quality_summary_hourly') }}
    {% if is_incremental() %}
    where datetime_hour >= (select max(datetime_hour) - interval 1 day from {{ this }})
    {% endif %}
),

ranked_sources as (
    select
        *,
        -- Priority: aqiin (ground sensors) > openweather (models)
        case 
            when source = 'aqiin' then 1
            when source = 'openweather' then 2
            else 3
        end as source_priority
    from summary
),

consolidated as (
    select
        datetime_hour,
        date,
        province,
        district,
        region_3,
        region_8,
        
        -- Weighted Average logic (Ground=5, Model=1)
        sum(final_aqi_us * source_weight) / sum(source_weight) as avg_aqi_us,
        sum(final_aqi_vn * source_weight) / sum(source_weight) as avg_aqi_vn,
        
        -- Concentrations (Weighted)
        sum(pm25_value * source_weight) / sum(source_weight) as pm25_value,
        sum(pm10_value * source_weight) / sum(source_weight) as pm10_value,
        sum(co_value   * source_weight) / sum(source_weight) as co_value,
        sum(no2_value  * source_weight) / sum(source_weight) as no2_value,
        sum(so2_value  * source_weight) / sum(source_weight) as so2_value,
        sum(o3_value   * source_weight) / sum(source_weight) as o3_value,

        -- Sub-AQIs (Weighted)
        sum(pm25_aqi * source_weight) / sum(source_weight) as pm25_aqi,
        sum(pm10_aqi * source_weight) / sum(source_weight) as pm10_aqi,
        sum(co_aqi   * source_weight) / sum(source_weight) as co_aqi,
        sum(no2_aqi  * source_weight) / sum(source_weight) as no2_aqi,
        sum(so2_aqi  * source_weight) / sum(source_weight) as so2_aqi,
        sum(o3_aqi   * source_weight) / sum(source_weight) as o3_aqi,
        
        max(ingest_time) as last_ingested_at
        
    from summary
    group by
        datetime_hour,
        date,
        province,
        district,
        region_3,
        region_8
)

select * from consolidated
