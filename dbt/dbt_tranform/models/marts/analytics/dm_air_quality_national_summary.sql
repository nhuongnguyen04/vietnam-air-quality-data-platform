{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(time_grain, timestamp)',
    order_by='(time_grain, timestamp)'
) }}

with hourly_source as (
    select
        'hourly' as time_grain,
        datetime_hour as timestamp,
        avg_aqi_us as h_aqi_us,
        avg_aqi_vn as h_aqi_vn,
        main_pollutant as h_pollutant,
        province,
        last_ingested_at
    from {{ ref('fct_air_quality_province_level_hourly') }}
),

hourly_agg as (
    select
        time_grain,
        timestamp,
        avg(h_aqi_us) as avg_aqi_us,
        max(h_aqi_us) as max_aqi_us,
        avg(h_aqi_vn) as avg_aqi_vn,
        max(h_aqi_vn) as max_aqi_vn,
        count(distinct province) as active_provinces,
        0 as active_wards,
        argMax(h_pollutant, h_aqi_vn) as dominant_pollutant,
        max(last_ingested_at) as last_ingested_at
    from hourly_source
    group by 1, 2
),

daily_source as (
    select
        'daily' as time_grain,
        toStartOfDay(date) as timestamp,
        avg_aqi_us as d_aqi_us,
        max_aqi_us as d_max_aqi_us,
        avg_aqi_vn as d_aqi_vn,
        max_aqi_vn as d_max_aqi_vn,
        main_pollutant as d_pollutant,
        province,
        last_ingested_at
    from {{ ref('fct_air_quality_province_level_daily') }}
),

daily_agg as (
    select
        time_grain,
        timestamp,
        avg(d_aqi_us) as avg_aqi_us,
        max(d_max_aqi_us) as max_aqi_us,
        avg(d_aqi_vn) as avg_aqi_vn,
        max(d_max_aqi_vn) as max_aqi_vn,
        count(distinct province) as active_provinces,
        0 as active_wards,
        argMax(d_pollutant, d_aqi_vn) as dominant_pollutant,
        max(last_ingested_at) as last_ingested_at
    from daily_source
    group by 1, 2
),

final as (
    select * from hourly_agg
    union all
    select * from daily_agg
)

select * from final
