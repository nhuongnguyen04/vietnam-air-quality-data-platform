{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, datetime_hour)',
    order_by='(province, date, datetime_hour)',
    partition_by='toYYYYMM(date)'
) }}

with window_bounds as (
    select
        {% if is_incremental() %}
        (select max(datetime_hour) - interval 1 day from {{ this }}) as window_start
        {% else %}
        toDateTime('1970-01-01 00:00:00') as window_start
        {% endif %}
),

province_coverage as (
    select
        province,
        count() as total_ward_count
    from {{ ref('dim_administrative_units') }}
    group by province
),

traffic_by_province_hour as (
    select
        t.timestamp_utc as datetime_hour,
        toDate(t.timestamp_utc) as date,
        coalesce(a.province, t.province_name) as province,
        any(coalesce(a.region_3, {{ get_vietnam_region_3('t.province_name') }})) as region_3,
        any(coalesce(a.region_8, {{ get_vietnam_region_8('t.province_name') }})) as region_8,
        avg(t.value) as avg_congestion,
        count() as traffic_sample_count,
        uniqExact(t.ward_code) as traffic_ward_count,
        uniqExactIf(t.ward_code, t.value > 0) as positive_traffic_ward_count,
        max(t.ingest_time) as last_traffic_ingested_at
    from {{ ref('stg_tomtom__flow') }} t
    left join {{ ref('dim_administrative_units') }} a
        on t.ward_code = a.ward_code
    where t.timestamp_utc >= (select window_start from window_bounds)
    group by
        datetime_hour,
        date,
        province
),

aqi_by_province_hour as (
    select
        datetime_hour,
        date,
        province,
        region_3,
        region_8,
        pm25_avg as pm25,
        pm10_avg as pm10,
        co_avg as co,
        last_ingested_at
    from {{ ref('fct_air_quality_province_level_hourly') }}
    where datetime_hour >= (select window_start from window_bounds)
),

joined as (
    select
        a.datetime_hour as datetime_hour,
        a.date as date,
        a.province as province,
        '' as ward_code,
        coalesce(t.region_3, a.region_3) as region_3,
        coalesce(t.region_8, a.region_8) as region_8,
        a.pm25,
        a.pm10,
        a.co,
        t.avg_congestion,
        t.avg_congestion as congestion_index,
        t.traffic_sample_count,
        t.traffic_ward_count,
        t.positive_traffic_ward_count,
        cast(
            t.traffic_ward_count / nullIf(pc.total_ward_count, 0)
            as Float32
        ) as traffic_coverage_ratio,
        cast(a.pm25 * t.avg_congestion as Float32) as traffic_pollution_impact_score,
        cast(0.0 as Float32) as traffic_contribution_pct,
        t.last_traffic_ingested_at,
        a.last_ingested_at
    from aqi_by_province_hour a
    inner join traffic_by_province_hour t
        on a.province = t.province
        and a.datetime_hour = t.datetime_hour
    left join province_coverage pc
        on a.province = pc.province
)

select * from joined
