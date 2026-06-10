{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    engine='ReplacingMergeTree',
    unique_key='(province, datetime_hour, source_mix)',
    order_by='(province, date, source_mix, datetime_hour)',
    partition_by='toYYYYMM(date)'
) }}

with
{% if is_incremental() %}
affected_hours as (
    select distinct timestamp_utc as affected_hour
    from {{ ref('stg_tomtom__flow') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
    
    union distinct
    
    select distinct datetime_hour as affected_hour
    from {{ ref('fct_air_quality_province_level_hourly') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),
{% endif %}

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
    where 1 = 1
    {% if is_incremental() %}
      and t.timestamp_utc in (select affected_hour from affected_hours)
    {% endif %}
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
        source_mix,
        confidence_score,
        confidence_level,
        last_ingested_at
    from {{ ref('fct_air_quality_province_level_hourly') }}
    where 1 = 1
    {% if is_incremental() %}
      and datetime_hour in (select affected_hour from affected_hours)
    {% endif %}
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
        a.source_mix,
        a.confidence_score,
        a.confidence_level,
        t.avg_congestion,
        t.avg_congestion as congestion_index,
        t.traffic_sample_count,
        t.traffic_ward_count,
        t.positive_traffic_ward_count,
        cast(
            t.traffic_ward_count / nullIf(pc.total_ward_count, 0)
            as Nullable(Float32)
        ) as traffic_coverage_ratio,
        cast(a.pm25 * t.avg_congestion as Nullable(Float32)) as traffic_pollution_impact_score,
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
