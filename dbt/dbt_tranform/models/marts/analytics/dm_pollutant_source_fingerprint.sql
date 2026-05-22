{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete_insert',
    engine='ReplacingMergeTree(ingest_time)',
    unique_key=['province', 'ward_code', 'date', 'source_mix'],
    order_by='(province, date, ward_code, source_mix)',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 1,
        'max_block_size': 4096,
        'max_bytes_before_external_sort': 67108864,
        'max_bytes_before_external_group_by': 67108864,
        'optimize_aggregation_in_order': 1
    }
) }}

with hourly_data as (
    select
        date,
        province,
        ward_code,
        region_3,
        region_8,
        pm25_avg as pm25_value,
        pm10_avg as pm10_value,
        source_mix,
        confidence_score,
        confidence_level,
        last_ingested_at as ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ ref('fct_air_quality_ward_level_hourly') }}
    where pm25_avg > 0 and pm10_avg > 0
    and {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

source_calc as (
    select
        date,
        province,
        ward_code,
        any(region_3) as region_3,
        any(region_8) as region_8,
        avg(pm25_value) as pm25,
        avg(pm10_value) as pm10,
        avg(confidence_score) as confidence_score,
        topK(1)(confidence_level)[1] as confidence_level,
        source_mix,
        avg(pm25_value) / nullIf(avg(pm10_value), 0) as pm25_pm10_ratio,
        case
            when avg(pm25_value) / nullIf(avg(pm10_value), 0) > 0.6 then 'Combustion/Traffic'
            when avg(pm25_value) / nullIf(avg(pm10_value), 0) < 0.4 then 'Dust/Construction'
            else 'Mixed'
        end as probable_source,
        max(ingest_time) as ingest_time
    from hourly_data
    group by date, province, ward_code, source_mix
)

select
    date,
    province,
    ward_code,
    region_3,
    region_8,
    pm25,
    pm10,
    confidence_score,
    confidence_level,
    source_mix,
    pm25_pm10_ratio,
    probable_source,
    ingest_time
from source_calc
