{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    incremental_strategy='delete_insert',
    engine='ReplacingMergeTree(raw_loaded_at)',
    unique_key=['ward_code', 'datetime_hour'],
    order_by='(province, date, assumeNotNull(ward_code), datetime_hour)',
    partition_by='toYYYYMM(date)',
    query_settings={
        'max_threads': 2,
        'join_use_nulls': 1
    }
) }}

with traffic_incremental as (
    select
        ward_code,
        province_name as province,
        timestamp_utc as datetime_hour,
        toDate(timestamp_utc) as date,
        value as congestion_index,
        quality_flag,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at
    from {{ ref('stg_tomtom__flow') }}
    where {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
),

traffic_dedup as (
    select
        ward_code,
        province,
        datetime_hour,
        date,
        avg(congestion_index) as congestion_index,
        any(quality_flag) as traffic_data_type,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as raw_sync_started_at
    from traffic_incremental
    group by
        ward_code,
        province,
        datetime_hour,
        date
)

select
    datetime_hour,
    date,
    assumeNotNull(province) as province,
    assumeNotNull(ward_code) as ward_code,
    cast(congestion_index as Nullable(Float64)) as congestion_index,
    cast(traffic_data_type as Nullable(String)) as traffic_data_type,
    
    now() as dbt_updated_at,
    max_raw_loaded_at as raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at
from traffic_dedup
where ward_code != ''
