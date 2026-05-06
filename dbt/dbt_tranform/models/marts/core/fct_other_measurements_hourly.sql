{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour, parameter)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with calculations as (
    select
        source,
        ward_code,
        province,
        timestamp_utc,
        parameter,
        value,
        quality_flag,
        ingest_time,
        raw_loaded_at,
        raw_sync_run_id,
        raw_sync_started_at,
        region_3,
        region_8
    from {{ ref('int_core__measurements_unified') }}
    where parameter not in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
      and {{ downstream_incremental_predicate('raw_sync_run_id', 'raw_loaded_at') }}
)

select
    timestamp_utc as datetime_hour,
    toDate(timestamp_utc) as date,
    province,
    ward_code,
    region_3,
    region_8,
    parameter,
    value,
    source,
    quality_flag,
    ingest_time,
    raw_loaded_at,
    raw_sync_run_id,
    raw_sync_started_at
from calculations
