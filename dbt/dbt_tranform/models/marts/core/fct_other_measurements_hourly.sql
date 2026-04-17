{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree',
    unique_key='(province, ward_code, datetime_hour, parameter)',
    order_by='(province, date, assumeNotNull(ward_code))',
    partition_by='toYYYYMM(date)'
) }}

with calculations as (
    select * from {{ ref('int_aqi__calculations') }}
    {% if is_incremental() %}
    where ingest_time > (select max(ingest_time) from {{ this }})
    {% endif %}
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
    ingest_time
from calculations
where parameter not in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
