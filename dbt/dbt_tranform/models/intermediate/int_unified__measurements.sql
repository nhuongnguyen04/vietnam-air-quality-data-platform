{{ config(materialized='view') }}

with aqicn as (
    select
        station_id,
        timestamp_utc,
        parameter,
        value,
        unit,
        quality_flag,
        aqi_reported,
        source,
        ingest_time
    from {{ ref('stg_aqicn__measurements') }}
),
openweather as (
    select
        station_id,
        timestamp_utc,
        parameter,
        value,
        unit,
        quality_flag,
        aqi_reported,
        source,
        ingest_time
    from {{ ref('stg_openweather__measurements') }}
),
sensorscm as (
    select
        station_id,
        timestamp_utc,
        parameter,
        value,
        unit,
        quality_flag,
        null::Nullable(Int32) as aqi_reported,
        source,
        ingest_time
    from {{ ref('stg_sensorscm__measurements') }}
)
select * from aqicn
union all
select * from openweather
union all
select * from sensorscm
where value is not null and timestamp_utc is not null
