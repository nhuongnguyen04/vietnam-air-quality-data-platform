{{ config(materialized='view') }}

-- D-AQI-02: Phase 6 — Thay AQICN + Sensors.Community bằng AQI.in + OpenWeather
-- Chỉ 2 nguồn: aqiin (~540 vị trí VN) + openweather (62 tỉnh/thành)

with aqiin as (
    select
        station_id,
        timestamp_utc,
        parameter_clean                                      AS parameter,
        value,
        unit,
        quality_flag,
        aqi_reported,
        source,
        ingest_time
    from {{ ref('stg_aqiin__measurements') }}
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
)
select * from aqiin
union all
select * from openweather
where value is not null and timestamp_utc is not null