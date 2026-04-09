{{ config(materialized='view') }}

-- D-AQI-02: Phase 6 — AQI.in stations (no longer joined with AQICN + SensorsCM)
with aqiin as (
    select
        station_id,
        station_name,
        province_clean                                  AS province,
        province                                        AS raw_province,
        last_ingest_time                                AS last_ingest_time,
        total_ingestions
    from {{ ref('stg_aqiin__stations') }}
)
select * from aqiin