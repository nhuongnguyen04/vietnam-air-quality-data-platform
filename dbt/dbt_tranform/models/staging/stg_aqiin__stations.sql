{{ config(materialized='view') }}

-- AQI.in stations: deduplicated station metadata
-- Use latest record per station_id

with stations as (
    select * from {{ source('aqiin', 'raw_aqiin_stations') }}
),
deduped as (
    select
        station_id,
        argMax(station_name, ingest_time)          as station_name,
        argMax(province, ingest_time)              as province,
        argMax(url, ingest_time)                   as source_url,
        argMax(ingest_time, ingest_time)           as last_ingest_time,
        count(*)                                   as total_ingestions
    from stations
    group by station_id
),
with_slug as (
    select
        station_id,
        station_name,
        province,

        -- Normalize province name (remove '-province' suffix)
        case
            when province like '%province' then substring(province, 1, length(province) - 9)
            else province
        end                                        as province_clean,

        source_url,
        last_ingest_time,
        total_ingestions
    from deduped
)
select * from with_slug