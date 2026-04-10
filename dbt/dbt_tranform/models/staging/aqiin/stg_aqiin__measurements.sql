{{ config(materialized='view') }}

with source as (
    select * from {{ source('aqiin', 'raw_aqiin_measurements') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by station_name, timestamp_utc, parameter
            order by ingest_time desc
        ) as rn
    from source
),

normalized as (
    select
        'aqiin' as source,
        station_name,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        unit,
        quality_flag,
        ingest_time
    from deduplicated
    where rn = 1
)

select * from normalized
