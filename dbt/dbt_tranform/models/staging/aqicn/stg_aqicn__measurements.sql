{{ config(materialized='view') }}

with source as (
    select * from {{ source('aqicn', 'raw_aqicn_measurements') }}
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by station_id, time_v, pollutant 
                   order by ingest_time desc
               ) as rn
        from source
    )
    where rn = 1
),

transformed as (
    select
        -- Metadata
        source,
        ingest_time,
        ingest_batch_id,
        
        -- Station info
        station_id,
        
        -- Time information
        time_s,
        time_tz,
        time_v,
        time_iso,
        {{ parse_unix_timestamp('time_v') }} as measurement_datetime,
        {{ parse_iso_timestamp('time_iso') }} as measurement_datetime_iso,
        
        -- AQI
        toInt32OrNull(aqi) as aqi,
        dominentpol,
        
        -- Pollutant info
        {{ standardize_pollutant_name('pollutant') }} as pollutant,
        toFloat64OrNull(value) as value,
        
        -- Other fields
        attributions,
        debug_sync,
        
        -- Data quality score (based on completeness)
        case
            when value is not null and time_v is not null then 100
            when value is not null then 50
            else 25
        end as data_quality_score,
        
        raw_payload
        
    from deduplicated
)

select * from transformed
