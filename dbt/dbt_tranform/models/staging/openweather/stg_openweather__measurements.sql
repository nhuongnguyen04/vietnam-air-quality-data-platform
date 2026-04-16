{{ config(materialized='view') }}

with source as (
    select * from {{ source('openweather', 'raw_openweather_measurements') }}
    -- Filter malformed station_ids (e.g. 'openweather:openweather:...' from upstream bug)
    where not station_id like 'openweather:openweather:%'
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by
                -- Deduplicate by normalized location key (lat/lon), not raw station_id
                -- This handles multiple station_id formats for the same location
                printf('%.4f', toFloat64OrNull(
                    splitByChar(':', station_id)[length(splitByChar(':', station_id)) - 1]
                )),
                printf('%.4f', toFloat64OrNull(
                    splitByChar(':', station_id)[length(splitByChar(':', station_id))]
                )),
                timestamp_utc,
                parameter
            order by ingest_time desc
        ) as rn
    from source
),

parts_extracted as (
    -- Extract coordinate parts first to keep the main query clean
    select
        *,
        splitByChar(':', station_id) as _parts
    from deduplicated
    where rn = 1
),

cleaned as (
    select
        'openweather' as source,
        -- Standardize station_name format to: openweather:LAT:LON
        -- Using printf('%.4f') to ensure bit-perfect string matching with stg_core__stations
        case
            when length(_parts) >= 2 then
                concat(
                    'openweather:',
                    printf('%.4f', toFloat64OrNull(_parts[length(_parts)-1])),
                    ':',
                    printf('%.4f', toFloat64OrNull(_parts[length(_parts)]))
                )
            else station_id
        end as station_name,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        'valid' as quality_flag,
        ingest_time
    from parts_extracted
)

select * from cleaned
