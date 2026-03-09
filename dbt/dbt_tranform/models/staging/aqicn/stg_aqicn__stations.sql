{{ config(materialized='view') }}

with source as (
    select * from {{ source('aqicn', 'raw_aqicn_stations') }}
    where {{ filter_vietnam_aqicn() }}
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by station_id
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
        ifNull(station_name, '') as station_name,
        city_url,
        city_location,
        
        -- Location coordinates (convert from string to float)
        toFloat64OrNull(latitude) as latitude,
        toFloat64OrNull(longitude) as longitude,
        
        -- Location hierarchy (extract from station_name)
        splitByString('/', ifNull(station_name, ''))[1] as province,
        case
            when position(ifNull(station_name, ''), '/') > 0 
            then trim(both ' ' from replace(splitByString('/', ifNull(station_name, ''))[2], ', Vietnam', ''))
            else ifNull(station_name, '')
        end as city,
        case
            when position(ifNull(station_name, ''), 'KTTV') > 0 then 'KTTV'
            when position(ifNull(station_name, ''), 'KCN') > 0 then 'KCN'
            when position(ifNull(station_name, ''), 'Chi cục') > 0 then 'Chi cục BVMT'
            else 'Other'
        end as location_type,
        
        -- Time information
        {{ parse_iso_timestamp('station_time') }} as station_datetime,
        station_time as station_time_str,
        
        -- AQI
        toInt32OrNull(aqi) as aqi,
        
        -- Flags
        true as is_vietnam,
        
        -- Location key (unique identifier for location)
        concat('AQICN_', station_id) as location_key,
        
        raw_payload
        
    from deduplicated
)

select * from transformed
