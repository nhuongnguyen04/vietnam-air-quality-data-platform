{{ config(materialized='view') }}

with source as (
    select * from {{ source('openaq', 'raw_openaq_locations') }}
    where {{ filter_vietnam_openaq() }}
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by location_id
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
        
        -- Location info
        location_id,
        name as location_name,
        locality,
        timezone,
        
        -- Country info
        country_id,
        country_code,
        country_name,
        
        -- Owner and provider info
        owner_id,
        owner_name,
        provider_id,
        provider_name,
        
        -- Location type flags
        is_mobile,
        is_monitor,
        
        -- Coordinates (already Float64)
        latitude,
        longitude,
        
        -- Raw sensors
        raw_sensors,
        
        -- Datetime info
        datetime_first,
        datetime_last,
        
        -- Location hierarchy (extract from name/locality)
        case
            when locality is not null and locality != '' and locality != 'N/A' then locality
            when position(name, ',') > 0 then splitByString(',', name)[1]
            else name
        end as city,
        case
            when position(name, ',') > 1 then splitByString(',', name)[2]
            else null
        end as province,
        
        -- Flags
        true as is_vietnam,
        
        -- Location key (unique identifier for location)
        concat('OPENAQ_', toString(location_id)) as location_key,
        
        raw_payload
        
    from deduplicated
)

select * from transformed

