{{ config(materialized='view') }}

with source as (
    select 
        station_name,
        latitude,
        longitude,
        source as station_source,
        province as source_province,
        district as source_district
    from {{ ref('unified_stations_metadata') }}
    
    union all
    
    select
        concat('openweather:', province, ':', toString(latitude), ':', toString(longitude)) as station_name,
        latitude,
        longitude,
        'openweather' as station_source,
        province as source_province,
        district as source_district
    from {{ ref('openweather_ingestion_points') }}
),

renamed as (
    select
        station_name,
        latitude,
        longitude,
        station_source,
        source_province,
        source_district,
        -- Parsing original station name if needed
        splitByString(', ', station_name) as parts,
        length(parts) as parts_len
    from source
),

geographic as (
    -- Strategy: Use station names for old stations, but join the new seed for OpenWeather points
    select
        r.station_name,
        r.latitude,
        r.longitude,
        r.station_source,
        case
            when r.station_source = 'openweather' then o.province
            when r.source_province is not null and r.source_province != '' then r.source_province
            when r.parts_len >= 4 then trim(r.parts[3])
            when r.parts_len = 3 then trim(r.parts[2])
            when r.parts_len = 2 then trim(r.parts[1])
            else 'Unknown'
        end as province,
        case
            when r.station_source = 'openweather' then o.district
            when r.source_district is not null and r.source_district != '' then r.source_district
            when r.parts_len >= 4 then trim(r.parts[2])
            when r.parts_len = 3 then trim(r.parts[1])
            else NULL
        end as district,
        case
            when r.station_source = 'openweather' then 'AutoCluster'
            when r.parts_len >= 4 then trim(r.parts[1])
            else NULL
        end as ward
    from renamed r
    left join {{ ref('openweather_ingestion_points') }} o 
        on r.station_name = concat('openweather:', o.province, ':', toString(o.latitude), ':', toString(o.longitude))
),

normalized as (
    select
        station_name,
        latitude,
        longitude,
        station_source,
        ward,
        -- We now rely on the Reverse Geocoding script to provide fully normalized names
        district,
        province
    from geographic
)

select * from normalized
