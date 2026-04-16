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
        -- Standardized ID format: openweather:LAT:LON 
        -- Using printf to ensure fixed precision (e.g. 21.0000) for consistent string joining
        concat(
            'openweather:', 
            printf('%.4f', latitude), 
            ':', 
            printf('%.4f', longitude)
        ) as station_name,
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
        source_district
    from source
),

geographic as (
    select
        r.station_name,
        r.latitude,
        r.longitude,
        r.station_source,
        case
            when r.station_source = 'openweather' then o.province
            else r.source_province
        end as province,
        case
            when r.station_source = 'openweather' then o.district
            else r.source_district
        end as district,
        case
            when r.station_source = 'openweather' then 'AutoCluster'
            else null
        end as ward
    from renamed r
    left join {{ ref('openweather_ingestion_points') }} o 
        -- Join using the regenerated standardized ID to ensure 1:1 metadata match
        on r.station_name = concat(
            'openweather:', 
            printf('%.4f', o.latitude), 
            ':', 
            printf('%.4f', o.longitude)
        )
),

normalized as (
    select
        station_name,
        latitude,
        longitude,
        station_source,
        ward,
        district,
        province
    from geographic
)

select * from normalized
