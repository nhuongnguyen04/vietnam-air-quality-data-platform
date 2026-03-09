{{ config(materialized='view') }}

with aqicn_stations as (
    select
        'aqicn' as source_system,
        location_key as unified_location_id,
        station_id as source_location_id,
        station_name as location_name,
        latitude,
        longitude,
        province,
        city,
        location_type,
        aqi as current_aqi,
        station_datetime as last_update,
        ingest_time
    from {{ ref('stg_aqicn__stations') }}
),

openaq_locations as (
    select
        'openaq' as source_system,
        location_key as unified_location_id,
        toString(location_id) as source_location_id,
        location_name,
        latitude,
        longitude,
        province,
        city,
        case
            when is_mobile then 'Mobile'
            when is_monitor then 'Monitor'
            else 'Other'
        end as location_type,
        null as current_aqi,
        {{ parse_iso_timestamp('datetime_last') }} as last_update,
        ingest_time
    from {{ ref('stg_openaq__locations') }}
),

all_locations as (
    select * from aqicn_stations
    union all
    select * from openaq_locations
),

-- Deduplicate by coordinates (stations within 1km of each other)
with_distance as (
    select
        *,
        arrayJoin(
            arrayMap(
                x -> (x, greatCircleDistance(longitude, latitude, x.longitude, x.latitude)),
                arrayFilter(x -> x.unified_location_id != unified_location_id, all_locations)
            )
        ) as nearby_station
    from all_locations
),

deduplicated as (
    select
        unified_location_id,
        min(unified_location_id) over (
            partition by 
                round(latitude, 2),
                round(longitude, 2)
        ) as primary_location_id,
        source_system,
        source_location_id,
        location_name,
        latitude,
        longitude,
        province,
        city,
        location_type,
        current_aqi,
        last_update,
        ingest_time,
        arrayDistinct(groupArray(source_system) over (
            partition by 
                round(latitude, 2),
                round(longitude, 2)
        )) as source_systems
    from all_locations
)

select
    case
        when primary_location_id = unified_location_id then unified_location_id
        else primary_location_id
    end as unified_location_id,
    source_system,
    source_location_id,
    location_name,
    latitude,
    longitude,
    province,
    city,
    location_type,
    current_aqi,
    last_update,
    source_systems,
    ingest_time
from deduplicated
where primary_location_id = unified_location_id

