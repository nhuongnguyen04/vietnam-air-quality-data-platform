{{ config(materialized='view') }}

with source as (
    select * from {{ ref('unified_stations_metadata') }}
),

renamed as (
    select
        station_name,
        latitude,
        longitude,
        source as station_source,
        -- Parsing original station name if needed (similar to old stg_aqiin__measurements)
        splitByString(', ', station_name) as parts,
        length(parts) as parts_len
    from source
),

geographic as (
    select
        *,
        case
            when parts_len >= 4 then trim(parts[1])
            else NULL
        end as ward,
        
        case
            when parts_len >= 4 then trim(parts[2])
            when parts_len = 3 then trim(parts[1])
            else NULL
        end as district,
        
        case
            when parts_len >= 4 then trim(parts[3])
            when parts_len = 3 then trim(parts[2])
            when parts_len = 2 then trim(parts[1])
            else 'Unknown'
        end as province
    from renamed
),

normalized as (
    select
        station_name,
        latitude,
        longitude,
        station_source,
        ward,
        replaceRegexpAll(
            replaceRegexpAll(
                replaceRegexpAll(district, '(?i)Thanh pho ', ''),
                '(?i)District', ''
            ),
            '(?i)Thi xa ', ''
        ) as district,
        replaceRegexpAll(
            replaceRegexpAll(province, '(?i) Province', ''),
            '(?i) Vietnam', ''
        ) as province
    from geographic
)

select * from normalized
