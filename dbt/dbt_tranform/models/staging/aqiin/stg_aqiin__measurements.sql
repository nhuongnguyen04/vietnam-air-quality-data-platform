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

cleaned as (
    select * from deduplicated where rn = 1
),

parsed as (
    select
        *,
        -- Split name: "Ward, District, Province, Vietnam"
        splitByString(', ', station_name) as parts,
        length(parts) as parts_len
    from cleaned
),

geographic as (
    select
        parsed.*,
        -- Extract components based on length
        -- Usually: parts[1] = Ward, parts[2] = District, parts[3] = Province
        -- If length 2: parts[1] = Province, parts[2] = Vietnam
        -- If length 3: parts[1] = District, parts[2] = Province, parts[3] = Vietnam
        
        case
            when parts_len >= 4 then trim(parts[1])
            else NULL
        end as ward_raw,
        
        case
            when parts_len >= 4 then trim(parts[2])
            when parts_len = 3 then trim(parts[1])
            else NULL
        end as district_raw,
        
        case
            when parts_len >= 4 then trim(parts[3])
            when parts_len = 3 then trim(parts[2])
            when parts_len = 2 then trim(parts[1])
            else 'Unknown'
        end as province_raw
    from parsed
),

normalized as (
    select
        'aqiin' as source,
        station_name,
        timestamp_utc,
        parameter,
        value,
        aqi_reported,
        
        -- Clean up geographic names
        ward_raw as ward,
        
        replaceRegexpAll(
            replaceRegexpAll(
                replaceRegexpAll(district_raw, '(?i)Thanh pho ', ''),
                '(?i)District', ''
            ),
            '(?i)Thi xa ', ''
        ) as district,
        
        replaceRegexpAll(
            replaceRegexpAll(province_raw, '(?i) Province', ''),
            '(?i) Vietnam', ''
        ) as province,
        
        unit,
        quality_flag,
        ingest_time
    from geographic
)

select * from normalized
