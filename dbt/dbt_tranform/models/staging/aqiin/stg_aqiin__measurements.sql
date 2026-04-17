{{ config(
    engine='ReplacingMergeTree',
    unique_key='(station_name, timestamp_utc, parameter)',
    order_by='(timestamp_utc, station_name, parameter)',
    partition_by='toYYYYMM(timestamp_utc)'
) }}

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
        -- Standardize units to µg/m³ at 25°C, 1 atm
        case 
            when unit = 'ppb' then 
                case 
                    when parameter = 'o3' then value * 1.96
                    when parameter = 'no2' then value * 1.88
                    when parameter = 'so2' then value * 2.62
                    else value 
                end
            when unit = 'ppm' and parameter = 'co' then value * 1145
            else value 
        end as value,
        aqi_reported,
        'µg/m³' as unit, -- Standardized unit
        quality_flag,
        ingest_time
    from deduplicated
    where rn = 1
)

select * from normalized
