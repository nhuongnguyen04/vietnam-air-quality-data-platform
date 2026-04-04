{{ config(materialized='view') }}

-- Vietnam filter: JOIN với raw_aqicn_stations để lấy city_location
-- raw_aqicn_measurements không lưu station_name (chỉ raw_aqicn_stations lưu)
-- Filter: city_location chứa 'Vietnam' HOẶC station_name chứa 'Vietnam'
with measurements as (
    select * from {{ source('aqicn', 'raw_aqicn_measurements') }}
),
stations as (
    select
        station_id,
        argMax(station_name, ingest_time)   as station_name,
        argMax(city_location, ingest_time)  as city_location,
        argMax(latitude, ingest_time)       as latitude,
        argMax(longitude, ingest_time)      as longitude
    from {{ source('aqicn', 'raw_aqicn_stations') }}
    group by station_id
),
joined as (
    select
        m.*,
        s.station_name    as station_name_join,
        s.city_location   as city_location_join,
        s.latitude        as latitude_join,
        s.longitude       as longitude_join
    from measurements m
    inner join stations s on m.station_id = s.station_id
),
filtered as (
    select * from joined
    where
        -- Vietnam filter: city_location hoặc station_name chứa 'Vietnam'
        (
            city_location_join like '%Vietnam%'
            or station_name_join like '%Vietnam%'
        )
),
canonical as (
    select
        'aqicn'                                                           AS source,
        concat('AQICN_', station_id)                                       AS station_id,
        station_name_join                                                 AS station_name,
        latitude_join                                                     AS latitude,
        longitude_join                                                    AS longitude,

        -- FIX: Timestamp timezone cho 2 loại station AQICN
        -- Numeric stations (1583, 8688...): time_v theo giờ local Vietnam (+07:00)
        --   → trừ 25200 giây (7 tiếng) để ra UTC đúng
        -- A-prefix stations (A573400...): time_v đã theo UTC
        --   → giữ nguyên
        -- Detection dựa trên time_tz:
        --   +07:00 → trừ 7 tiếng (Vietnam)
        --   +08:00 → trừ 8 tiếng (China/HK/Singapore)
        --   khác → giữ nguyên (UTC)
        case
            when time_tz = '+07:00' then toDateTime(toInt64OrNull(time_v) - 25200)
            when time_tz = '+08:00' then toDateTime(toInt64OrNull(time_v) - 28800)
            else toDateTime(toInt64OrNull(time_v))
        end                                                                 AS timestamp_utc,

        {{ standardize_pollutant_name('pollutant') }}                    AS parameter,
        toFloat64OrNull(value)                                             AS value,
        case
            when {{ standardize_pollutant_name('pollutant') }} in ('pm25', 'pm10') then 'µg/m³'
            when {{ standardize_pollutant_name('pollutant') }} in ('o3', 'no2', 'so2') then 'ppb'
            when {{ standardize_pollutant_name('pollutant') }} = 'co' then 'µg/m³'
            else 'unknown'
        end                                                                 AS unit,
        'valid'                                                             AS quality_flag,
        toInt32OrNull(aqi)                                                  AS aqi_reported,
        ingest_time,
        raw_payload
    from filtered
    where
        time_v IS NOT NULL
        and time_v != '0'
)
select * from canonical
