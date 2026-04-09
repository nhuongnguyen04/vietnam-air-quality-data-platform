-- D-AQI-02: Phase 6 — 2 nguồn: AQI.in + OpenWeather
{{ config(materialized='view') }}

with aqiin_stations as (
    select
        concat('AQIIN_', station_id)                    AS station_id,
        'aqiin'                                        AS source,
        station_name,
        ''                                              AS latitude,
        ''                                              AS longitude,
        province_clean                                  AS province,
        station_name                                    AS city,
        'community'                                     AS location_type,
        'community'                                     AS sensor_quality_tier,
        true                                            AS is_active,
        min(ingest_time)                               AS first_seen,
        max(ingest_time)                               AS last_seen
    from {{ ref('stg_aqiin__stations') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),
openweather_stations as (
    select
        station_id,                                        -- raw format: openweather:Da Nang:16.1:108.2
        'openweather'                                      AS source,
        city_name                                          AS station_name,
        latitude,
        longitude,
        city_name                                         AS province,
        city_name                                         AS city,
        'city_centroid'                                    AS location_type,
        'city_centroid'                                    AS sensor_quality_tier,
        true                                               AS is_active,
        min(ingest_time)                                  AS first_seen,
        max(ingest_time)                                  AS last_seen
    from {{ ref('stg_openweather__measurements') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)
select * from aqiin_stations
union all
select * from openweather_stations