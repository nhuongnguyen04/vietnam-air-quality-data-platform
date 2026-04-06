-- depends_on: {{ ref('stg_aqicn__stations') }}
-- depends_on: {{ ref('stg_openweather__measurements') }}
-- depends_on: {{ ref('stg_sensorscm__measurements') }}
{{ config(materialized='view') }}

with aqicn_stations as (
    select
        concat('AQICN_', station_id)   AS station_id,
        'aqicn'                        AS source,
        station_name,
        latitude,
        longitude,
        province,
        city,
        location_type,
        'research'                      AS sensor_quality_tier,
        true                           AS is_active,
        min(ingest_time)               AS first_seen,
        max(ingest_time)               AS last_seen
    from {{ ref('stg_aqicn__stations') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8
),
openweather_stations as (
    select
        station_id,                                -- raw format: openweather:Da Nang:16.1:108.2
        'openweather'                             AS source,
        city_name                                 AS station_name,
        latitude,
        longitude,
        city_name                                 AS province,
        city_name                                 AS city,
        'city_centroid'                           AS location_type,
        'city_centroid'                           AS sensor_quality_tier,
        true                                      AS is_active,
        min(ingest_time)                          AS first_seen,
        max(ingest_time)                          AS last_seen
    from {{ ref('stg_openweather__measurements') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8
),
sensorscm_stations as (
    select
        concat('SENSORSCM_', toString(sensor_id))  AS station_id,
        'sensorscm'                                AS source,
        concat('SENSORSCM_', toString(sensor_id))  AS station_name,
        latitude,
        longitude,
        ''                                          AS province,
        ''                                          AS city,
        'community'                                AS location_type,
        'community'                                AS sensor_quality_tier,
        true                                       AS is_active,
        min(ingest_time)                           AS first_seen,
        max(ingest_time)                           AS last_seen
    from {{ ref('stg_sensorscm__measurements') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)
select * from aqicn_stations
union all
select * from openweather_stations
union all
select * from sensorscm_stations
