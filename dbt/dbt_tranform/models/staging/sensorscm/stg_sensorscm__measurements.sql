{{ config(materialized='view') }}

select
    'sensorscm'                                                AS source,
    concat('SENSORSCM_', toString(sensor_id))                  AS station_id,
    sensor_id,
    latitude,
    longitude,
    timestamp_utc,
    {{ standardize_pollutant_name('parameter') }}               AS parameter,
    value,
    unit,
    quality_flag,
    ingest_time,
    raw_payload
from {{ source('sensorscm', 'raw_sensorscm_measurements') }}
