{{ config(
    materialized='table',
    engine='ReplacingMergeTree',
    order_by=['station_id'],
    partition_by='toYYYYMM(last_seen)',
    version='location_version'
) }}

select
    station_id,
    source,
    station_name,
    latitude,
    longitude,
    city,
    province,
    location_type,
    sensor_quality_tier,
    is_active,
    first_seen,
    last_seen,
    last_seen as location_version
from {{ ref('int_unified__stations') }}
