{{ config(materialized='view') }}

select
    station_id,
    datetime_hour,
    threshold_breached,
    normalized_aqi,
    dominant_pollutant,
    source,
    sensor_quality_tier,
    created_at
from {{ ref('fact_aqi_alerts') }}
