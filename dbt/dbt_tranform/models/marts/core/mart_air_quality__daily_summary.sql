{{ config(materialized='view') }}

-- Thin wrapper: exposes fct_daily_aqi_summary_final for mart and KPI models
-- that expect the old mart_air_quality__daily_summary column names.
-- Rename station_id → unified_station_id for backward compatibility.
select
    date,
    station_id                                                      AS unified_station_id,
    avg_aqi,
    max_aqi,
    min_aqi,
    hourly_count,
    exceedance_count_150,
    exceedance_count_200,
    dominant_pollutant,
    sensor_quality_tier,
    source
from {{ ref('fct_daily_aqi_summary_final') }}
