{{ config(materialized='table') }}

-- Pollutant concentration KPIs against WHO/EPA standards.
-- Aggregates from fct_hourly_aqi (public view) which has pollutant-level data.
with pollutant_daily as (
    select
        toDate(datetime_hour)                    AS date,
        pollutant,
        avg(normalized_aqi)                      AS avg_aqi,
        max(normalized_aqi)                      AS max_aqi,
        min(normalized_aqi)                      AS min_aqi,
        count(*)                                 AS reading_count
    from {{ ref('fct_hourly_aqi') }}
    where pollutant in ('pm25', 'pm10', 'o3', 'no2', 'co', 'so2')
    group by toDate(datetime_hour), pollutant
),

with_ref as (
    select
        date,
        pollutant,
        avg_aqi                                                          AS daily_avg_aqi,
        max_aqi,
        min_aqi,
        reading_count,
        case pollutant
            when 'pm25' then 50
            when 'pm10' then 100
            when 'o3'   then 50
            when 'no2'  then 100
            when 'co'   then 50
            when 'so2'  then 100
            else 100
        end AS reference_threshold
    from pollutant_daily
),

with_counts as (
    select
        date,
        pollutant,
        reference_threshold,
        sum(reading_count)                                               AS total_readings,
        sumIf(reading_count, daily_avg_aqi > reference_threshold)       AS exceedance_count
    from with_ref
    group by date, pollutant, reference_threshold
)

select
    date,
    pollutant,
    reference_threshold,
    round(100.0 * exceedance_count / total_readings, 1)                  AS exceedance_rate,
    total_readings
from with_counts
