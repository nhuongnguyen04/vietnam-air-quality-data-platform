{{ config(materialized='table') }}

-- Geographic air quality analytics: province and city level aggregations
-- Join mart_air_quality__daily_summary with dim_locations for province/city hierarchy
with daily_station as (
    select
        d.date,
        d.unified_station_id,
        l.province,
        l.city,
        d.avg_aqi,
        d.max_aqi,
        d.min_aqi,
        d.source,
        d.sensor_quality_tier
    from {{ ref('mart_air_quality__daily_summary') }} d
    inner join {{ ref('dim_locations') }} l on d.unified_station_id = l.station_id
),

province_summary as (
    select
        date,
        province,
        avg(avg_aqi)         AS avg_aqi,
        max(max_aqi)         AS max_aqi,
        min(min_aqi)         AS min_aqi,
        count(distinct unified_station_id) AS station_count
    from daily_station
    where province is not null and province != ''
    group by date, province
),

city_summary as (
    select
        date,
        province,
        city,
        avg(avg_aqi)         AS avg_aqi,
        max(max_aqi)         AS max_aqi,
        min(min_aqi)         AS min_aqi,
        count(distinct unified_station_id) AS station_count
    from daily_station
    where city is not null and city != ''
    group by date, province, city
),

with_category as (
    select
        date,
        province,
        city,
        avg_aqi,
        max_aqi,
        min_aqi,
        station_count,
        case
            when avg_aqi <= 50   then 'Good'
            when avg_aqi <= 100  then 'Moderate'
            when avg_aqi <= 150  then 'Unhealthy for Sensitive Groups'
            when avg_aqi <= 200  then 'Unhealthy'
            when avg_aqi <= 300  then 'Very Unhealthy'
            else 'Hazardous'
        end AS pollution_level_category
    from city_summary
)

select * from with_category
