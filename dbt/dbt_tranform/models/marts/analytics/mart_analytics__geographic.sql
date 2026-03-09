{{ config(materialized='table') }}

with daily_summary as (
    select
        date,
        province,
        city,
        unified_station_id,
        avg_aqi,
        max_aqi,
        min_aqi,
        avg_pm25,
        avg_pm10,
        avg_o3,
        exceedance_count
    from {{ ref('mart_air_quality__daily_summary') }}
),

province_summary as (
    select
        date,
        province,
        avg(avg_aqi) as avg_aqi,
        max(max_aqi) as max_aqi,
        min(min_aqi) as min_aqi,
        avg(avg_pm25) as avg_pm25,
        avg(avg_pm10) as avg_pm10,
        avg(avg_o3) as avg_o3,
        sum(exceedance_count) as total_exceedance_count,
        count(distinct unified_station_id) as station_count
    from daily_summary
    where province is not null
    group by date, province
),

city_summary as (
    select
        date,
        province,
        city,
        avg(avg_aqi) as avg_aqi,
        max(max_aqi) as max_aqi,
        min(min_aqi) as min_aqi,
        avg(avg_pm25) as avg_pm25,
        avg(avg_pm10) as avg_pm10,
        avg(avg_o3) as avg_o3,
        sum(exceedance_count) as total_exceedance_count,
        count(distinct unified_station_id) as station_count
    from daily_summary
    where city is not null
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
        avg_pm25,
        avg_pm10,
        avg_o3,
        total_exceedance_count,
        station_count,
        case
            when avg_aqi <= 50 then 'Good'
            when avg_aqi <= 100 then 'Moderate'
            when avg_aqi <= 150 then 'Unhealthy for Sensitive Groups'
            when avg_aqi <= 200 then 'Unhealthy'
            when avg_aqi <= 300 then 'Very Unhealthy'
            else 'Hazardous'
        end as pollution_level_category
    from city_summary
)

select * from with_category

