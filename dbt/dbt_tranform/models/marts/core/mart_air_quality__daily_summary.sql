{{ config(materialized='table') }}

with aqi_data as (
    select
        unified_station_id,
        toDate(measurement_datetime) as date,
        unified_pollutant,
        aqi_value,
        concentration,
        is_dominant_pollutant,
        province,
        city
    from {{ ref('int_aqi_calculations') }}
    where aqi_value is not null
),

daily_aqi as (
    select
        unified_station_id,
        date,
        province,
        city,
        avg(aqi_value) as avg_aqi,
        min(aqi_value) as min_aqi,
        max(aqi_value) as max_aqi,
        count(*) as measurement_count
    from aqi_data
    group by unified_station_id, date, province, city
),

pollutant_summary as (
    select
        unified_station_id,
        toDate(measurement_datetime) as date,
        unified_pollutant,
        avg(concentration) as avg_concentration,
        min(concentration) as min_concentration,
        max(concentration) as max_concentration,
        count(*) as measurement_count
    from {{ ref('int_aqi_calculations') }}
    where concentration is not null
    group by unified_station_id, toDate(measurement_datetime), unified_pollutant
),

pollutant_pivot as (
    select
        unified_station_id,
        date,
        maxIf(avg_concentration, unified_pollutant = 'pm25') as avg_pm25,
        maxIf(avg_concentration, unified_pollutant = 'pm10') as avg_pm10,
        maxIf(avg_concentration, unified_pollutant = 'o3') as avg_o3,
        maxIf(avg_concentration, unified_pollutant = 'no2') as avg_no2,
        maxIf(avg_concentration, unified_pollutant = 'co') as avg_co,
        maxIf(avg_concentration, unified_pollutant = 'so2') as avg_so2,
        maxIf(max_concentration, unified_pollutant = 'pm25') as max_pm25,
        maxIf(max_concentration, unified_pollutant = 'pm10') as max_pm10,
        maxIf(max_concentration, unified_pollutant = 'o3') as max_o3,
        maxIf(max_concentration, unified_pollutant = 'no2') as max_no2,
        maxIf(max_concentration, unified_pollutant = 'co') as max_co,
        maxIf(max_concentration, unified_pollutant = 'so2') as max_so2
    from pollutant_summary
    group by unified_station_id, date
),

dominant_pollutant as (
    select
        unified_station_id,
        date,
        argMax(unified_pollutant, aqi_value) as dominant_pollutant
    from aqi_data
    where is_dominant_pollutant = true
    group by unified_station_id, date
),

exceedances as (
    select
        unified_station_id,
        toDate(measurement_datetime) as date,
        unified_pollutant,
        countIf(
            (unified_pollutant = 'pm25' and concentration > 35.4) or
            (unified_pollutant = 'pm10' and concentration > 154) or
            (unified_pollutant = 'o3' and concentration > 70) or
            (unified_pollutant = 'no2' and concentration > 100) or
            (unified_pollutant = 'co' and concentration > 9.4) or
            (unified_pollutant = 'so2' and concentration > 75)
        ) as exceedance_count
    from {{ ref('int_aqi_calculations') }}
    where concentration is not null
    group by unified_station_id, toDate(measurement_datetime), unified_pollutant
),

exceedance_total as (
    select
        unified_station_id,
        date,
        sum(exceedance_count) as total_exceedance_count
    from exceedances
    group by unified_station_id, date
)

select
    d.unified_station_id as unified_station_id,
    d.date as date,
    d.province,
    d.city,
    d.avg_aqi,
    d.min_aqi,
    d.max_aqi,
    d.measurement_count,
    p.avg_pm25,
    p.avg_pm10,
    p.avg_o3,
    p.avg_no2,
    p.avg_co,
    p.avg_so2,
    p.max_pm25,
    p.max_pm10,
    p.max_o3,
    p.max_no2,
    p.max_co,
    p.max_so2,
    dp.dominant_pollutant,
    coalesce(e.total_exceedance_count, 0) as exceedance_count
from daily_aqi d
left join pollutant_pivot p on d.unified_station_id = p.unified_station_id and d.date = p.date
left join dominant_pollutant dp on d.unified_station_id = dp.unified_station_id and d.date = dp.date
left join exceedance_total e on d.unified_station_id = e.unified_station_id and d.date = e.date

