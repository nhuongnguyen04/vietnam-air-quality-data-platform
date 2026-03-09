{{ config(materialized='table') }}

select
    unified_station_id,
    toStartOfHour(measurement_datetime) as datetime_hour,
    province,
    city,
    unified_pollutant,
    avg(concentration) as avg_concentration,
    min(concentration) as min_concentration,
    max(concentration) as max_concentration,
    avg(aqi_value) as avg_aqi,
    min(aqi_value) as min_aqi,
    max(aqi_value) as max_aqi,
    count(*) as measurement_count,
    maxIf(concentration, unified_pollutant = 'pm25') as pm25_value,
    maxIf(concentration, unified_pollutant = 'pm10') as pm10_value,
    maxIf(concentration, unified_pollutant = 'o3') as o3_value,
    maxIf(concentration, unified_pollutant = 'no2') as no2_value,
    maxIf(concentration, unified_pollutant = 'co') as co_value,
    maxIf(concentration, unified_pollutant = 'so2') as so2_value,
    maxIf(aqi_value, unified_pollutant = 'pm25') as pm25_aqi,
    maxIf(aqi_value, unified_pollutant = 'pm10') as pm10_aqi,
    maxIf(aqi_value, unified_pollutant = 'o3') as o3_aqi,
    maxIf(aqi_value, unified_pollutant = 'no2') as no2_aqi,
    maxIf(aqi_value, unified_pollutant = 'co') as co_aqi,
    maxIf(aqi_value, unified_pollutant = 'so2') as so2_aqi
from {{ ref('int_aqi_calculations') }}
where measurement_datetime is not null
  and concentration is not null
group by 
    unified_station_id,
    toStartOfHour(measurement_datetime),
    province,
    city,
    unified_pollutant

