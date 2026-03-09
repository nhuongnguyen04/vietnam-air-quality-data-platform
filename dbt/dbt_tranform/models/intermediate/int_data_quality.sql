{{ config(materialized='view') }}

with aqicn_quality as (
    select
        station_id as unified_station_id,
        'aqicn' as source_system,
        toDate(measurement_datetime) as measurement_date,
        count(*) as total_measurements,
        count(value) as non_null_measurements,
        count(*) - count(value) as missing_measurements,
        count(distinct pollutant) as unique_pollutants,
        min(measurement_datetime) as earliest_measurement,
        max(measurement_datetime) as latest_measurement,
        dateDiff('hour', max(measurement_datetime), now()) as data_freshness_hours,
        avg(data_quality_score) as avg_data_quality_score
    from {{ ref('stg_aqicn__measurements') }}
    group by station_id, toDate(measurement_datetime)
),

openaq_quality as (
    select
        concat('OPENAQ_', toString(location_id)) as unified_station_id,
        'openaq' as source_system,
        toDate(period_datetime_from_utc) as measurement_date,
        count(*) as total_measurements,
        count(value) as non_null_measurements,
        count(*) - count(value) as missing_measurements,
        count(distinct parameter_id) as unique_pollutants,
        min(period_datetime_from_utc) as earliest_measurement,
        max(period_datetime_from_utc) as latest_measurement,
        dateDiff('hour', max(period_datetime_from_utc), now()) as data_freshness_hours,
        avg(data_quality_score) as avg_data_quality_score
    from {{ ref('stg_openaq__measurements') }}
    group by location_id, toDate(period_datetime_from_utc)
),

combined as (
    select * from aqicn_quality
    union all
    select * from openaq_quality
),

with_outliers as (
    select
        *,
        case
            when total_measurements > 0 
            then (missing_measurements::Float64 / total_measurements) * 100
            else 100
        end as missing_data_rate,
        case
            when data_freshness_hours <= 24 then 100
            when data_freshness_hours <= 48 then 75
            when data_freshness_hours <= 72 then 50
            when data_freshness_hours <= 168 then 25
            else 10
        end as freshness_score,
        case
            when missing_data_rate > 50 then true
            when data_freshness_hours > 168 then true
            else false
        end as is_outlier
    from combined
)

select
    unified_station_id,
    source_system,
    measurement_date,
    total_measurements,
    non_null_measurements,
    missing_measurements,
    missing_data_rate,
    unique_pollutants,
    earliest_measurement,
    latest_measurement,
    data_freshness_hours,
    freshness_score,
    avg_data_quality_score,
    is_outlier,
    (avg_data_quality_score * 0.5 + freshness_score * 0.3 + (100 - missing_data_rate) * 0.2) as overall_data_quality_score
from with_outliers

