{{ config(materialized='view') }}

-- D-AQI-02: Phase 6 — Data quality metrics for AQI.in + OpenWeather
with unified as (
    select
        station_id                                        AS unified_station_id,
        source,
        toDate(timestamp_utc)                           AS measurement_date,
        count(*)                                          AS total_measurements,
        count(value)                                     AS non_null_measurements,
        count(*) - count(value)                          AS missing_measurements,
        count(distinct parameter)                         AS unique_pollutants,
        min(timestamp_utc)                               AS earliest_measurement,
        max(timestamp_utc)                               AS latest_measurement,
        dateDiff('hour', max(timestamp_utc), now())     AS data_freshness_hours
    from {{ ref('int_unified__measurements') }}
    group by station_id, source, toDate(timestamp_utc)
),
scored as (
    select
        *,
        -- Missing data rate
        if(total_measurements > 0,
            (missing_measurements::Float64 / total_measurements) * 100,
            0)                                               AS missing_data_rate,
        -- Freshness score
        case
            when data_freshness_hours <= 2  then 100
            when data_freshness_hours <= 6  then 80
            when data_freshness_hours <= 24 then 60
            when data_freshness_hours <= 48 then 40
            else 20
        end                                               AS freshness_score,
        -- Overall quality: weighted average
        (60 + freshness_score * 0.4)                       AS overall_data_quality_score,
        -- Outlier flag
        if(data_freshness_hours > 48 OR missing_data_rate > 50, true, false) AS is_outlier
    from unified
)
select
    unified_station_id,
    source,
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
    overall_data_quality_score,
    is_outlier
from scored