{{ config(materialized='view') }}

-- Pre-materialized daily actuals: avoids ClickHouse correlated subquery limitation in JOINs
-- Joins on: station_id + pollutant + measurement_date
select
    station_id,
    parameter                                    AS pollutant,
    toDate(timestamp_utc)                       AS measurement_date,
    avg(value)                                   AS avg_value,
    max(value)                                   AS max_value,
    count(*)                                     AS reading_count
from {{ ref('stg_aqicn__measurements') }}
where value is not null
group by station_id, parameter, toDate(timestamp_utc)
