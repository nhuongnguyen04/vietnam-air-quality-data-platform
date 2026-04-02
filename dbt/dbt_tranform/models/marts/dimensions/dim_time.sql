{{ config(materialized='table') }}

select
    parseDateTime(datetime_hour, '%Y-%m-%dT%H:%M:%S')  AS datetime_hour,
    parseDateTime(date, '%Y-%m-%d')                     AS date,
    day_of_week,
    toUInt8(hour_of_day)                                 AS hour_of_day,
    is_weekend,
    toUInt8(month)                                       AS month,
    toUInt16(year)                                       AS year,
    vietnam_tz_offset
from {{ ref('seed_dim_time') }}
