with measurements as (
    select * from {{ ref('int_core__measurements_unified') }}
),

normalized_for_us as (
    select
        *,
        -- Convert units for US AQI macros (which expect ppm for CO and ppb for others)
        -- Raw values are assumed to be µg/m³
        case
            when parameter = 'co' then value / 1145.0
            when parameter = 'no2' then value * 0.532
            when parameter = 'so2' then value * 0.382
            when parameter = 'o3' then value * 0.510
            else value
        end as value_us_standard
    from measurements
),

calculated as (
    select
        *,
        {{ calculate_aqi('parameter', 'value_us_standard') }} as aqi_us,
        {{ calculate_aqi_vn('parameter', 'value') }}           as aqi_vn
    from normalized_for_us
),

max_values as (
    select
        province,
        district,
        timestamp_utc,
        max(aqi_us) as max_aqi_us_in_hour,
        max(aqi_vn) as max_aqi_vn_in_hour
    from calculated
    group by province, district, timestamp_utc
)

select
    c.*,
    mv.max_aqi_us_in_hour,
    mv.max_aqi_vn_in_hour,
    case when c.aqi_us = mv.max_aqi_us_in_hour then true else false end as is_dominant_us,
    case when c.aqi_vn = mv.max_aqi_vn_in_hour then true else false end as is_dominant_vn
from calculated c
inner join max_values mv using (province, district, timestamp_utc)
