{{ config(materialized='view') }}

with measurements as (
    select
        m.*,
        p.units
    from {{ ref('int_unified__measurements') }} m
    left join {{ ref('stg_openaq__parameters') }} p 
        on m.unified_pollutant = p.parameter_key
),

aqi_calculated as (
    select
        *,
        {{ calculate_aqi('unified_pollutant', 'concentration') }} as aqi_value,
        {{ get_aqi_category('aqi_value') }} as aqi_category
    from measurements
    where unified_pollutant in ('pm25', 'pm10', 'o3', 'no2', 'co', 'so2')
),

with_dominant as (
    select
        *,
        max(aqi_value) over (
            partition by unified_station_id, measurement_datetime
        ) as max_aqi_per_measurement
    from aqi_calculated
),

final as (
    select
        source_system,
        unified_station_id,
        source_station_id,
        location_id,
        unified_pollutant,
        source_pollutant,
        concentration,
        units,
        measurement_datetime,
        measurement_datetime_iso,
        latitude,
        longitude,
        province,
        city,
        location_type,
        aqi_value,
        aqi_category,
        case
            when aqi_value = max_aqi_per_measurement then true
            else false
        end as is_dominant_pollutant,
        data_quality_score,
        ingest_time
    from with_dominant
)

select * from final

