{{ config(materialized='view') }}

with aqicn_measurements as (
    select
        'aqicn' as source_system,
        m.station_id as unified_station_id,
        m.station_id as source_station_id,
        null as location_id,
        m.pollutant as unified_pollutant,
        m.pollutant as source_pollutant,
        m.value as concentration,
        m.measurement_datetime,
        m.measurement_datetime_iso,
        s.latitude as latitude,
        s.longitude as longitude,
        s.province as province,
        s.city as city,
        s.location_type as location_type,
        m.aqi as aqi,
        m.data_quality_score,
        m.ingest_time
    from {{ ref('stg_aqicn__measurements') }} m
    left join {{ ref('stg_aqicn__stations') }} s on m.station_id = s.station_id
),

openaq_measurements as (
    select
        'openaq' as source_system,
        concat('OPENAQ_', toString(location_id)) as unified_station_id,
        null as source_station_id,
        m.location_id as location_id,
        p.parameter_key as unified_pollutant,
        p.parameter_name_original as source_pollutant,
        m.value as concentration,
        period_datetime_from_utc as measurement_datetime,
        period_datetime_from_local as measurement_datetime_iso,
        m.latitude,
        m.longitude,
        l.province,
        l.city,
        case
            when l.is_mobile then 'Mobile'
            when l.is_monitor then 'Monitor'
            else 'Other'
        end as location_type,
        null as aqi,
        m.data_quality_score,
        m.ingest_time
    from {{ ref('stg_openaq__measurements') }} m
    inner join {{ ref('stg_openaq__parameters') }} p on m.parameter_id = p.parameter_id
    inner join {{ ref('stg_openaq__locations') }} l on m.location_id = l.location_id
),

unified as (
    select * from aqicn_measurements
    union all
    select * from openaq_measurements
)

select
    source_system,
    unified_station_id,
    source_station_id,
    location_id,
    unified_pollutant,
    source_pollutant,
    concentration,
    measurement_datetime,
    measurement_datetime_iso,
    latitude,
    longitude,
    province,
    city,
    location_type,
    aqi,
    data_quality_score,
    ingest_time
from unified
where concentration is not null
  and measurement_datetime is not null
