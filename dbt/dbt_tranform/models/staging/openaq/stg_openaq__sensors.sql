{{ config(materialized='view') }}

with vietnam_locations as (
    select distinct location_id
    from {{ ref('stg_openaq__locations') }}
),

source as (
    select s.*
    from {{ source('openaq', 'raw_openaq_sensors') }} s
    inner join vietnam_locations l on s.location_id = l.location_id
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by sensor_id
                   order by ingest_time desc
               ) as rn
        from source
    )
    where rn = 1
),

transformed as (
    select
        -- Metadata
        source,
        ingest_time,
        ingest_batch_id,
        
        -- Sensor info
        sensor_id,
        location_id,
        parameter_id,
        name as sensor_name,
        
        -- Datetime information
        datetime_first_utc,
        {{ parse_iso_timestamp('datetime_first_local') }} as datetime_first_local,
        datetime_last_utc,
        {{ parse_iso_timestamp('datetime_last_local') }} as datetime_last_local,
        
        -- Coverage information
        coverage_expected_count,
        coverage_expected_interval,
        coverage_observed_count,
        coverage_observed_interval,
        coverage_percent_complete,
        coverage_percent_coverage,
        coverage_datetime_from_utc,
        coverage_datetime_to_utc,
        
        -- Latest measurement info
        latest_datetime_utc,
        {{ parse_iso_timestamp('latest_datetime_local') }} as latest_datetime_local,
        latest_value,
        latest_latitude,
        latest_longitude,
        
        -- Sensor metrics
        case
            when datetime_first_utc is not null and datetime_last_utc is not null
            then dateDiff('day', datetime_first_utc, datetime_last_utc)
            else null
        end as sensor_uptime_days,
        
        coalesce(coverage_percent_complete, 0) as sensor_data_completeness,
        
        -- Flags
        true as is_vietnam,
        
        raw_payload
        
    from deduplicated
)

select * from transformed

