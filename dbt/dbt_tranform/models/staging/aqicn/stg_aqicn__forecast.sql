{{ config(materialized='view') }}

with source as (
    select * from {{ source('aqicn', 'raw_aqicn_forecast') }}
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by station_id, measurement_time_v, forecast_type, pollutant, day
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
        
        -- Station and forecast info
        station_id,
        {{ parse_unix_timestamp('measurement_time_v') }} as measurement_datetime,
        forecast_type,
        {{ standardize_pollutant_name('pollutant') }} as pollutant,
        
        -- Forecast date
        {{ parse_date_string('day') }} as forecast_date,
        day as forecast_date_str,
        
        -- Forecast values (convert from string to float)
        -- D-36: max_val/min_val renamed from max/min to avoid ClickHouse built-in function name conflict
        toFloat64OrNull(avg) as forecast_avg,
        toFloat64OrNull(max_val) as forecast_max,
        toFloat64OrNull(min_val) as forecast_min,
        
        -- Calculate forecast lead days
        dateDiff('day', toDate(measurement_datetime), forecast_date) as forecast_lead_days,
        
        raw_forecast_item
        
    from deduplicated
    where forecast_date is not null
)

select * from transformed
