{% macro process_source_to_hourly(source_ref) %}
with aggregated_data as (
    select
        toStartOfHour(timestamp_utc) as datetime_hour,
        any(toDate(timestamp_utc)) as date,
        ward_code,
        province,
        any(region_3) as region_3,
        any(region_8) as region_8,
        max(aqi_vn) as avg_aqi_vn,
        max(aqi_us) as avg_aqi_us,
        nullIf(avgIf(value, parameter='pm25'), 0/0) as pm25_avg,
        nullIf(avgIf(value, parameter='pm10'), 0/0) as pm10_avg,
        nullIf(avgIf(value, parameter='co'),   0/0) as co_avg,
        nullIf(avgIf(value, parameter='no2'),  0/0) as no2_avg,
        nullIf(avgIf(value, parameter='so2'),  0/0) as so2_avg,
        nullIf(avgIf(value, parameter='o3'),   0/0) as o3_avg,
        -- Sub-AQIs
        nullIf(avgIf(aqi_vn, parameter='pm25'), 0/0) as pm25_aqi,
        nullIf(avgIf(aqi_vn, parameter='pm10'), 0/0) as pm10_aqi,
        nullIf(avgIf(aqi_vn, parameter='co'),   0/0) as co_aqi,
        nullIf(avgIf(aqi_vn, parameter='no2'),  0/0) as no2_aqi,
        nullIf(avgIf(aqi_vn, parameter='so2'),  0/0) as so2_aqi,
        nullIf(avgIf(aqi_vn, parameter='o3'),   0/0) as o3_aqi,
        count() as observation_count,
        {{ get_main_pollutant(
            "avgIf(aqi_vn, parameter='pm25')",
            "avgIf(aqi_vn, parameter='pm10')",
            "avgIf(aqi_vn, parameter='co')",
            "avgIf(aqi_vn, parameter='no2')",
            "avgIf(aqi_vn, parameter='so2')",
            "avgIf(aqi_vn, parameter='o3')"
        ) }} as main_pollutant,
        max(ingest_time) as last_ingested_at,
        max(raw_loaded_at) as max_raw_loaded_at,
        argMax(raw_sync_run_id, raw_loaded_at) as latest_raw_sync_run_id,
        argMax(raw_sync_started_at, raw_loaded_at) as latest_raw_sync_started_at
    from {{ source_ref }}
    where parameter in ('pm25', 'pm10', 'co', 'no2', 'so2', 'o3')
    group by datetime_hour, ward_code, province
)
select
    datetime_hour,
    date,
    ward_code,
    province,
    region_3,
    region_8,
    avg_aqi_vn,
    avg_aqi_us,
    pm25_avg,
    pm10_avg,
    co_avg,
    no2_avg,
    so2_avg,
    o3_avg,
    pm25_aqi,
    pm10_aqi,
    co_aqi,
    no2_aqi,
    so2_aqi,
    o3_aqi,
    observation_count,
    main_pollutant,
    last_ingested_at,
    max_raw_loaded_at as raw_loaded_at,
    latest_raw_sync_run_id as raw_sync_run_id,
    latest_raw_sync_started_at as raw_sync_started_at
from aggregated_data
{% endmacro %}
