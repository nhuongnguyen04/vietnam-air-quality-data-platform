{{ config(materialized='view') }}

select * from {{ ref('fct_hourly_aqi_final') }}
