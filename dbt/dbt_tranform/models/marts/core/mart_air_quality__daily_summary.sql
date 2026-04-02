{{ config(materialized='view') }}

select * from {{ ref('fct_daily_aqi_summary_final') }}
