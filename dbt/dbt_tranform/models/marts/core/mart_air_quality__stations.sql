{{ config(materialized='view') }}

select * from {{ ref('dim_locations') }}
