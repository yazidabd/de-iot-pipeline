{{
    config(
        materialized='table'
    )
}}

select distinct
    location_name,
    latitude,
    longitude
from {{ ref('stg_weather_readings') }}
