{{
    config(
        materialized='table'
    )
}}

select distinct
    ingested_at,
    date(ingested_at) as reading_date,
    extract(hour from ingested_at) as reading_hour,
    extract(minute from ingested_at) as reading_minute
from {{ ref('stg_weather_readings') }}
