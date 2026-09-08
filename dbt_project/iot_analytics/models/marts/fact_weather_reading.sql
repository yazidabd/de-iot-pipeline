{{
    config(
        materialized='incremental',
        unique_key='ingested_at || location_name'
    )
}}

select
    s.ingested_at,
    s.location_name,
    s.temperature_c,
    s.humidity_pct,
    s.wind_speed_kmh,
    s.precipitation_mm
from {{ ref('stg_weather_readings') }} s

{% if is_incremental() %}
where s.ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
