select
    location_name,
    latitude,
    longitude,
    temperature_c,
    humidity_pct,
    wind_speed_kmh,
    precipitation_mm,
    ingested_at
from {{ source('raw', 'raw_weather_readings') }}
