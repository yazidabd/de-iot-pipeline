import sys
import os

# Tambahkan folder induk ke path biar bisa import weather_producer.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from weather_producer import build_reading


def test_build_reading_extracts_correct_fields():
    """Pastikan build_reading() ambil field yang benar dari response API mentah."""
    raw_api_response = {
        "current": {
            "temperature_2m": 31.5,
            "relative_humidity_2m": 46,
            "wind_speed_10m": 12.8,
            "precipitation": 0.0,
        }
    }

    result = build_reading("Depok", -6.4025, 106.7942, raw_api_response)

    assert result["location_name"] == "Depok"
    assert result["latitude"] == -6.4025
    assert result["longitude"] == 106.7942
    assert result["temperature_c"] == 31.5
    assert result["humidity_pct"] == 46
    assert result["wind_speed_kmh"] == 12.8
    assert result["precipitation_mm"] == 0.0


def test_build_reading_includes_timestamp():
    """Pastikan build_reading() selalu menambahkan timestamp ingested_at."""
    raw_api_response = {"current": {"temperature_2m": 30.0}}

    result = build_reading("Jakarta", -6.2088, 106.8456, raw_api_response)

    assert "ingested_at" in result
    assert result["ingested_at"] is not None


def test_build_reading_handles_missing_field():
    """Pastikan build_reading() gak crash kalau salah satu field cuaca hilang dari response."""
    raw_api_response = {"current": {"temperature_2m": 31.5}}  # humidity dll hilang

    result = build_reading("Depok", -6.4025, 106.7942, raw_api_response)

    assert result["temperature_c"] == 31.5
    assert result["humidity_pct"] is None  # harus None, bukan crash
