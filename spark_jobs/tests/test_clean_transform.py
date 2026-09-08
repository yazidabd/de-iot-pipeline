import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("test_clean_transform")
        .master("local[1]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


SAMPLE_SCHEMA = StructType([
    StructField("location_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", IntegerType(), True),
    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
])


def test_filters_out_rows_with_null_temperature(spark):
    """Baris dengan temperature_c null harus terbuang saat validasi."""
    data = [
        ("Depok", -6.4, 106.8, "2026-01-01T00:00:00", 30.0, 50, 5.0, 0.0),
        ("Jakarta", -6.2, 106.8, "2026-01-01T00:01:00", None, 50, 5.0, 0.0),
    ]
    df = spark.createDataFrame(data, SAMPLE_SCHEMA)

    from pyspark.sql.functions import col
    validated_df = df.filter(
        col("location_name").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("temperature_c").isNotNull()
    )

    assert validated_df.count() == 1
    assert validated_df.collect()[0]["location_name"] == "Depok"


def test_deduplication_removes_exact_duplicates(spark):
    """Baris dengan location_name + ingested_at yang sama persis harus di-dedup jadi satu."""
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col

    data = [
        ("Depok", -6.4, 106.8, "2026-01-01T00:00:00", 30.0, 50, 5.0, 0.0),
        ("Depok", -6.4, 106.8, "2026-01-01T00:00:00", 30.0, 50, 5.0, 0.0),  # duplikat
        ("Jakarta", -6.2, 106.8, "2026-01-01T00:01:00", 29.0, 55, 4.0, 0.0),
    ]
    df = spark.createDataFrame(data, SAMPLE_SCHEMA)

    dedup_window = Window.partitionBy(
        "location_name", "ingested_at"
    ).orderBy("ingested_at")

    deduped_df = (
        df.withColumn("row_num", row_number().over(dedup_window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    assert deduped_df.count() == 2
