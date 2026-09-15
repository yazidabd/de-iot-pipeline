from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import psycopg2

SILVER_PATH = "../data/silver"

JDBC_URL = "jdbc:postgresql://localhost:5432/iot_warehouse"
DB_PROPERTIES = {
    "user": "iot_user",
    "password": "iot_pass123",
    "driver": "org.postgresql.Driver",
}
TARGET_TABLE = "raw_weather_readings"
STAGING_TABLE = "raw_weather_readings_staging"

PG_CONN_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "iot_warehouse",
    "user": "iot_user",
    "password": "iot_pass123",
}


def get_last_loaded_timestamp(spark) -> str | None:
    """Cek batas air terakhir: ingested_at paling baru yang sudah ada di Postgres."""
    try:
        result_df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"(SELECT MAX(ingested_at) as max_ts FROM {TARGET_TABLE}) as t",
            properties=DB_PROPERTIES,
        )
        return result_df.collect()[0]["max_ts"]
    except Exception:
        return None


def upsert_staging_to_target() -> int:
    """Pindahkan data dari staging ke target pakai UPSERT (ON CONFLICT DO UPDATE),
    supaya proses ini aman dijalankan berkali-kali tanpa menghasilkan duplikat,
    bahkan kalau run sebelumnya gagal di tengah jalan."""
    conn = psycopg2.connect(**PG_CONN_PARAMS)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {TARGET_TABLE} (
                    location_name, latitude, longitude, temperature_c,
                    humidity_pct, wind_speed_kmh, precipitation_mm, ingested_at
                )
                SELECT
                    location_name, latitude, longitude, temperature_c,
                    humidity_pct, wind_speed_kmh, precipitation_mm, ingested_at
                FROM {STAGING_TABLE}
                ON CONFLICT (location_name, ingested_at)
                DO UPDATE SET
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    temperature_c = EXCLUDED.temperature_c,
                    humidity_pct = EXCLUDED.humidity_pct,
                    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                    precipitation_mm = EXCLUDED.precipitation_mm;
            """)
            row_count = cur.rowcount
            cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE};")
        conn.commit()
        return row_count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("LoadSilverToPostgresIncremental")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver_df = spark.read.parquet(SILVER_PATH)
    last_loaded_ts = get_last_loaded_timestamp(spark)

    if last_loaded_ts is not None:
        print(f"Last loaded timestamp in Postgres: {last_loaded_ts}")
        new_rows_df = silver_df.filter(col("ingested_at") > last_loaded_ts)
    else:
        print("No existing data in Postgres, loading everything.")
        new_rows_df = silver_df

    new_count = new_rows_df.count()
    print(f"New rows to stage: {new_count}")

    if new_count > 0:
        (
            new_rows_df.write
            .mode("overwrite")
            .jdbc(url=JDBC_URL, table=STAGING_TABLE, properties=DB_PROPERTIES)
        )
        upserted = upsert_staging_to_target()
        print(f"Upserted {upserted} rows into {TARGET_TABLE}")
    else:
        print("Nothing new to load, skipping write.")

    spark.stop()


if __name__ == "__main__":
    main()
