from pyspark.sql import SparkSession
from pyspark.sql.functions import col

SILVER_PATH = "../data/silver"

JDBC_URL = "jdbc:postgresql://localhost:5432/iot_warehouse"
DB_PROPERTIES = {
    "user": "iot_user",
    "password": "iot_pass123",
    "driver": "org.postgresql.Driver",
}
TARGET_TABLE = "raw_weather_readings"


def get_last_loaded_timestamp(spark) -> str | None:
    """Cek 'batas air' terakhir: ingested_at paling baru yang SUDAH ada di Postgres.
    Kalau tabel belum ada/kosong, balikin None (artinya load semua dari awal)."""
    try:
        result_df = spark.read.jdbc(
            url=JDBC_URL,
            table=f"(SELECT MAX(ingested_at) as max_ts FROM {TARGET_TABLE}) as t",
            properties=DB_PROPERTIES,
        )
        max_ts = result_df.collect()[0]["max_ts"]
        return max_ts
    except Exception:
        # Tabel belum ada sama sekali (baru pertama kali load)
        return None


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
    print(f"New rows to append: {new_count}")

    if new_count > 0:
        (
            new_rows_df.write
            .mode("append")
            .jdbc(url=JDBC_URL, table=TARGET_TABLE, properties=DB_PROPERTIES)
        )
        print(f"Appended {new_count} new rows into {TARGET_TABLE}")
    else:
        print("Nothing new to load, skipping write.")

    spark.stop()


if __name__ == "__main__":
    main()
