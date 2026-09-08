from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

BRONZE_PATH = "../data/bronze"
SILVER_PATH = "../data/silver"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("IoTSilverTransform")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Baca semua parquet Bronze sekaligus (batch job, bukan streaming)
    bronze_df = spark.read.parquet(BRONZE_PATH)

    print(f"Bronze rows read: {bronze_df.count()}")

    # 1. Validasi: buang baris yang field pentingnya null
    #    (data cuaca tanpa suhu/lokasi gak berguna buat analisis)
    validated_df = bronze_df.filter(
        col("location_name").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("temperature_c").isNotNull()
    )

    # 2. Cast ingested_at dari string ISO8601 ke tipe timestamp asli
    #    supaya bisa di-query/di-sort secara temporal, bukan string
    typed_df = validated_df.withColumn(
        "ingested_at_ts", to_timestamp(col("ingested_at"))
    )

    # 3. Dedup: kalau ada duplikat exact (location + timestamp sama persis),
    #    ambil satu aja. window function partition by kunci unik logis,
    #    urutkan, ambil baris pertama tiap grup.
    dedup_window = Window.partitionBy(
        "location_name", "ingested_at"
    ).orderBy("ingested_at")

    deduped_df = (
        typed_df
        .withColumn("row_num", row_number().over(dedup_window))
        .filter(col("row_num") == 1)
        .drop("row_num", "ingested_at")  # buang kolom bantu + string asli
        .withColumnRenamed("ingested_at_ts", "ingested_at")
    )

    print(f"Silver rows after clean+dedup: {deduped_df.count()}")

    # Tulis ke Silver, overwrite penuh tiap run (batch job biasa,
    # bukan incremental -- untuk versi produksi nanti bisa upgrade
    # jadi partitioned + merge/upsert)
    deduped_df.write.mode("overwrite").parquet(SILVER_PATH)

    print(f"Silver layer written to {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
