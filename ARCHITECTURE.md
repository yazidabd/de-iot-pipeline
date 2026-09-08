# Architecture

## Overview

Pipeline ini mengambil data cuaca real-time dari API publik, memprosesnya melalui arsitektur medallion (Bronze/Silver), lalu memodelkannya menjadi star schema di data warehouse — seluruhnya di-orchestrate otomatis dan divalidasi lewat automated testing.

## Data Flow

## Keputusan Desain Utama

### Kenapa Kafka di antara Producer dan Consumer?

Producer dan consumer didesain **decoupled** — mereka tidak saling bergantung untuk nyala bersamaan. Kafka jadi buffer: kalau consumer down/restart, data yang dikirim producer selama itu tidak hilang, tetap tersimpan di topic sampai consumer siap mengonsumsinya kembali.

### Kenapa Bronze dan Silver dipisah (bukan langsung ke Postgres)?

Bronze menyimpan data mentah apa adanya — ini memberi kemampuan untuk mengulang proses transformasi kapan saja tanpa perlu mengambil ulang dari API (misalnya kalau logic cleaning berubah). Silver adalah hasil yang sudah divalidasi dan dibersihkan, siap dikonsumsi tanpa perlu logic tambahan.

### Kenapa incremental loading, bukan full refresh?

Baik `load_to_postgres.py` maupun model dbt `fact_weather_reading` awalnya pakai full overwrite — reproses seluruh data tiap run. Ini works untuk skala kecil, tapi tidak scalable. Solusinya: watermark berbasis `ingested_at` — tiap run hanya mengambil baris yang lebih baru dari data yang sudah ada, lalu di-append (bukan replace). Ini mengurangi waktu proses secara signifikan seiring data bertambah, dan merupakan pattern standar di sistem data production.

### Kenapa trigger consumer di-set 60 detik (bukan lebih rapat)?

Producer mengirim data tiap 60 detik. Trigger consumer yang lebih rapat dari itu (awalnya 30 detik) menghasilkan banyak micro-batch kosong (small file problem). Trigger diselaraskan ke 60 detik untuk mengurangi file kosong, dengan trade-off: pendekatan ini lebih simpel daripada event-driven pure streaming, dan cukup untuk skala data saat ini.

### Kenapa Airflow tidak menjalankan producer/consumer?

Airflow adalah batch orchestrator — dirancang untuk task yang mulai, berjalan, lalu selesai. Producer dan consumer adalah proses always-on (jalan terus-menerus), yang merupakan anti-pattern jika dipaksakan sebagai Airflow task. Karena itu, producer/consumer/Kafka dijalankan sebagai proses terpisah, sementara Airflow hanya mengorkestrasi bagian batch (Bronze→Silver→Postgres→dbt).

### Kenapa checkpoint dan output folder harus direset bersamaan?

Spark Structured Streaming menyimpan dua metadata terpisah: checkpoint (progress offset Kafka) dan `_spark_metadata` di folder output (daftar batch yang sudah di-commit). Keduanya saling terkait lewat nomor batch. Mereset salah satu tanpa yang lain menyebabkan nomor batch collision — Spark mengira sebuah batch baru sudah pernah ditulis sebelumnya (dari sesi lama), lalu diam-diam melewatkannya. Pelajaran: checkpoint dan output metadata harus selalu direset bersamaan.

### Kenapa Docker volume permanen penting?

Tanpa named volume, `docker compose down` diikuti `up` menghasilkan container baru yang kosong — semua data Kafka/Postgres hilang, padahal checkpoint consumer masih menunjuk ke offset lama. Ini menyebabkan error berulang setiap restart. Solusinya: named volume permanen untuk Kafka, Zookeeper, dan Postgres, sehingga data bertahan lintas restart container.

## Known Limitations

- Producer/consumer/Kafka/Postgres perlu dinyalakan manual tiap sesi baru (bukan auto-start saat boot) — hanya bagian Airflow DAG yang otomatis terjadwal.
- `dbt test` saat ini hanya berjalan terjadwal via Airflow, belum terintegrasi ke CI GitHub Actions.
- Belum ada integrasi cloud data warehouse (BigQuery/Snowflake) — semua masih berjalan di infrastruktur lokal (Docker).
- Belum ada Infrastructure as Code (Terraform) — provisioning masih manual via `docker-compose.yml`.

## Troubleshooting yang Pernah Dihadapi

| Masalah | Penyebab | Solusi |
|---|---|---|
| `kafka-python` gagal import di Python 3.14 | Library lama tidak dimaintain, vendor `six` rusak di Python baru | Ganti ke `kafka-python-ng` |
| Spark gagal start dengan Java 25 | Hadoop di dalam Spark 3.5.3 tidak kompatibel dengan Java 23+ | Install JDK 17 (Temurin) terpisah, set `JAVA_HOME` per sesi |
| Consumer error `IllegalStateException` soal offset | Kafka direset tapi checkpoint tidak (atau sebaliknya) | Selalu reset checkpoint + output metadata bersamaan |
| Consumer diam-diam tidak menulis file baru | Batch ID collision antara checkpoint baru dan `_spark_metadata` lama | Hapus keduanya sebelum restart |
| dbt install menarik `dbt-core 2.0.0rc1` | `dbt-postgres` tidak membatasi versi maksimum `dbt-core`, dan 2.0 (Fusion engine) belum mendukung Postgres | Pin `dbt-core<2.0` di `requirements.txt` |
| PySpark `PicklingError`/`RecursionError` saat `createDataFrame()` di test | Python 3.14 mengubah perilaku `pickle` untuk objek lokal, PySpark 3.5.3 belum menyesuaikan | Gunakan Python 3.12 khusus untuk venv testing PySpark |
| GitHub Actions gagal push workflow file | Personal Access Token tanpa scope `workflow` | Tambahkan scope `workflow` saat generate token |
