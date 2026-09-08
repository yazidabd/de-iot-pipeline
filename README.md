# IoT Weather Data Pipeline

End-to-end data engineering pipeline yang mengambil data cuaca real-time, memprosesnya lewat arsitektur Bronze/Silver/Gold, dan menyajikannya sebagai star schema di data warehouse — dengan orchestration, data quality testing, dan CI/CD otomatis.

## Arsitektur

## Tech Stack

- **Ingestion**: Python, Open-Meteo API
- **Messaging**: Apache Kafka
- **Stream Processing**: PySpark Structured Streaming
- **Orchestration**: Apache Airflow 3.x
- **Data Warehouse**: PostgreSQL
- **Data Modeling**: dbt (incremental materialization, star schema)
- **Testing**: dbt test (data quality), pytest (unit test)
- **CI/CD**: GitHub Actions
- **Infrastructure**: Docker Compose (persistent volumes)

## Struktur Project

## Cara Menjalankan

1. Nyalakan infrastruktur: `docker compose up -d`
2. Nyalakan producer: lihat `producer/README` (atau jalankan `python weather_producer.py`)
3. Nyalakan consumer: jalankan `spark_streaming_consumer.py`
4. Nyalakan Airflow: `airflow standalone`, buka `localhost:8080`

## Testing

- Unit test: `pytest tests/` (di folder `producer/` dan `spark_jobs/`)
- Data quality test: `dbt test` (di folder `dbt_project/iot_analytics/`)
- CI: otomatis jalan tiap push via GitHub Actions

## Catatan Desain

- **Incremental loading**: baik load ke Postgres maupun dbt fact table menggunakan incremental materialization (bukan full refresh), berdasarkan watermark `ingested_at`.
- **Persistent volumes**: Kafka, Zookeeper, dan Postgres menggunakan Docker volume permanen, sehingga data bertahan meski container di-restart.
