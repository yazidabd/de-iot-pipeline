from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


SPARK_JOBS_DIR = "/home/tupay/PROJECT/de-iot-pipeline/spark_jobs"
SPARK_JOBS_PYTHON = f"{SPARK_JOBS_DIR}/venv/bin/python"
JAVA_HOME = "/usr/lib/jvm/jdk-17.0.13+11"

DBT_PROJECT_DIR = "/home/tupay/PROJECT/de-iot-pipeline/dbt_project/iot_analytics"
DBT_BIN = "/home/tupay/PROJECT/de-iot-pipeline/dbt_project/venv/bin/dbt"


@dag(
    dag_id="iot_bronze_to_silver",
    description="Transform Bronze IoT sensor data into Silver, load to Postgres, run dbt models",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 8, 29),
    catchup=False,
    tags=["iot", "silver", "dbt"],
)
def iot_bronze_to_silver():
    run_silver_transform = BashOperator(
        task_id="run_silver_transform",
        bash_command=(
            f"cd {SPARK_JOBS_DIR} && "
            f"JAVA_HOME={JAVA_HOME} "
            f"PATH={JAVA_HOME}/bin:$PATH "
            f"{SPARK_JOBS_PYTHON} clean_transform.py"
        ),
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=(
            f"cd {SPARK_JOBS_DIR} && "
            f"JAVA_HOME={JAVA_HOME} "
            f"PATH={JAVA_HOME}/bin:$PATH "
            f"{SPARK_JOBS_PYTHON} load_to_postgres.py"
        ),
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} run",
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {DBT_PROJECT_DIR} && {DBT_BIN} test",
    )

    run_silver_transform >> load_to_postgres >> run_dbt_models >> run_dbt_tests


iot_bronze_to_silver()
