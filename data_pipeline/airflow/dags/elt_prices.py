import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from fetch_price import fetch_spot_prices

HOST_PROJECT_PATH = os.getenv("HOST_PROJECT_PATH", "/home/klein/enedis_pipeline")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

DBT_IMAGE = "ghcr.io/dbt-labs/dbt-postgres:1.9.latest"
DBT_MOUNT = {
    "source": f"{HOST_PROJECT_PATH}/data_pipeline/dbt",
    "target": "/usr/app/dbt",
    "type": "bind"
}
DBT_COMMON = {
    "image": DBT_IMAGE,
    "mounts": [DBT_MOUNT],
    "mount_tmp_dir": False,
    "network_mode": "enedis_pipeline_elt_network",
    "auto_remove": True,
    "docker_url": "unix://var/run/docker.sock",
}
PROFILES = "--profiles-dir /usr/app/dbt/my_project --project-dir /usr/app/dbt/my_project"

with DAG(
    dag_id="elt_prices",
    description="Extract RTE spot prices and transform with dbt",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 17),
    catchup=False,
    default_args=default_args,
) as dag:

    extract_prices = PythonOperator(
        task_id="extract_prices",
        python_callable=fetch_spot_prices,
        op_kwargs={"data_interval_start": "{{ data_interval_start }}"},
    )

    bronze_prices = DockerOperator(
        task_id="bronze_rte_spot_prices",
        command=f"run --select bronze_prices {PROFILES}",
        **DBT_COMMON,
    )

    silver_prices = DockerOperator(
        task_id="silver_rte_spot_prices",
        command=f"run --select silver_prices {PROFILES}",
        **DBT_COMMON,
    )

    extract_prices >> bronze_prices >> silver_prices