from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from fetch_enedis import fetch

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

DBT_IMAGE = "ghcr.io/dbt-labs/dbt-postgres:1.9.latest"
DBT_MOUNT = {
    "source": "/home/klein/enedis_pipeline/dbt",
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
    dag_id="elt_enedis",
    description="Extract Enedis electricity data, transform with dbt, build gold layer",
    schedule_interval="@hourly",
    start_date=datetime(2021, 1, 17),
    catchup=False,
    default_args=default_args,
) as dag:

    extract_enedis = PythonOperator(
        task_id="extract_enedis",
        python_callable=fetch,
    )

    bronze_electricite = DockerOperator(
        task_id="bronze_electricite",
        command=f"run --select bronze_electricite {PROFILES}",
        **DBT_COMMON,
    )

    silver_electricite = DockerOperator(
        task_id="silver_electricite",
        command=f"run --select silver_electricite {PROFILES}",
        **DBT_COMMON,
    )

    gold_consumption = DockerOperator(
        task_id="gold_consumption_vs_price",
        command=f"run --select gold_consumption_vs_price {PROFILES}",
        **DBT_COMMON,
    )


    extract_enedis >> bronze_electricite >> silver_electricite >> gold_consumption
