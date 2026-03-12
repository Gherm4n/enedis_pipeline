from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

from fetch_enedis import fetch
from fetch_price import scrape_rte_spot_prices

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay":timedelta(minutes=5),
}

with DAG(
    dag_id="elt_pipeline",
    description="extract from Enedis API, load to postgres, transform with dbt",
    schedule_interval="@hourly",
    start_date=datetime(2024,1,1),
    catchup=False,
    default_args=default_args,
) as dag:
    task1 = PythonOperator(
        task_id="extract_and_load_enedis",
        python_callable=fetch,
    )

    task2 = PythonOperator(
        task_id="extract_and_load_prices",
        python_callable=scrape_rte_spot_prices,
    )

    task3 = DockerOperator(
        task_id="dbt_bronze_data",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run --select bronze_data --profiles-dir /usr/app/dbt/my_project --project-dir /usr/app/dbt/my_project",
        mounts=[
            {
                "source": "/home/klein/projet_yadari/dbt",
                "target": "/usr/app/dbt",
                "type": "bind"
            }
        ],
        mount_tmp_dir=False,
        network_mode="projet_yadari_elt_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    task4 = DockerOperator(
        task_id="dbt_bronze_prices",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run --select bronze_prices --profiles-dir /usr/app/dbt/my_project --project-dir /usr/app/dbt/my_project",
        mounts=[
            {
                "source": "/home/klein/projet_yadari/dbt",
                "target": "/usr/app/dbt",
                "type": "bind"
            }
        ],
        mount_tmp_dir=False,
        network_mode="projet_yadari_elt_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    task5 = DockerOperator(
        task_id="dbt_silver_data",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run --select silver_data --profiles-dir /usr/app/dbt/my_project --project-dir /usr/app/dbt/my_project",
        mounts=[
            {
                "source": "/home/klein/projet_yadari/dbt",
                "target": "/usr/app/dbt",
                "type": "bind"
            }
        ],
        mount_tmp_dir=False,
        network_mode="projet_yadari_elt_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )



    task6 = DockerOperator(
        task_id="dbt_silver_prices",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run --select silver_prices --profiles-dir /usr/app/dbt/my_project --project-dir /usr/app/dbt/my_project",
        mounts=[
            {
                "source": "/home/klein/projet_yadari/dbt",
                "target": "/usr/app/dbt",
                "type": "bind"
            }
        ],
        mount_tmp_dir=False,
        network_mode="projet_yadari_elt_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",    
    )

    task7 = DockerOperator(
        task_id="dbt_gold",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run --select gold_consumption_vs_price --profiles-dir /usr/app/dbt/my_project --project-dir /usr/app/dbt/my_project",
        mounts=[
            {
                "source": "/home/klein/projet_yadari/dbt",
                "target": "/usr/app/dbt",
                "type": "bind"
            }
        ],
        mount_tmp_dir=False,
        network_mode="projet_yadari_elt_network",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )


    [task1,task2] >> [task3,task4] >> [task5,task6] >> task7