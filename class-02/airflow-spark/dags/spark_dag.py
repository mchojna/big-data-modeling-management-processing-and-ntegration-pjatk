from __future__ import annotations
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

APP_DIR = "/app"
INPUT = f"{APP_DIR}/data/input.txt"
OUTPUT = f"{APP_DIR}/data/out"

# Host path for mounts: docker-compose passes PROJECT_DIR from the host
HOST_PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow")
HOST_JOBS_DIR = os.path.join(HOST_PROJECT_DIR, "jobs")

with DAG(
    dag_id="spark_wordcount_in_docker",
    description="Uruchom spark-submit w kontenerze docker (local[*])",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spark", "docker"],
) as dag:

    run_spark_job = DockerOperator(
        task_id="spark_wordcount",
        image="apache/spark:3.5.1-python3",
        mounts=[
            Mount(
                source=HOST_JOBS_DIR,  # absolute host path to jobs folder
                target=APP_DIR,        # path inside Spark container
                type="bind",
                read_only=False,
            )
        ],
        command=(
            "bash -lc "
            f"\"/opt/spark/bin/spark-submit --master local[*] "
            f"/app/wordcount.py {INPUT} {OUTPUT}\""
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=True,
    )
