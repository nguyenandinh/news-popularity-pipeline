from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from operators.gold_docker_operator import GoldDockerOperator


IMAGE = "news-popularity-pipeline-etl:latest"
NETWORK = "news-popularity-pipeline_default"

MOUNTS = [
    Mount(
        source="D:/news-popularity-pipeline/src",
        target="/app/src",
        type="bind",
        read_only=True,
    ),
    Mount(
        source="D:/news-popularity-pipeline/data",
        target="/app/data",
        type="bind",
        read_only=False,
    ),
]

DOCKER_CONFIG = {
    "image": IMAGE,
    "api_version": "auto",
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": NETWORK,
    "mount_tmp_dir": False,
    "mounts": MOUNTS,
}

with DAG(
    dag_id="news_silver_to_gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["news-pipeline", "etl"],
):

    transform_tasks = []

    for batch_id in ["20260101", "20260102", "20260103"]:
        task = DockerOperator(
            task_id=f"transform_{batch_id}",
            command=(
                f"python /app/src/transformation/transform.py {batch_id}"
            ),
            **DOCKER_CONFIG,
        )

        transform_tasks.append(task)

    load_gold = GoldDockerOperator(
        task_id="load_gold",
        command="python /app/src/loading/load_gold.py",
        **DOCKER_CONFIG,
    )

    transform_tasks >> load_gold