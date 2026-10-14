
from datetime import datetime

from airflow.sdk import DAG
from docker.types import Mount

from operators.gold_docker_operator import GoldDockerOperator


with DAG(
    dag_id="news_gold_loader",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["news-pipeline", "gold"],
):
    load_gold = GoldDockerOperator(
        task_id="load_gold",
        image="news-popularity-pipeline-etl:latest",
        api_version="auto",
        auto_remove="success",
        command="python /app/src/loading/load_gold.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="news-popularity-pipeline_default",
        mount_tmp_dir=False,
        mounts=[
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
                read_only=True,
            ),
        ],
    )
