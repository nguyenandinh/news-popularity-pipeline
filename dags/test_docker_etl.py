from docker.types import Mount
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# airflow DAG
with DAG(
    dag_id="test_docker_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["news-pipeline", "docker-test"],
):
    test_etl_container = DockerOperator(
        task_id="test_etl_container",
        image="news-popularity-pipeline-etl:latest",
        api_version="auto",
        auto_remove="success",
        command="python /app/src/test_silver_docker.py",
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
