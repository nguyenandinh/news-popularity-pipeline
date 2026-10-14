
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="hello_news_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["news-pipeline", "learning"],
):

    @task
    def check_environment():
        print("Airflow environment is ready!")
        return "Environment OK"

    @task
    def check_pipeline():
        print("News Popularity Pipeline initialized!")
        return "Pipeline OK"

    check_pipeline_task = check_pipeline()
    check_environment_task = check_environment()

    check_environment_task >> check_pipeline_task
