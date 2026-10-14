
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="test_gold_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["news-pipeline", "connection-test"],
):

    @task
    def check_connection():
        from airflow.sdk import Connection

        conn = Connection.get("news_gold_postgres")

        print("CONNECTION_FOUND:", conn is not None)
        print("HOST_MATCH:", conn.host == "host.docker.internal")
        print("PASSWORD_AVAILABLE:", bool(conn.password))

    check_connection()
