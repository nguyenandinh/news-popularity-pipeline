
from airflow.sdk import Connection
from airflow.providers.docker.operators.docker import DockerOperator


class GoldDockerOperator(DockerOperator):
    def execute(self, context):
        conn = Connection.get("news_gold_postgres")

        self._private_environment = {
            **self._private_environment,
            "PGHOST": conn.host,
            "PGPORT": str(conn.port or 5432),
            "PGDATABASE": conn.schema,
            "PGUSER": conn.login,
            "PGPASSWORD": conn.password,
        }

        return super().execute(context)
