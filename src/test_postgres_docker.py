
import os
import psycopg2

conn = psycopg2.connect(
    host=os.environ.get("PGHOST", "host.docker.internal"),
    port=os.environ.get("PGPORT", "5432"),
    dbname=os.environ.get("PGDATABASE", "news_popularity_dw"),
    user=os.environ.get("PGUSER", "postgres"),
    password=os.environ["PGPASSWORD"],
    connect_timeout=10,
)

try:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gold.fact_news_popularity")
        count = cur.fetchone()[0]
        print("POSTGRES_CONNECTION: SUCCESS")
        print("GOLD_ROW_COUNT:", count)
finally:
    conn.close()
