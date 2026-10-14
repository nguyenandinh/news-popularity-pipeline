
FROM python:3.11-slim-bookworm

# Install Java 17 for PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.7 \
    psycopg2-binary \
    boto3 \
    "botocore[crt]"

WORKDIR /app
