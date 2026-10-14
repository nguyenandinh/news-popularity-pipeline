
from psycopg2.extras import execute_values
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("LoadGold")
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem"
        )
        .getOrCreate()
    )

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet("data/processed/silver")
    )

    # Channel mapping
    df_gold = df.withColumn(
        "channel_name",
        F.when(F.col("data_channel_is_lifestyle") == 1, "Lifestyle")
        .when(F.col("data_channel_is_entertainment") == 1, "Entertainment")
        .when(F.col("data_channel_is_bus") == 1, "Business")
        .when(F.col("data_channel_is_socmed") == 1, "Social Media")
        .when(F.col("data_channel_is_tech") == 1, "Technology")
        .when(F.col("data_channel_is_world") == 1, "World")
        .otherwise("Unknown")
    )

    # Sentiment mapping
    df_gold = df_gold.withColumn(
        "sentiment_name",
        F.when(
            F.col("global_sentiment_polarity") < -0.05,
            "Negative"
        )
        .when(
            F.col("global_sentiment_polarity") > 0.05,
            "Positive"
        )
        .otherwise("Neutral")
    )

    # Weekday mapping
    df_gold = df_gold.withColumn(
        "weekday_name",
        F.when(F.col("weekday_is_monday") == 1, "Monday")
        .when(F.col("weekday_is_tuesday") == 1, "Tuesday")
        .when(F.col("weekday_is_wednesday") == 1, "Wednesday")
        .when(F.col("weekday_is_thursday") == 1, "Thursday")
        .when(F.col("weekday_is_friday") == 1, "Friday")
        .when(F.col("weekday_is_saturday") == 1, "Saturday")
        .otherwise("Sunday")
    )

    print("\nGOLD DIMENSION MAPPING SAMPLE:")

    df_gold.select(
        "url",
        "channel_name",
        "sentiment_name",
        "weekday_name",
        "shares"
    ).show(5, truncate=35)

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"]
    )

    dimension_queries = {
        "channel": """
            SELECT channel_id, channel_name
            FROM gold.dim_channel
        """,
        "sentiment": """
            SELECT sentiment_id, sentiment_name
            FROM gold.dim_sentiment
        """,
        "weekday": """
            SELECT weekday_id, weekday_name
            FROM gold.dim_weekday
        """
    }

    dimension_data = {}

    with conn.cursor() as cursor:
        for name, query in dimension_queries.items():
            cursor.execute(query)

            columns = [
                description[0]
                for description in cursor.description
            ]

            rows = cursor.fetchall()

            dimension_data[name] = spark.createDataFrame(
                rows,
                schema=columns
            )

    conn.close()

    print("\nPOSTGRESQL DIMENSION LOOKUP:")

    for name, dim_df in dimension_data.items():
        print(f"\nDimension: {name}")
        dim_df.show()

    # Lookup dimension keys
    df_fact = (
        df_gold
        .join(
            F.broadcast(dimension_data["channel"]),
            on="channel_name",
            how="left"
        )
        .join(
            F.broadcast(dimension_data["sentiment"]),
            on="sentiment_name",
            how="left"
        )
        .join(
            F.broadcast(dimension_data["weekday"]),
            on="weekday_name",
            how="left"
        )
    )

    # Select columns required by Gold Fact Table
    df_fact = df_fact.select(
        "url",
        "channel_id",
        "sentiment_id",
        "weekday_id",
        "shares",
        "batch_id",
        "ingestion_timestamp"
    )

    print("\nGOLD FACT MAPPING SAMPLE:")

    df_fact.show(5, truncate=35)

    print("\nGOLD FOREIGN KEY VALIDATION:")

    df_fact.select(
        F.count("*").alias("total_rows"),
        F.sum(
            F.col("channel_id").isNull().cast("int")
        ).alias("null_channel_ids"),
        F.sum(
            F.col("sentiment_id").isNull().cast("int")
        ).alias("null_sentiment_ids"),
        F.sum(
            F.col("weekday_id").isNull().cast("int")
        ).alias("null_weekday_ids")
    ).show(truncate=False)

    # Prepare records for PostgreSQL
    records = [
        (
            row["url"],
            row["channel_id"],
            row["sentiment_id"],
            row["weekday_id"],
            row["shares"],
            row["batch_id"],
            row["ingestion_timestamp"]
        )
        for row in df_fact.collect()
    ]

    upsert_sql = """
        INSERT INTO gold.fact_news_popularity (
            url,
            channel_id,
            sentiment_id,
            weekday_id,
            shares,
            batch_id,
            ingestion_timestamp
        )
        VALUES %s

        ON CONFLICT (url)
        DO UPDATE SET
            channel_id = EXCLUDED.channel_id,
            sentiment_id = EXCLUDED.sentiment_id,
            weekday_id = EXCLUDED.weekday_id,
            shares = EXCLUDED.shares,
            batch_id = EXCLUDED.batch_id,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp

        WHERE
            (
                EXCLUDED.ingestion_timestamp,
                EXCLUDED.batch_id
            )
            >
            (
                gold.fact_news_popularity.ingestion_timestamp,
                gold.fact_news_popularity.batch_id
            );
    """

    print(f"\nPrepared {len(records)} records for Gold UPSERT.")

    # Execute Gold UPSERT
    conn = None

    try:
        conn = psycopg2.connect(
            host=os.environ["PGHOST"],
            port=os.environ["PGPORT"],
            dbname=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"]
        )

        with conn:
            with conn.cursor() as cursor:
                execute_values(
                    cursor,
                    upsert_sql,
                    records,
                    page_size=500
                )

                cursor.execute("""
                    SELECT COUNT(*)
                    FROM gold.fact_news_popularity
                """)

                total_gold = cursor.fetchone()[0]

        print("\nGOLD LOAD RESULT:")
        print(f"Source records: {len(records)}")
        print(f"Gold fact records: {total_gold}")
        print("Transaction committed successfully.")

    except Exception:
        print("\nGold UPSERT failed. Transaction rolled back.")
        raise

    finally:
        if conn is not None:
            conn.close()

    spark.stop()
if __name__ == "__main__":
    main()