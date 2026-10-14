
from datetime import datetime

from pyspark.sql import SparkSession
from src.transformation.transform import deduplicate_url


def test_deduplicate_url():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestDeduplicateURL")
        .getOrCreate()
    )

    try:
        data = [
            ("url-A", 100, datetime(2026, 1, 2), "20260102"),
            ("url-A", 200, datetime(2026, 1, 1), "20260101"),
            ("url-B", 300, datetime(2026, 1, 1), "20260101"),
            ("url-B", 300, datetime(2026, 1, 2), "20260102"),
            ("url-C", 400, datetime(2026, 1, 2), "20260101"),
            ("url-C", 400, datetime(2026, 1, 2), "20260102"),
        ]

        columns = [
            "url",
            "shares",
            "ingestion_timestamp",
            "batch_id"
        ]

        df = spark.createDataFrame(data, columns)

        unique_df, duplicate_df = deduplicate_url(df)

        result = {
            row["url"]: row
            for row in unique_df.collect()
        }

        assert len(result) == 3
        assert duplicate_df.count() == 3

        assert result["url-A"]["shares"] == 200
        assert result["url-B"]["ingestion_timestamp"] == datetime(2026, 1, 2)
        assert result["url-C"]["batch_id"] == "20260102"

        print("DEDUPLICATION UNIT TEST: PASS")

    finally:
        spark.stop()


def test_hash_tiebreaker():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestHashTieBreaker")
        .getOrCreate()
    )

    try:
        data = [
            ("url-D", 500, datetime(2026, 1, 2), "20260102", "Article A"),
            ("url-D", 500, datetime(2026, 1, 2), "20260102", "Article B"),
        ]

        columns = [
            "url",
            "shares",
            "ingestion_timestamp",
            "batch_id",
            "title"
        ]

        df = spark.createDataFrame(data, columns)

        unique_1, _ = deduplicate_url(df)
        unique_2, _ = deduplicate_url(
            df.orderBy("title", ascending=False)
        )

        result_1 = unique_1.select("title").first()["title"]
        result_2 = unique_2.select("title").first()["title"]

        assert result_1 == result_2

        print("HASH TIE-BREAKER TEST: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_deduplicate_url()
    test_hash_tiebreaker()
