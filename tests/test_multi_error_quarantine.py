
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.transformation.transform import build_basic_quarantine


def test_multi_error_quarantine():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestMultiErrorQuarantine")
        .getOrCreate()
    )

    try:
        data = [
            (-100, 2.0, 1, 0, 0, 0, 0, 0, 0)
        ]

        columns = [
            "shares",
            "global_sentiment_polarity",
            "weekday_is_monday",
            "weekday_is_tuesday",
            "weekday_is_wednesday",
            "weekday_is_thursday",
            "weekday_is_friday",
            "weekday_is_saturday",
            "weekday_is_sunday"
        ]

        df = spark.createDataFrame(data, columns)

        result = build_basic_quarantine(df).collect()

        assert len(result) == 1
        assert result[0]["error_reason"] == (
            "INVALID_SHARES|INVALID_SENTIMENT"
        )

        print("MULTI-ERROR UNIT TEST: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    test_multi_error_quarantine()
