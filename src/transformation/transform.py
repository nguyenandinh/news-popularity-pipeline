import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    return (
        SparkSession.builder
        .appName("NewsPopularityTransformation")
        .master("local[1]")
        .config(
            "spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem"
        )
        .getOrCreate()
    )


def read_bronze(spark, file_path):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    return df


def standardize_schema(df):
    # Remove whitespace from column names
    df = df.toDF(*[col.strip() for col in df.columns])

    # Standardize metadata columns
    df = (
        df.withColumn(
            "batch_id",
            F.col("batch_id").cast("string")
        )
        .withColumn(
            "ingestion_timestamp",
            F.col("ingestion_timestamp").cast("timestamp")
        )
    )

    return df


def validate_shares(df):
    invalid_condition = (
        F.col("shares").isNull()
        | (F.col("shares") <= 0)
    )

    invalid_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    return valid_df, invalid_df


def validate_sentiment(df):
    invalid_condition = (
        F.col("global_sentiment_polarity").isNull()
        | (F.col("global_sentiment_polarity") < -1)
        | (F.col("global_sentiment_polarity") > 1)
    )

    invalid_df = df.filter(invalid_condition)
    valid_df = df.filter(~invalid_condition)

    return valid_df, invalid_df


def validate_weekday(df):
    weekday_columns = [
        "weekday_is_monday",
        "weekday_is_tuesday",
        "weekday_is_wednesday",
        "weekday_is_thursday",
        "weekday_is_friday",
        "weekday_is_saturday",
        "weekday_is_sunday"
    ]

    weekday_sum = sum(
        (F.col(column) for column in weekday_columns),
        F.lit(0)
    )

    valid_values = F.lit(True)

    for column in weekday_columns:
        valid_values = valid_values & (
            F.col(column).isin(0, 1)
        )

    valid_condition = valid_values & (weekday_sum == 1)

    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~F.coalesce(valid_condition, F.lit(False)))

    return valid_df, invalid_df


def validate_duplicate_url(df):
    total_rows = df.count()

    unique_urls = df.select("url").distinct().count()

    duplicate_rows = total_rows - unique_urls

    return total_rows, unique_urls, duplicate_rows



def validate_basic_quality(df):
    valid_shares, _ = validate_shares(df)
    valid_sentiment, _ = validate_sentiment(valid_shares)
    valid_df, _ = validate_weekday(valid_sentiment)

    return valid_df


def deduplicate_url(df):
    # Create a stable hash based on record content
    hash_columns = sorted(df.columns)

    df_with_hash = df.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(c) for c in hash_columns])
            ),
            256
        )
    )

    window_spec = Window.partitionBy("url").orderBy(
        F.col("shares").desc(),
        F.col("ingestion_timestamp").desc(),
        F.col("batch_id").desc(),
        F.col("_record_hash").asc()
    )


    ranked_df = df_with_hash.withColumn(
        "_row_number",
        F.row_number().over(window_spec)
    )

    unique_df = (
        ranked_df
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "_record_hash")
    )

    duplicate_df = (
        ranked_df
        .filter(F.col("_row_number") > 1)
        .drop("_row_number", "_record_hash")
    )

    return unique_df, duplicate_df


def build_basic_quarantine(df):
    weekday_columns = [
        "weekday_is_monday",
        "weekday_is_tuesday",
        "weekday_is_wednesday",
        "weekday_is_thursday",
        "weekday_is_friday",
        "weekday_is_saturday",
        "weekday_is_sunday"
    ]

    weekday_sum = sum(
        (F.col(column) for column in weekday_columns),
        F.lit(0)
    )

    valid_weekday = (weekday_sum == 1)

    for column in weekday_columns:
        valid_weekday = valid_weekday & F.col(column).isin(0, 1)

    quarantine_df = df.withColumn(
        "error_reason",
        F.concat_ws(
            "|",
            F.when(
                F.col("shares").isNull() | (F.col("shares") <= 0),
                "INVALID_SHARES"
            ),
            F.when(
                F.col("global_sentiment_polarity").isNull()
                | (F.col("global_sentiment_polarity") < -1)
                | (F.col("global_sentiment_polarity") > 1),
                "INVALID_SENTIMENT"
            ),
            F.when(
                ~F.coalesce(valid_weekday, F.lit(False)),
                "INVALID_WEEKDAY"
            )
        )
    )

    return quarantine_df.filter(F.col("error_reason") != "")

    unique_df = (
        ranked_df
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    duplicate_df = (
        ranked_df
        .filter(F.col("row_number") > 1)
        .drop("row_number")
    )

    return unique_df, duplicate_df



def build_basic_quarantine(df):
    weekday_columns = [
        "weekday_is_monday",
        "weekday_is_tuesday",
        "weekday_is_wednesday",
        "weekday_is_thursday",
        "weekday_is_friday",
        "weekday_is_saturday",
        "weekday_is_sunday"
    ]

    weekday_sum = sum(
        (F.col(column) for column in weekday_columns),
        F.lit(0)
    )

    valid_weekday = (weekday_sum == 1)

    for column in weekday_columns:
        valid_weekday = valid_weekday & F.col(column).isin(0, 1)

    quarantine_df = df.withColumn(
        "error_reason",
        F.concat_ws(
            "|",
            F.when(
                F.col("shares").isNull() | (F.col("shares") <= 0),
                "INVALID_SHARES"
            ),
            F.when(
                F.col("global_sentiment_polarity").isNull()
                | (F.col("global_sentiment_polarity") < -1)
                | (F.col("global_sentiment_polarity") > 1),
                "INVALID_SENTIMENT"
            ),
            F.when(
                ~F.coalesce(valid_weekday, F.lit(False)),
                "INVALID_WEEKDAY"
            )
        )
    )

    return quarantine_df.filter(F.col("error_reason") != "")



def reconcile_data(bronze_df, silver_df, quarantine_df):
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    quarantine_count = quarantine_df.count()

    is_balanced = (
        bronze_count == silver_count + quarantine_count
    )

    print("BRONZE COUNT:", bronze_count)
    print("SILVER COUNT:", silver_count)
    print("QUARANTINE COUNT:", quarantine_count)
    print("RECONCILIATION:", "PASS" if is_balanced else "FAIL")

    if not is_balanced:
        raise ValueError("Data reconciliation failed")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        batch_id = sys.argv[1] if len(sys.argv) > 1 else "20260101"
        input_path = f"data/generated/batch_{batch_id}.csv"
        bronze_df = read_bronze(spark, input_path)
        silver_candidate_df = standardize_schema(bronze_df)

        valid_df, invalid_df = validate_shares(silver_candidate_df)
        print("VALID SHARES:", valid_df.count())
        print("INVALID SHARES:", invalid_df.count())
        invalid_df.select("url", "shares").show(5, truncate=False)
        print("BRONZE ROWS:", bronze_df.count())
        print("STANDARDIZED ROWS:", silver_candidate_df.count())
        print("BATCH ID TYPE:", silver_candidate_df.schema["batch_id"].dataType)
        print(
            "INGESTION TIMESTAMP TYPE:",
            silver_candidate_df.schema["ingestion_timestamp"].dataType
        )

        # Combined validation: Shares + Sentiment + Weekday
        basic_valid_df = validate_basic_quality(silver_candidate_df)

        print("BASIC VALID ROWS:", basic_valid_df.count())
        print(
            "BASIC INVALID ROWS:",
            silver_candidate_df.count() - basic_valid_df.count()
        )

        # Check duplicate URLs after basic quality validation
        total_valid, unique_valid_urls, duplicate_valid = validate_duplicate_url(
            basic_valid_df
        )

        print("AFTER BASIC VALIDATION:", total_valid)
        print("UNIQUE VALID URLS:", unique_valid_urls)
        print("REMAINING DUPLICATES:", duplicate_valid)

        # Deduplicate valid records
        unique_df, duplicate_df = deduplicate_url(basic_valid_df)

        print("UNIQUE RECORDS:", unique_df.count())
        print("DUPLICATE RECORDS:", duplicate_df.count())

        # Build basic quarantine
        basic_quarantine_df = build_basic_quarantine(
            silver_candidate_df
        )

        print("BASIC QUARANTINE ROWS:", basic_quarantine_df.count())

        basic_quarantine_df.groupBy(
            "error_reason"
        ).count().show()


        # Add duplicate records to quarantine
        duplicate_quarantine_df = duplicate_df.withColumn(
            "error_reason",
            F.lit("DUPLICATE_URL")
        )

        quarantine_df = basic_quarantine_df.unionByName(
            duplicate_quarantine_df
        )

        print("TOTAL QUARANTINE ROWS:", quarantine_df.count())

        quarantine_df.groupBy(
            "error_reason"
        ).count().show()

        # Reconcile Bronze, Silver and Quarantine
        reconcile_data(
            bronze_df,
            unique_df,
            quarantine_df
        )

        # Write Silver Layer to local Parquet
        silver_output_path = f"data/processed/silver/batch_id={batch_id}"
        (
            unique_df.write
            .mode("overwrite")
            .parquet(silver_output_path)
        )

        print("SILVER PARQUET WRITTEN:", silver_output_path)


        # Read back Silver Parquet for verification
        silver_readback_df = spark.read.parquet(silver_output_path)

        print("SILVER READBACK ROWS:", silver_readback_df.count())
        print("SILVER READBACK COLUMNS:", len(silver_readback_df.columns))

        readback_duplicates = (
            silver_readback_df.count()
            - silver_readback_df.select("url").distinct().count()
        )

        print("SILVER READBACK DUPLICATES:", readback_duplicates)

        silver_readback_df.select(
            "url",
            "shares",
            "batch_id",
            "ingestion_timestamp"
        ).show(5, truncate=False)


        # Write Quarantine to local Parquet
        quarantine_output_path = f"data/processed/quarantine/batch_id={batch_id}"
        (
            quarantine_df.write
            .mode("overwrite")
            .parquet(quarantine_output_path)
        )

        print("QUARANTINE PARQUET WRITTEN:", quarantine_output_path)


        # Read back Quarantine Parquet
        quarantine_readback_df = spark.read.parquet(
            quarantine_output_path
        )

        print(
            "QUARANTINE READBACK ROWS:",
            quarantine_readback_df.count()
        )

        print(
            "QUARANTINE READBACK COLUMNS:",
            len(quarantine_readback_df.columns)
        )

        quarantine_readback_df.groupBy(
            "error_reason"
        ).count().show()


        duplicate_df.select(
            "url",
            "shares"
        ).show(5, truncate=False)

        # Rule 2: Sentiment validation
        valid_sentiment_df, invalid_sentiment_df = validate_sentiment(
            silver_candidate_df
        )

        print("VALID SENTIMENT:", valid_sentiment_df.count())
        print("INVALID SENTIMENT:", invalid_sentiment_df.count())

        invalid_sentiment_df.select(
            "url",
            "global_sentiment_polarity"
        ).show(5, truncate=False)


        # Rule 3: Weekday one-hot validation
        valid_weekday_df, invalid_weekday_df = validate_weekday(
            silver_candidate_df
        )

        print("VALID WEEKDAY:", valid_weekday_df.count())
        print("INVALID WEEKDAY:", invalid_weekday_df.count())

        invalid_weekday_df.select(
            "url",
            "weekday_is_monday",
            "weekday_is_tuesday",
            "weekday_is_wednesday",
            "weekday_is_thursday",
            "weekday_is_friday",
            "weekday_is_saturday",
            "weekday_is_sunday"
        ).show(5, truncate=False)

        # Rule 4: Duplicate URL validation
        total_rows, unique_urls, duplicate_rows = validate_duplicate_url(
            silver_candidate_df
        )

        print("TOTAL URL ROWS:", total_rows)
        print("UNIQUE URLS:", unique_urls)
        print("DUPLICATE ROWS:", duplicate_rows)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
