from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("InspectSilverColumns")
    .config("spark.hadoop.fs.file.impl",
            "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()
)

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("data/processed/silver")
)

print("\nCHANNEL-RELATED COLUMNS:")
for column in df.columns:
    if "channel" in column.lower():
        print(column)

from pyspark.sql import functions as F

channel_cols = [
    c for c in df.columns
    if c.startswith("data_channel_is_")
]

df_check = df.withColumn(
    "channel_flag_count",
    sum(F.coalesce(F.col(c).cast("int"), F.lit(0)) for c in channel_cols)
)

df_check.groupBy("channel_flag_count").count().orderBy(
    "channel_flag_count"
).show()

df.select(
    "global_sentiment_polarity",
    "global_subjectivity"
).describe().show()

df_sentiment = df.withColumn(
    "sentiment_label",
    F.when(
        F.col("global_sentiment_polarity") < -0.05,
        "Negative"
    ).when(
        F.col("global_sentiment_polarity") > 0.05,
        "Positive"
    ).otherwise("Neutral")
)

df_sentiment.groupBy("sentiment_label").count().show()

print("\nDATE-RELATED COLUMNS:")

for column in df.columns:
    if (
        "date" in column.lower()
        or "weekday" in column.lower()
        or "timedelta" in column.lower()
    ):
        print(column)

weekday_cols = [
    c for c in df.columns
    if c.startswith("weekday_is_")
]

df_weekday = df.withColumn(
    "weekday_flag_count",
    sum(
        F.coalesce(F.col(c).cast("int"), F.lit(0))
        for c in weekday_cols
    )
)

print("\nWEEKDAY DISTRIBUTION:")

df_weekday.groupBy("weekday_flag_count") \
    .count() \
    .orderBy("weekday_flag_count") \
    .show()

print("\nGOLD FACT SOURCE SCHEMA:")

df.select(
    "url",
    "shares",
    "batch_id",
    "ingestion_timestamp"
).printSchema()

from pyspark.sql import functions as F

print("\nGOLD LOAD PRECHECK:")

df.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("url").alias("unique_urls"),
    F.sum(F.col("url").isNull().cast("int")).alias("null_urls"),
    F.sum(F.col("shares").isNull().cast("int")).alias("null_shares"),
    F.sum((F.col("shares") < 0).cast("int")).alias("negative_shares"),
    F.sum(
        (
            F.col("batch_id").isNull()
            | (F.length("batch_id") != 8)
        ).cast("int")
    ).alias("invalid_batch_ids"),
    F.sum(
        F.col("ingestion_timestamp").isNull().cast("int")
    ).alias("null_ingestion_timestamps")
).show(truncate=False)

spark.stop()