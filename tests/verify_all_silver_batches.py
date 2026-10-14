
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("VerifyAllSilverBatches")
    .getOrCreate()
)

try:
    silver_df = spark.read.option(
    "recursiveFileLookup", "true"
).parquet(
    "data/processed/silver"
)

    print("TOTAL SILVER ROWS:", silver_df.count())

    silver_df.groupBy("batch_id").count().orderBy(
        "batch_id"
    ).show()

    duplicate_count = (
        silver_df.count()
        - silver_df.select("url").distinct().count()
    )

    print("CROSS-BATCH DUPLICATE URLS:", duplicate_count)

finally:
    spark.stop()
