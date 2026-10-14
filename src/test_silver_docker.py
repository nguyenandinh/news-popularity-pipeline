
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("NewsPipelineSilverTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

silver_path = "/app/data/processed/silver"

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(silver_path)
)

print("SILVER_ROW_COUNT:", df.count())
print("SILVER_COLUMN_COUNT:", len(df.columns))

print("SAMPLE_DATA:")
df.select("url", "shares", "batch_id").show(5, truncate=False)

spark.stop()
