
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("NewsPipelineDockerTest")
    .getOrCreate()
)

print("SPARK_VERSION:", spark.version)
print("ROW_COUNT:", spark.range(10).count())

spark.stop()
