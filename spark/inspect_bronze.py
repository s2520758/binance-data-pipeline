from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("InspectBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("../data/bronze_trade_event")

print("=== SCHEMA ===")
df.printSchema()

print("=== SAMPLE ROWS ===")
df.show(5, truncate=False)

spark.stop()
