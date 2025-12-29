from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType

kafka_bootstrap = "localhost:9092"
topic = "binance_trades_raw"
output_path = "../data/bronze_trade_event"
checkpoint_path = "../chk/bronze_trade_event"

schema = StructType(
    [
        StructField("stream", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("trade_id", LongType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("buyer_order_id", LongType(), True),
        StructField("seller_order_id", LongType(), True),
        StructField("trade_time", LongType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ignore", BooleanType(), True),
        StructField("ingest_time", LongType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("BinanceBronzeWriter")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

value_df = raw_df.select(col("key").cast("string"), col("value").cast("string").alias("json"))

parsed_df = value_df.select(
    col("key").alias("symbol_key"),
    from_json(col("json"), schema).alias("data"),
)

bronze_df = parsed_df.select("data.*")

query = (
    bronze_df.writeStream.outputMode("append")
    .format("parquet")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

query.awaitTermination()
