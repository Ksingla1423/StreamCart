from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


order_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("currency", StringType(), True),
])
# KAFKA_PACKAGE = (
#     "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
# )


spark = (
    SparkSession.builder
    .appName("StreamCartKafkaTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# orders_stream = (
#     spark.readStream
#     .format("kafka")
#     .option(
#         "kafka.bootstrap.servers",
#         "kafka:29092",
#     )
#     .option(
#         "subscribe",
#         "streamcart.orders",
#     )
#     .option(
#         "startingOffsets",
#         "latest",
#     )
#     .load()
# )


orders_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "streamcart.orders")
    .option("startingOffsets", "earliest")
    .load()
)

orders_string = orders_stream.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)

orders_parsed = orders_string.select(
    from_json(
        col("value"),
        order_schema
    ).alias("order"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)


orders_final = orders_parsed.select(
    "order.*",
    "topic",
    "partition",
    "offset",
    "timestamp",
)

query = (
    orders_final
    .writeStream
    .format("console")
    .option(
        "truncate",
        "false",
    )
    .start()
)


query.awaitTermination()