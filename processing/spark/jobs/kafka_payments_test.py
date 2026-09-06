from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


payment_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
])


spark = (
    SparkSession.builder
    .appName("StreamCartKafkaPaymentTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


payments_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "streamcart.payments")
    .option("startingOffsets", "earliest")
    .load()
)


payments_string = payments_stream.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)


payments_parsed = payments_string.select(
    from_json(
        col("value"),
        payment_schema,
    ).alias("payment"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
)


payments_final = payments_parsed.select(
    "payment.*",
    "topic",
    "partition",
    "offset",
    "timestamp",
)

query = (
    payments_final
    .writeStream
    .format("console")
    .option("truncate", "false")
    .start()
)


query.awaitTermination()

