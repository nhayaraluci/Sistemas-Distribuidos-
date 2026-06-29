from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("KafkaMetricsStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("evento", StringType(), True),
    StructField("latencia", DoubleType(), True),
    StructField("retry_count", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)  # epoch seconds
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "metrics-topic") \
    .option("startingOffsets", "earliest") \
    .load()


json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")


df_clean = json_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

windowed = df_clean \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute")) \
    .agg(
        count("*").alias("throughput"),

        # HIT RATE
        (sum(when(col("evento") == "hit", 1).otherwise(0)).cast("double") /
         count("*").cast("double")).alias("hit_rate"),

        # RETRY RATE
        (sum(when(col("retry_count") > 0, 1).otherwise(0)).cast("double") /
         count("*").cast("double")).alias("retry_rate"),

        # LATENCIAS
        expr("percentile_approx(latencia, 0.5)").alias("p50_latency"),
        expr("percentile_approx(latencia, 0.95)").alias("p95_latency")
    )


query = windowed.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()