import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    when,
    count,
    sum,
    avg,
    max,
    expr,
    window,
    to_timestamp,
    from_unixtime
)
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType
)


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INPUT_TOPIC = "metrics-topic"


spark = (
    SparkSession.builder
    .appName("StreamingMetrics")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("tipo", StringType(), True),
    StructField("clave", StringType(), True),
    StructField("retry_count", IntegerType(), True),
    StructField("latencia", DoubleType(), True),
    StructField("service", StringType(), True)
])


raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


df = (
    raw.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("json"))
    .select("json.*")
)


df = df.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("timestamp")))
)

df = (
    df
    .withColumn("is_hit", when(col("tipo") == "hit", 1).otherwise(0))
    .withColumn("is_miss", when(col("tipo") == "miss", 1).otherwise(0))
    .withColumn("is_retry", when(col("tipo") == "retry", 1).otherwise(0))
    .withColumn("is_dlq", when(col("tipo") == "dlq", 1).otherwise(0))
    .withColumn("is_response", when(col("tipo") == "response", 1).otherwise(0))
    .withColumn("is_eviction", when(col("tipo") == "eviction", 1).otherwise(0))
)


windowed = (
    df
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds")
    )
)


metrics = windowed.agg(

    count("*").alias("throughput"),

    sum("is_hit").alias("hits"),
    sum("is_miss").alias("misses"),
    sum("is_retry").alias("retries"),
    sum("is_dlq").alias("dlq"),
    sum("is_response").alias("responses"),
    sum("is_eviction").alias("evictions"),

    avg("latencia").alias("avg_latency"),
    expr("percentile_approx(latencia,0.50)").alias("p50_latency"),
    expr("percentile_approx(latencia,0.95)").alias("p95_latency"),
    max("latencia").alias("max_latency"),

    avg("retry_count").alias("avg_retry"),
    max("retry_count").alias("max_retry")
)


metrics = (
    metrics
    .withColumn(
        "hit_rate",
        when(
            (col("hits") + col("misses")) > 0,
            col("hits") / (col("hits") + col("misses"))
        ).otherwise(0.0)
    )
    .withColumn(
        "miss_rate",
        when(
            (col("hits") + col("misses")) > 0,
            col("misses") / (col("hits") + col("misses"))
        ).otherwise(0.0)
    )
    .withColumn(
        "retry_rate",
        when(col("throughput") > 0,
             col("retries") / col("throughput")
        ).otherwise(0.0)
    )
    .withColumn(
        "dlq_rate",
        when(col("throughput") > 0,
             col("dlq") / col("throughput")
        ).otherwise(0.0)
    )
)
elastic = (
    metrics.writeStream
    .format("org.elasticsearch.spark.sql")
    .outputMode("append")  
    .option("checkpointLocation", "/tmp/checkpoints/metrics")
    .option("es.nodes", "elasticsearch")
    .option("es.port", "9200")
    .option("es.nodes.wan.only", "true")
    .option("es.resource", "metrics/_doc")
)

debug = (
    metrics.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

spark.streams.awaitAnyTermination()