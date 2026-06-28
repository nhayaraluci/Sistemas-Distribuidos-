from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, expr,
    from_unixtime, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("StreamingMetricasFinal") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# ESQUEMA JSON
# =========================
schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("evento", StringType(), True),
    StructField("latencia", DoubleType(), True),
    StructField("retry_count", IntegerType(), True)
])

# =========================
# KAFKA SOURCE
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "metrics-topic") \
    .option("startingOffsets", "latest") \
    .load()

# =========================
# PARSE JSON
# =========================
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# =========================
# TIMESTAMP SPARK
# =========================
stream = json_df.withColumn(
    "event_time",
    to_timestamp(from_unixtime(col("timestamp")))
)

# =========================
# WINDOW 1 MINUTO
# =========================
agg = stream.groupBy(
    window(col("event_time"), "1 minute")
).agg(

    expr("count(*)").alias("throughput"),

    expr("percentile_approx(latencia, 0.5)").alias("p50"),
    expr("percentile_approx(latencia, 0.95)").alias("p95"),

    expr("avg(case when evento = 'hit' then 1 else 0 end)").alias("hit_rate"),

    expr("avg(case when retry_count > 0 then 1 else 0 end)").alias("retry_rate")
)

# =========================
# OUTPUT FINAL
# =========================
output = agg.select(
    col("window.start").alias("inicio_ventana"),
    col("window.end").alias("fin_ventana"),
    col("throughput"),
    col("p50"),
    col("p95"),
    col("hit_rate"),
    col("retry_rate")
)

# =========================
# STREAM OUTPUT
# =========================
query = output.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()