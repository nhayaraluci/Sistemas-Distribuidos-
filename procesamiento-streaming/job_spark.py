import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INPUT_TOPIC = "metrics-topic"



spark = SparkSession.builder \
    .appName("StreamingMetrics") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("service", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("latencia", DoubleType(), True),
    StructField("retry_count", IntegerType(), True),

    StructField("is_hit", IntegerType(), True),
    StructField("is_miss", IntegerType(), True),
    StructField("is_retry", IntegerType(), True),
    StructField("is_dlq", IntegerType(), True),
    StructField("is_response", IntegerType(), True),
    StructField("is_eviction", IntegerType(), True)
])



df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()



df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")



df = df.withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))



windowed = df.withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds")
    )



metrics = windowed.agg(

    
    
    count("*").alias("throughput"),

    
    
    expr("percentile_approx(latencia, 0.5)").alias("p50_latency"),
    expr("percentile_approx(latencia, 0.95)").alias("p95_latency"),

   
   
    (sum("is_hit") / count("*")).alias("hit_rate"),

    
    
    (sum("is_retry") / count("*")).alias("retry_rate")

)



query = metrics.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/checkpoints/metrics") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "metrics") \
    .outputMode("update") \
    .start()

query.awaitTermination()