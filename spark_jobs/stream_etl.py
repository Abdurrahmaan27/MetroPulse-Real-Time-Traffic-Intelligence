from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("city_firehose_stream")
    .config("spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "io.delta:delta-core_2.12:2.4.0"
            ]))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("DEBUG")

# Traffic schema
schema_t = """
event_time STRING,
segment_id STRING,
speed_kmh DOUBLE,
congestion_pct DOUBLE,
source STRING
"""

# Air schema
schema_a = """
event_time STRING,
station_id STRING,
pm25 DOUBLE,
pm10 DOUBLE,
no2 DOUBLE,
source STRING
"""

# Read from Kafka
traffic_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "sensors.traffic.v1")
    .option("startingOffsets", "latest")
    .load()
)
air_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "sensors.air.v1")
    .option("startingOffsets", "latest")
    .load()
)


# Parse JSON messages
traffic = (
    traffic_raw
    .select(F.from_json(F.col("value").cast("string"), schema_t).alias("data"))
    .select("data.*")
    .withColumn("event_ts", F.to_timestamp("event_time"))
    .withWatermark("event_ts", "10 minutes")
)

air = (
    air_raw
    .select(F.from_json(F.col("value").cast("string"), schema_a).alias("data"))
    .select("data.*")
    .withColumn("event_ts", F.to_timestamp("event_time"))
    .withWatermark("event_ts", "10 minutes")
)

# Write to Bronze
(
    traffic.writeStream.format("delta")
    .option("checkpointLocation", "deltas/bronze/traffic/_chk")
    .start("deltas/bronze/traffic")
)

(
    air.writeStream.format("delta")
    .option("checkpointLocation", "deltas/bronze/air/_chk")
    .start("deltas/bronze/air")
)

# Deduplicate → Silver
traffic_silver = (
    traffic.dropDuplicates(["segment_id", "event_ts"])
    .withColumn("ingested_at", F.current_timestamp())
)

air_silver = (
    air.dropDuplicates(["station_id", "event_ts"])
    .withColumn("ingested_at", F.current_timestamp())
)

(
    traffic_silver.writeStream.format("delta")
    .option("checkpointLocation", "deltas/silver/traffic/_chk")
    .start("deltas/silver/traffic")
)

(
    air_silver.writeStream.format("delta")
    .option("checkpointLocation", "deltas/silver/air/_chk")
    .start("deltas/silver/air")
)

spark.streams.awaitAnyTermination()
