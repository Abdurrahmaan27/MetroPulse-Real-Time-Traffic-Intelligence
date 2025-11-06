from pyspark.sql import SparkSession, functions as F

# Spark + Delta via Maven (works from venv)
spark = (
    SparkSession.builder
    .appName("city_firehose_anomaly_window")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Read Silver stream
silver = spark.readStream.format("delta").load("deltas/silver/traffic")

# Ensure we have a timestamp column for windows
traffic = silver.withColumn(
    "event_time_ts",
    F.coalesce(F.col("event_ts"), F.to_timestamp("event_time"))
)

# Compute rolling stats per segment on a 10-minute tumbling window
stats = (
    traffic
    .withWatermark("event_time_ts", "15 minutes")
    .groupBy(
        "segment_id",
        F.window("event_time_ts", "10 minutes").alias("w")
    )
    .agg(
        F.avg("speed_kmh").alias("mu"),
        F.stddev("speed_kmh").alias("sigma")
    )
)

# Join each record to its window stats
joined = (
    traffic.withWatermark("event_time_ts", "15 minutes")
    .join(
        stats,
        [
            "segment_id",
            (traffic.event_time_ts >= F.col("w.start")) &
            (traffic.event_time_ts <  F.col("w.end"))
        ],
        "left"
    )
)

# Z-score and anomaly flag (2σ for more hits)
anomalies = (
    joined
    .withColumn("sigma_nz", F.when((F.col("sigma").isNull()) | (F.col("sigma") == 0), None).otherwise(F.col("sigma")))
    .withColumn("zspeed", F.when(F.col("sigma_nz").isNotNull(), F.abs(F.col("speed_kmh") - F.col("mu")) / F.col("sigma_nz")))
    .withColumn("is_anomaly", F.col("zspeed") > F.lit(2.0))
    .filter(F.col("is_anomaly") == True)
    .withColumn("detected_at", F.current_timestamp())
    .drop("w", "sigma_nz")
)

# Write Gold stream
query = (
    anomalies.writeStream
    .format("delta")
    .option("checkpointLocation", "deltas/gold/traffic_anom/_chk")
    .outputMode("append")
    .start("deltas/gold/traffic_anom")
)

query.awaitTermination()
