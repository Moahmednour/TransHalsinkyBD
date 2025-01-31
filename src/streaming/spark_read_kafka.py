from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, LongType, ArrayType, StructField

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka-Streaming-Transport") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka Broker Configuration
KAFKA_BROKER = "spark-kafka-kafka-1:9092"

# Schema for trip-updates
trip_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("start_time", StringType(), True),  # Now in HH:MM format
    StructField("start_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("stop_time_updates", ArrayType(StructType([
        StructField("stop_id", StringType(), True),
        StructField("arrival_time", LongType(), True),
        StructField("departure_time", LongType(), True)
    ])))
])

# Schema for vehicle positions (mqtt-data)
vehicle_schema = StructType([
    StructField("vehicle_id", IntegerType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("operator", IntegerType(), True),
    StructField("route", StringType(), True),
    StructField("start_time", StringType(), True),  # Now in HH:MM format
    StructField("start_date", StringType(), True),  # Ensured format consistency
    StructField("speed", DoubleType(), True),
    StructField("occupancy", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("heading", IntegerType(), True),
        StructField("accuracy", DoubleType(), True)
    ])),
    StructField("odometer", DoubleType(), True),
    StructField("location_source", StringType(), True)
])

# Schema for weather data
weather_schema = StructType([
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True)
    ])),
    StructField("cloud_coverage", IntegerType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("visibility", StringType(), True),
    StructField("weather_main", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("date", StringType(), True),  # Extracted date
    StructField("time", StringType(), True),
    StructField("sunrise", LongType(), True),
    StructField("sunset", LongType(), True)
])

# Read trip-updates from Kafka
trip_updates_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "trip-updates") \
    .load() \
    .selectExpr("CAST(value AS STRING) as kafka_message") \
    .select(from_json(col("kafka_message"), trip_schema).alias("trip"))

# Read vehicle positions (mqtt-data) from Kafka
vehicle_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "mqtt-data") \
    .load() \
    .selectExpr("CAST(value AS STRING) as kafka_message") \
    .select(from_json(col("kafka_message"), vehicle_schema).alias("vehicle"))

# Read weather data from Kafka
weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "weather-data") \
    .load() \
    .selectExpr("CAST(value AS STRING) as kafka_message") \
    .select(from_json(col("kafka_message"), weather_schema).alias("weather"))

# Format output for structured display
trip_updates_df = trip_updates_df.select("trip.*")
vehicle_df = vehicle_df.select("vehicle.*")
weather_df = weather_df.select("weather.*")

# Write to Console to Verify
trip_updates_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

vehicle_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

spark.streams.awaitAnyTermination()
