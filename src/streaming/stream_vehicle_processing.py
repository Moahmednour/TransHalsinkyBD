import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, date_format, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# PostgreSQL Railway Database Configuration
DB_URL = "jdbc:postgresql://monorail.proxy.rlwy.net:59712/railway"
DB_USER = "postgres"
DB_PASSWORD = "nQTzRNmlaOMRrWEWQDLbXgDiYsjGPmOb"
DB_TABLE = "vehicle_positions"

# Kafka Configuration
KAFKA_BROKER = "spark-kafka-kafka-1:9092"
KAFKA_TOPIC = "mqtt-data"

# Define Schema for Kafka JSON messages
vehicle_schema = StructType() \
    .add("vehicle_id", IntegerType()) \
    .add("vehicle_type", StringType()) \
    .add("operator", IntegerType()) \
    .add("route", StringType()) \
    .add("direction", StringType()) \
    .add("start_date", StringType()) \
    .add("start_time", StringType()) \
    .add("destination", StringType()) \
    .add("journey_id", IntegerType()) \
    .add("line", IntegerType()) \
    .add("timestamp", StringType()) \
    .add("timestamp_seconds", IntegerType()) \
    .add("delay", IntegerType()) \
    .add("speed", DoubleType()) \
    .add("location", StructType()
         .add("latitude", DoubleType())
         .add("longitude", DoubleType())
         .add("speed", DoubleType())
         .add("heading", IntegerType())
         .add("accuracy", DoubleType())) \
    .add("occupancy", IntegerType()) \
    .add("stop", IntegerType()) \
    .add("odometer", DoubleType()) \
    .add("location_source", StringType()) \
    .add("mqtt_topic", StringType())

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaVehicleStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.2.24") \
    .getOrCreate()

# Read data from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON messages
vehicle_df = kafka_df.selectExpr("CAST(value AS STRING) as kafka_message") \
    .select(from_json(col("kafka_message"), vehicle_schema).alias("data"))

# Extract relevant fields and transform date formats
processed_df = vehicle_df.select(
    col("data.vehicle_id"),
    col("data.vehicle_type"),
    col("data.operator"),
    col("data.route"),
    col("data.direction"),
    date_format(to_timestamp(col("data.start_time"), "HH:mm"), "HH:mm:00").alias("start_time"),
    date_format(to_timestamp(col("data.start_date"), "yyyy-MM-dd"), "yyyyMMdd").alias("start_date"),
    col("data.destination"),
    col("data.journey_id"),
    col("data.line"),
    col("data.timestamp"),
    col("data.timestamp_seconds"),
    col("data.delay"),
    col("data.speed"),
    col("data.location.latitude").alias("latitude"),
    col("data.location.longitude").alias("longitude"),
    col("data.location.speed").alias("vehicle_speed"),
    col("data.location.heading").alias("heading"),
    col("data.location.accuracy").alias("accuracy"),
    col("data.occupancy"),
    col("data.stop"),
    col("data.odometer"),
    col("data.location_source"),
    col("data.mqtt_topic"),
    current_timestamp().alias("processed_time")
)

# Write processed data to Railway PostgreSQL
processed_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", DB_TABLE) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .mode("append") \
        .save()) \
    .start()

# Output transformed data to console for verification
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

query.awaitTermination()