from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, size, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, LongType, ArrayType, StructField

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("Kafka-Streaming-Transport") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.24") \
    .getOrCreate()

# Configuration de Kafka
KAFKA_BROKER = "spark-kafka-kafka-1:9092"

# Configuration PostgreSQL Neon
DB_URL = "jdbc:postgresql://ep-mute-hill-a2eprr2t-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_QFVrW4E3SdPm"
DB_TABLE = "transport_data"

# Définition des schémas
trip_schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("stop_time_updates", ArrayType(StructType([
        StructField("stop_id", StringType(), True),
        StructField("arrival_time", LongType(), True),
        StructField("departure_time", LongType(), True)
    ])))
])

vehicle_schema = StructType([
    StructField("vehicle_id", IntegerType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("operator", IntegerType(), True),
    StructField("route", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("journey_id", IntegerType(), True),
    StructField("line", IntegerType(), True),
    StructField("speed", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("timestamp_seconds", LongType(), True),
    StructField("delay", IntegerType(), True),
    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("heading", IntegerType(), True),
        StructField("accuracy", DoubleType(), True)
    ])),
    StructField("occupancy", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("odometer", DoubleType(), True),
    StructField("location_source", StringType(), True)
])

# Lecture des flux Kafka
trip_updates_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "trip-updates") \
    .load() \
    .selectExpr("CAST(value AS STRING) as kafka_message") \
    .select(from_json(col("kafka_message"), trip_schema).alias("trip"))

vehicle_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "mqtt-data") \
    .load() \
    .selectExpr("CAST(value AS STRING) as kafka_message") \
    .select(from_json(col("kafka_message"), vehicle_schema).alias("vehicle"))

# Jointure des données
joined_trip_vehicle_df = trip_updates_df.alias("trip") \
    .join(vehicle_df.alias("vehicle"),
          (col("trip.route_id") == col("vehicle.route")) &
          (col("trip.start_time") == col("vehicle.start_time")) &
          (col("trip.start_date") == col("vehicle.start_date")),
          "inner") \
    .select(
        col("trip.trip_id"),
        col("trip.route_id"),
        col("trip.start_time"),
        col("trip.start_date"),
        col("trip.status"),
        size(col("trip.stop_time_updates")).alias("total_stops"),
        col("vehicle.vehicle_id"),
        col("vehicle.vehicle_type"),
        col("vehicle.operator"),
        col("vehicle.direction"),
        col("vehicle.journey_id"),
        col("vehicle.line"),
        col("vehicle.speed"),
        col("vehicle.occupancy"),
        col("vehicle.delay"),
        col("vehicle.location.latitude").alias("vehicle_latitude"),
        col("vehicle.location.longitude").alias("vehicle_longitude"),
        col("vehicle.timestamp"),
        col("vehicle.timestamp_seconds"),
        col("vehicle.stop"),
        col("vehicle.odometer"),
        col("vehicle.location_source"),
        current_timestamp().alias("processed_time")
    )

# Fonction pour écrire les données dans PostgreSQL
def write_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", DB_TABLE) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Écriture des données jointes dans PostgreSQL
joined_trip_vehicle_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Affichage des données dans la console pour vérification
joined_trip_vehicle_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

spark.streams.awaitAnyTermination()
