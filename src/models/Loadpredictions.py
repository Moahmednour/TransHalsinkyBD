from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, LongType
)

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("CSV-to-Postgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.24") \
    .getOrCreate()

# Définition du schéma basé sur tes données
schema = StructType([
    StructField("total_stops", IntegerType(), True),
    StructField("vehicle_id", IntegerType(), True),
    StructField("journey_id", IntegerType(), True),
    StructField("speed", DoubleType(), True),
    StructField("occupancy", IntegerType(), True),
    StructField("vehicle_latitude", DoubleType(), True),
    StructField("vehicle_longitude", DoubleType(), True),
    StructField("timestamp_seconds", LongType(), True),
    StructField("stop", IntegerType(), True),
    StructField("odometer", DoubleType(), True),
    StructField("hour", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("is_weekend", IntegerType(), True),
    StructField("vehicle_type_index", IntegerType(), True),
    StructField("route_id_index", IntegerType(), True),
    StructField("location_source_index", IntegerType(), True),
    StructField("operator_index", IntegerType(), True),
    StructField("direction_index", IntegerType(), True),
    StructField("line_index", IntegerType(), True),
    StructField("timestamp_numeric", LongType(), True),
    StructField("start_time_numeric", LongType(), True),
    StructField("start_date_numeric", LongType(), True),
    StructField("predicted_delay", DoubleType(), True),
])

# Charger le CSV dans un DataFrame Spark
df = spark.read.csv("data/output/predictions.csv", header=True, schema=schema)

# Configuration de PostgreSQL
DB_URL = "jdbc:postgresql://ep-mute-hill-a2eprr2t-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_QFVrW4E3SdPm"
DB_TABLE = "predictions"

# Création de la table si elle n'existe pas
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {DB_TABLE} (
    total_stops INT,
    vehicle_id INT,
    journey_id INT,
    speed DOUBLE PRECISION,
    occupancy INT,
    vehicle_latitude DOUBLE PRECISION,
    vehicle_longitude DOUBLE PRECISION,
    timestamp_seconds BIGINT,
    stop INT,
    odometer DOUBLE PRECISION,
    hour INT,
    day_of_week INT,
    is_weekend INT,
    vehicle_type_index INT,
    route_id_index INT,
    location_source_index INT,
    operator_index INT,
    direction_index INT,
    line_index INT,
    timestamp_numeric BIGINT,
    start_time_numeric BIGINT,
    start_date_numeric BIGINT,
    predicted_delay DOUBLE PRECISION
);
"""

# Exécuter la requête de création de table
spark.read \
    .format("jdbc") \
    .option("url", DB_URL) \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("query", create_table_query) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print("✅ Table 'predictions' créée ou déjà existante.")

# Écrire les données dans PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", DB_URL) \
    .option("dbtable", DB_TABLE) \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Importation réussie dans PostgreSQL !")