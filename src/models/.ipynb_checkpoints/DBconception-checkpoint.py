from pyspark.sql import SparkSession

# Paramètres de connexion PostgreSQL Neon
DB_URL = "jdbc:postgresql://ep-mute-hill-a2eprr2t-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_QFVrW4E3SdPm"
DB_TABLE = "transport_data"  # Remplace par le nom de ta table

# Dossier de destination pour les CSV
OUTPUT_PATH = "data/raw/transport_data"

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("PostgresDataExport") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.24") \
    .getOrCreate()

# Charger les données depuis PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", DB_URL) \
    .option("dbtable", DB_TABLE) \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Sauvegarde en CSV
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(OUTPUT_PATH)

print(f"Export terminé. Les données sont stockées dans {OUTPUT_PATH}")
