from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Store-Processed-Data-PostgreSQL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.3.1") \
    .getOrCreate()

# Read processed data from Spark Streaming processing script
# Ensure the main script saves `final_df` to a temporary table
processed_df = spark.sql("SELECT * FROM processed_transport_data")

# Store in NeonDB PostgreSQL
processed_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://ep-mute-hill-a2eprr2t-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require") \
    .option("dbtable", "processed_transport_data") \
    .option("user", "neondb_owner") \
    .option("password", "npg_QFVrW4E3SdPm") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("Data successfully stored in NeonDB PostgreSQL.")