from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import unix_timestamp, col, lit
import psycopg2

# ------------------------------------------------------------------------------
# 1. Define PostgreSQL Connection
# ------------------------------------------------------------------------------
DB_URL = "jdbc:postgresql://ep-mute-hill-a2eprr2t-pooler.eu-central-1.aws.neon.tech"
DB_NAME = "predictive"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_QFVrW4E3SdPm"
SOURCE_TABLE = "transport_data"
PREDICTION_TABLE = "predictions"
BATCH_SIZE = 1000

# ------------------------------------------------------------------------------
# 2. Create Database if Not Exists
# ------------------------------------------------------------------------------
try:
    conn = psycopg2.connect(dbname="neondb", user=DB_USER, password=DB_PASSWORD, host="ep-mute-hill-a2eprr2t-pooler.eu-central-1.aws.neon.tech")
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
    if not cursor.fetchone():
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"✅ Database '{DB_NAME}' created.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"❌ Error creating database: {e}")

# ------------------------------------------------------------------------------
# 3. Initialize Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("TransportPrediction") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.24") \
    .getOrCreate()

# ------------------------------------------------------------------------------
# 4. Load Data in Batches
# ------------------------------------------------------------------------------
df_full = spark.read \
    .format("jdbc") \
    .option("url", f"{DB_URL}/{DB_NAME}?sslmode=require") \
    .option("dbtable", SOURCE_TABLE) \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"✅ Loaded {df_full.count()} records from PostgreSQL.")

# ------------------------------------------------------------------------------
# 5. Process & Train Model in Batches
# ------------------------------------------------------------------------------
def process_batch(batch_df):
    if batch_df.count() == 0:
        return

    # Convert timestamps
    batch_df = batch_df.withColumn("timestamp_numeric", unix_timestamp("timestamp")) \
                       .withColumn("start_time_numeric", unix_timestamp("start_time")) \
                       .withColumn("start_date_numeric", unix_timestamp("start_date"))

    # Define Features
    features = [
        "speed", "occupancy", "vehicle_latitude", "vehicle_longitude", "odometer",
        "timestamp_numeric", "start_time_numeric", "start_date_numeric",
        "vehicle_type_index", "route_id_index", "location_source_index",
        "operator_index", "direction_index", "line_index",
        "hour", "day_of_week", "is_weekend"
    ]

    # Ensure all features exist
    for col_name in features:
        if col_name not in batch_df.columns:
            batch_df = batch_df.withColumn(col_name, lit(0))

    # Encode categorical variables
    categorical_cols = ["vehicle_type", "route_id", "location_source", "operator", "direction", "line"]
    for cat_col in categorical_cols:
        if cat_col in batch_df.columns and f"{cat_col}_index" not in batch_df.columns:
            indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_index", handleInvalid="keep")
            batch_df = indexer.fit(batch_df).transform(batch_df)
            batch_df = batch_df.drop(cat_col)

    # Train Model
    assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid="skip")
    batch_df = assembler.transform(batch_df)
    train_data, test_data = batch_df.randomSplit([0.8, 0.2], seed=42)

    model = RandomForestRegressor(featuresCol="features", labelCol="delay", numTrees=100).fit(train_data)
    predictions = model.transform(test_data).withColumnRenamed("prediction", "predicted_delay")

    # Store Predictions
    predictions.select(*features, "predicted_delay").write \
        .format("jdbc") \
        .option("url", f"{DB_URL}/{DB_NAME}?sslmode=require") \
        .option("dbtable", PREDICTION_TABLE) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"✅ Processed & stored batch of {batch_df.count()} records.")

# ------------------------------------------------------------------------------
# 6. Process Data in Batches
# ------------------------------------------------------------------------------
for i in range(0, df_full.count(), BATCH_SIZE):
    batch_df = df_full.orderBy("timestamp").limit(BATCH_SIZE)
    process_batch(batch_df)

print("✅ All batches processed.")

# ------------------------------------------------------------------------------
# 7. Stop Spark
# ------------------------------------------------------------------------------
spark.stop()
print("✅ Spark session stopped.")
