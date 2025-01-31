from pyspark.sql import SparkSession
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import unix_timestamp, col, lit
import os

# ------------------------------------------------------------------------------
# 1. Initialize Spark Session
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("TransportDelayPrediction").getOrCreate()

# ------------------------------------------------------------------------------
# 2. Load the Trained Model
# ------------------------------------------------------------------------------
model_path = "output/spark_mllib_delay_model"
model = RandomForestRegressionModel.load(model_path)

# Get the number of expected features
expected_num_features = model.numFeatures

# ------------------------------------------------------------------------------
# 3. Load Local Test Data for Prediction
# ------------------------------------------------------------------------------
DATA_PATH = "data/raw/transport_data/*.csv"
df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)

# ------------------------------------------------------------------------------
# 4. Convert Timestamps to Numeric Format
# ------------------------------------------------------------------------------
df = df.withColumn("timestamp_numeric", unix_timestamp("timestamp"))
df = df.withColumn("start_time_numeric", unix_timestamp("start_time"))
df = df.withColumn("start_date_numeric", unix_timestamp("start_date"))

# ------------------------------------------------------------------------------
# 5. Ensure Features Match Training Data (EXACTLY as in training)
# ------------------------------------------------------------------------------
expected_features = [
    "speed", "occupancy", "vehicle_latitude", "vehicle_longitude", "odometer",
    "timestamp_numeric", "start_time_numeric", "start_date_numeric",
    "vehicle_type_index", "route_id_index", "location_source_index",
    "operator_index", "direction_index", "line_index",
    "hour", "day_of_week", "is_weekend"
]

# Add missing columns with default values (0)
missing_features = [col for col in expected_features if col not in df.columns]
for col_name in missing_features:
    df = df.withColumn(col_name, lit(0))

# Ensure correct column order before VectorAssembler
df = df.select(*expected_features)

# ------------------------------------------------------------------------------
# 6. Create Feature Vector
# ------------------------------------------------------------------------------
assembler = VectorAssembler(inputCols=expected_features, outputCol="features", handleInvalid="skip")
df = assembler.transform(df)

# Verify the feature vector size before prediction
first_row = df.select("features").head()
actual_num_features = len(first_row["features"]) if first_row else 0
if actual_num_features != expected_num_features:
    raise ValueError(f"Error: Model expects {expected_num_features} features, but {actual_num_features} were generated.")

# ------------------------------------------------------------------------------
# 7. Make Predictions with the Model
# ------------------------------------------------------------------------------
predictions = model.transform(df)

# Rename "prediction" to "predicted_delay"
predictions = predictions.withColumnRenamed("prediction", "predicted_delay")

# Ensure correct column selection in output
predictions = predictions.select(*expected_features, "predicted_delay")

# ------------------------------------------------------------------------------
# 8. Save Predictions with Features to CSV
# ------------------------------------------------------------------------------
OUTPUT_CSV_PATH = "data/output/predictions_with_features.csv"
os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)

# Convert to Pandas and save
predictions.toPandas().to_csv(OUTPUT_CSV_PATH, index=False)

print(f"✅ Predictions saved to {OUTPUT_CSV_PATH}")

# ------------------------------------------------------------------------------
# 9. Stop Spark Session
# ------------------------------------------------------------------------------
spark.stop()
print("✅ Spark session stopped.")