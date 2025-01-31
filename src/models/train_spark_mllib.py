from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, when, unix_timestamp, isnan, abs
)
from pyspark.ml.feature import VectorAssembler, StringIndexer, Imputer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import DoubleType
import pandas as pd
import os

# ------------------------------------------------------------------------------
# 1. Initialize Spark Session
# ------------------------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("Transport Delay Prediction")
        .config("spark.jars.packages", "org.apache.spark:spark-sql_2.12:3.3.0")
        .getOrCreate()
)

print("✅ Spark session initialized.")

# ------------------------------------------------------------------------------
# 2. Load Dataset from CSV
# ------------------------------------------------------------------------------
df = spark.read.csv("data/raw/transport_data/*.csv", header=True, inferSchema=True)
print("✅ Data loaded from CSV.")

# ------------------------------------------------------------------------------
# 3. Convert Timestamps to Useful Features
# ------------------------------------------------------------------------------
if "timestamp" in df.columns:
    df = df.withColumn("hour", hour(col("timestamp")))
    df = df.withColumn("day_of_week", dayofweek(col("timestamp")))
    df = df.withColumn("is_weekend", when(col("day_of_week") >= 6, 1).otherwise(0))
    print("✅ Time-based features added.")

# ------------------------------------------------------------------------------
# 4. Handle Missing Values
# ------------------------------------------------------------------------------
df = df.filter((col("delay").isNotNull()) & (~isnan(col("delay"))))
df = df.na.fill(0, subset=["speed", "occupancy", "vehicle_latitude", "vehicle_longitude", 
                           "hour", "day_of_week", "is_weekend", "odometer"])
df = df.withColumn("delay", col("delay").cast(DoubleType()))
print("✅ Missing values handled.")

# ------------------------------------------------------------------------------
# 5. Encode Categorical Variables
# ------------------------------------------------------------------------------
categorical_columns = ["vehicle_type", "route_id", "location_source", "operator", "direction", "line"]
indexers = []
for cat_col in categorical_columns:
    if cat_col in df.columns:
        indexer = StringIndexer(inputCol=cat_col, outputCol=cat_col + "_index", handleInvalid="keep")
        df = indexer.fit(df).transform(df)
        df = df.drop(cat_col)
print("✅ Categorical variables encoded.")

# ------------------------------------------------------------------------------
# 6. Drop Unnecessary Columns
# ------------------------------------------------------------------------------
df = df.drop(*[c for c in ["trip_id", "status", "processed_time"] if c in df.columns])
print("✅ Dropped unnecessary columns.")

# ------------------------------------------------------------------------------
# 7. Convert DateTime Columns to Numeric
# ------------------------------------------------------------------------------
if "timestamp" in df.columns:
    df = df.withColumn("timestamp_numeric", unix_timestamp(col("timestamp"))).drop("timestamp")
if "start_time" in df.columns:
    df = df.withColumn("start_time_numeric", unix_timestamp(col("start_time"))).drop("start_time")
if "start_date" in df.columns:
    df = df.withColumn("start_date_numeric", unix_timestamp(col("start_date"))).drop("start_date")

print("✅ Converted timestamp columns to numeric.")

# ------------------------------------------------------------------------------
# 8. Assemble Features into a Feature Vector
# ------------------------------------------------------------------------------
feature_columns = [c for c in df.columns if c != "delay"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
df = assembler.transform(df)
print("✅ Assembled feature vector.")

# ------------------------------------------------------------------------------
# 9. Split Data into Training and Testing Sets
# ------------------------------------------------------------------------------
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
print("✅ Split dataset into training and test sets.")

# ------------------------------------------------------------------------------
# 10. Train Random Forest Model
# ------------------------------------------------------------------------------
rf = RandomForestRegressor(featuresCol="features", labelCol="delay", numTrees=100, maxBins=256)
print("ℹ️ Training Random Forest model...")
model = rf.fit(train_data)
print("✅ Random Forest model trained successfully.")

# ------------------------------------------------------------------------------
# 11. Extract Feature Importance
# ------------------------------------------------------------------------------
feature_importance = model.featureImportances
feature_names = feature_columns  # Match extracted features with their names
important_features = sorted(zip(feature_names, feature_importance), key=lambda x: x[1], reverse=True)

print("📊 Feature Importance:")
for feature, importance in important_features:
    print(f"🔹 {feature}: {importance:.4f}")

# ------------------------------------------------------------------------------
# 12. Evaluate Model Performance
# ------------------------------------------------------------------------------
predictions = model.transform(test_data)

rmse = RegressionEvaluator(labelCol="delay", predictionCol="prediction", metricName="rmse").evaluate(predictions)
mae = RegressionEvaluator(labelCol="delay", predictionCol="prediction", metricName="mae").evaluate(predictions)
r2_score = RegressionEvaluator(labelCol="delay", predictionCol="prediction", metricName="r2").evaluate(predictions)

predictions = predictions.withColumn("ape", abs(col("delay") - col("prediction")) / when(col("delay") != 0, col("delay")).otherwise(1))
mape = predictions.selectExpr("avg(ape) as mape").collect()[0]["mape"] * 100

print(f"📊 RMSE: {rmse:.4f}")
print(f"📊 MAE: {mae:.4f}")
print(f"📊 R² Score: {r2_score:.4f}")
print(f"📊 MAPE: {mape:.2f}%")

# ------------------------------------------------------------------------------
# 13. Save Predictions with Feature Names
# ------------------------------------------------------------------------------
OUTPUT_CSV_PATH = "data/output/predictions_with_features.csv"
os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)

# Convert to Pandas for saving
predictions_df = predictions.select(feature_names + ["prediction"]).toPandas()
predictions_df.to_csv(OUTPUT_CSV_PATH, index=False)

print(f"✅ Predictions saved to {OUTPUT_CSV_PATH}")

# ------------------------------------------------------------------------------
# 14. Save Model
# ------------------------------------------------------------------------------
model.write().overwrite().save("output/spark_mllib_delay_model")
print("✅ Model saved successfully.")

# ------------------------------------------------------------------------------
# 15. Stop Spark Session
# ------------------------------------------------------------------------------
spark.stop()
print("✅ Spark session stopped.")
