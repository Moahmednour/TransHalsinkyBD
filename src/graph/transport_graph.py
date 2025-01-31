from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from neo4j import GraphDatabase
import pandas as pd
import numpy as np

# Initialize Spark Session
spark = SparkSession.builder.appName("TransportGraphNeo4j").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # Reduce logs

# Load Transport Data
df = spark.read.csv("data/raw/transport_data/*.csv", header=True, inferSchema=True)

# Define Output Folder
OUTPUT_PATH = "data/neo4j_import/"

# Neo4j Aura Connection Details
NEO4J_URI = "neo4j+s://a2ff59d8.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Lu2vU4LvoIcUbvi4CUhsxhSudJid5PN3gB81dgmZOG0"

# Neo4j Connection Handler
class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_batch_query(self, query, batch_data):
        if batch_data:  # Run only if data is not empty
            with self.driver.session() as session:
                for batch in np.array_split(batch_data, max(1, len(batch_data) // 40000)):  # 40k per batch
                    session.run(query, parameters={"batch": batch.tolist()})

neo4j_handler = Neo4jHandler(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Function to save CSV only if DataFrame is not empty
def safe_save(df, filename):
    if not df.isEmpty():
        df.toPandas().to_csv(f"{OUTPUT_PATH}{filename}", index=False)

# --- 1. Extract & Save Nodes ---
vehicles_df = df.select("vehicle_id", "vehicle_type", "operator", "line", "speed", "occupancy").distinct().dropna(subset=["vehicle_id"])
safe_save(vehicles_df, "nodes_vehicles.csv")

trips_df = df.select("trip_id", "route_id", "start_time", "start_date", "direction", "total_stops").distinct().dropna(subset=["trip_id"])
safe_save(trips_df, "nodes_trips.csv")

stops_df = df.select("stop", "vehicle_latitude", "vehicle_longitude").distinct().dropna(subset=["stop"])
safe_save(stops_df, "nodes_stops.csv")

# --- 2. Extract & Save Edges ---
vehicle_trip_df = df.select("vehicle_id", "trip_id").distinct().dropna(subset=["vehicle_id", "trip_id"])
safe_save(vehicle_trip_df, "edges_vehicle_trip.csv")

trip_stop_df = df.select("trip_id", "stop").distinct().dropna(subset=["trip_id", "stop"])
safe_save(trip_stop_df, "edges_trip_stop.csv")

vehicle_position_df = df.select("vehicle_id", "vehicle_latitude", "vehicle_longitude").distinct().dropna(subset=["vehicle_id"])
safe_save(vehicle_position_df, "edges_vehicle_position.csv")

# Handle Stop Sequences (PRECEDE Relationship)
stop_pairs = df.select("trip_id", "stop").distinct().dropna(subset=["trip_id", "stop"]).orderBy("trip_id", "stop").collect()

precede_edges = [(stop_pairs[i]["stop"], stop_pairs[i + 1]["stop"]) 
                 for i in range(len(stop_pairs) - 1) 
                 if stop_pairs[i]["trip_id"] == stop_pairs[i + 1]["trip_id"]]

if precede_edges:
    precede_df = spark.createDataFrame(precede_edges, ["stop1", "stop2"])
else:
    schema = StructType([StructField("stop1", StringType(), True), StructField("stop2", StringType(), True)])
    precede_df = spark.createDataFrame([], schema)

safe_save(precede_df, "edges_stop_precede.csv")

print(f"✅ Nodes and edges saved in {OUTPUT_PATH}!")

# --- 3. Upload to Neo4j in Batches ---
def batch_insert(query, data_pd):
    if not data_pd.empty:
        neo4j_handler.execute_batch_query(query, data_pd.to_dict(orient="records"))

# Nodes Upload
batch_insert("""
UNWIND $batch AS row MERGE (v:Vehicle {id: row.vehicle_id}) 
SET v.type = row.vehicle_type, v.operator = row.operator, v.line = row.line, v.speed = row.speed, v.occupancy = row.occupancy
""", vehicles_df.toPandas())

batch_insert("""
UNWIND $batch AS row MERGE (t:Trip {id: row.trip_id}) 
SET t.route_id = row.route_id, t.start_time = row.start_time, t.start_date = row.start_date, t.direction = row.direction, t.total_stops = row.total_stops
""", trips_df.toPandas())

batch_insert("""
UNWIND $batch AS row MERGE (s:Stop {id: row.stop}) 
SET s.latitude = row.vehicle_latitude, s.longitude = row.vehicle_longitude
""", stops_df.toPandas())

# Edges Upload
batch_insert("""
UNWIND $batch AS row MATCH (v:Vehicle {id: row.vehicle_id}), (t:Trip {id: row.trip_id}) 
MERGE (v)-[:EFFECTUE]->(t)
""", vehicle_trip_df.toPandas())

batch_insert("""
UNWIND $batch AS row MATCH (t:Trip {id: row.trip_id}), (s:Stop {id: row.stop}) 
MERGE (t)-[:DESSERT]->(s)
""", trip_stop_df.toPandas())

batch_insert("""
UNWIND $batch AS row MATCH (v:Vehicle {id: row.vehicle_id}) 
MERGE (p:Position {latitude: row.vehicle_latitude, longitude: row.vehicle_longitude}) 
MERGE (v)-[:SE_DEPLACE_VERS]->(p)
""", vehicle_position_df.toPandas())

batch_insert("""
UNWIND $batch AS row MATCH (s1:Stop {id: row.stop1}), (s2:Stop {id: row.stop2}) 
MERGE (s1)-[:PRECEDE]->(s2)
""", precede_df.toPandas())

# Close Neo4j Connection
neo4j_handler.close()

print("✅ Transport Graph Imported into Neo4j Aura Successfully with Batches! 🚀")
