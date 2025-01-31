import networkx as nx
import pandas as pd
from pyvis.network import Network

# Define the input CSV files
INPUT_PATH = "data/neo4j_import/"

# Load Nodes with a sample of 500 rows max
def load_sample(file_name, sample_size=500):
    try:
        df = pd.read_csv(f"{INPUT_PATH}{file_name}")
        return df.sample(min(sample_size, len(df)))  # Take a sample if data exists
    except FileNotFoundError:
        print(f"⚠️ Warning: {file_name} not found. Skipping.")
        return None

nodes_vehicles = load_sample("nodes_vehicles.csv", sample_size=300)
nodes_trips = load_sample("nodes_trips.csv", sample_size=300)
nodes_stops = load_sample("nodes_stops.csv", sample_size=300)

# Load Edges with a sample of 500 rows max
edges_vehicle_trip = load_sample("edges_vehicle_trip.csv", sample_size=500)
edges_trip_stop = load_sample("edges_trip_stop.csv", sample_size=500)
edges_stop_precede = load_sample("edges_stop_precede.csv", sample_size=500)

# Create a NetworkX Graph
G = nx.Graph()

# --- 1. Add Nodes ---
if nodes_vehicles is not None:
    for _, row in nodes_vehicles.iterrows():
        G.add_node(f"V_{row['vehicle_id']}", label=f"Vehicle {row['vehicle_id']}", color="blue", size=10)

if nodes_trips is not None:
    for _, row in nodes_trips.iterrows():
        G.add_node(f"T_{row['trip_id']}", label=f"Trip {row['trip_id']}", color="green", size=8)

if nodes_stops is not None:
    for _, row in nodes_stops.iterrows():
        G.add_node(f"S_{row['stop']}", label=f"Stop {row['stop']}", color="red", size=6)

# --- 2. Add Edges ---
if edges_vehicle_trip is not None:
    for _, row in edges_vehicle_trip.iterrows():
        G.add_edge(f"V_{row['vehicle_id']}", f"T_{row['trip_id']}", title="EFFECTUE")

if edges_trip_stop is not None:
    for _, row in edges_trip_stop.iterrows():
        G.add_edge(f"T_{row['trip_id']}", f"S_{row['stop']}", title="DESSERT")

if edges_stop_precede is not None:
    for _, row in edges_stop_precede.iterrows():
        G.add_edge(f"S_{row['stop1']}", f"S_{row['stop2']}", title="PRECEDE")

# --- 3. Visualize with PyVis ---
net = Network(height="800px", width="100%", notebook=True, bgcolor="#222222", font_color="white")
net.from_nx(G)

# Save as an interactive HTML file
output_file = "transport_graph_sample.html"
net.show(output_file)

print(f"✅ Interactive graph saved as {output_file}. Open it in a browser to explore!")
