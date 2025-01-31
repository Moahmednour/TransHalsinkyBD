import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Configuration
KAFKA_BROKER = "spark-kafka-kafka-1:9092"
TRIP_UPDATES_TOPIC = "trip-updates"

# GTFS-RT API Endpoint (HSL)
GTFS_RT_URL = "https://realtime.hsl.fi/realtime/trip-updates/v2/hsl"

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

# Initialize Kafka Producer with retry mechanism
def initialize_kafka_producer():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
            )
            logging.info("Connected to Kafka Broker")
            return producer
        except Exception as e:
            logging.error(f"Attempt {attempt}/{MAX_RETRIES}: Kafka connection failed: {e}")
            time.sleep(RETRY_DELAY)
    logging.critical("Failed to connect to Kafka after multiple attempts.")
    return None

producer = initialize_kafka_producer()

def fetch_gtfs_rt_feed():
    """Fetch GTFS-RT trip updates feed from the HSL API with retry mechanism."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(GTFS_RT_URL, timeout=10)
            response.raise_for_status()
            return response.content  # GTFS-RT data is in protobuf format
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {attempt}/{MAX_RETRIES}: Failed to fetch GTFS-RT trip updates: {e}")
            time.sleep(RETRY_DELAY)
    logging.critical("Failed to fetch GTFS data after multiple attempts.")
    return None

def process_trip_updates(data):
    """Parse GTFS-RT data and publish structured trip updates to Kafka."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip_update = entity.trip_update

            # Convert GTFS start_date from YYYYMMDD to YYYY-MM-DD
            start_date_str = trip_update.trip.start_date
            formatted_start_date = datetime.strptime(start_date_str, "%Y%m%d").strftime("%Y-%m-%d") if start_date_str else None

            # Convert start_time from HH:MM:SS to HH:MM to match vehicle data
            start_time_str = trip_update.trip.start_time
            formatted_start_time = datetime.strptime(start_time_str, "%H:%M:%S").strftime("%H:%M") if start_time_str else None

            trip_data = {
                "trip_id": trip_update.trip.trip_id,
                "route_id": trip_update.trip.route_id,
                "start_time": formatted_start_time,  # Converted to HH:MM format
                "start_date": formatted_start_date,  # Converted to YYYY-MM-DD format
                "stop_time_updates": [
                    {
                        "stop_id": update.stop_id,
                        "arrival_time": update.arrival.time,
                        "departure_time": update.departure.time
                    }
                    for update in trip_update.stop_time_update
                ]
            }

            # Ensure data consistency before publishing
            if trip_data["start_date"] and trip_data["start_time"]:
                producer.send(TRIP_UPDATES_TOPIC, value=trip_data)
                logging.info(f"Published trip update: {trip_data}")
            else:
                logging.warning(f"Skipping trip update due to missing start_date/start_time: {trip_data}")

def run():
    """Continuously fetch and process GTFS-RT trip updates."""
    while True:
        trip_updates_data = fetch_gtfs_rt_feed()
        if trip_updates_data:
            process_trip_updates(trip_updates_data)
        time.sleep(15)  # Adjust frequency based on API update rate

if __name__ == "__main__":
    run()