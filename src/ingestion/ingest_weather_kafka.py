import requests
import json
from kafka import KafkaProducer
import time
from datetime import datetime

# Configuration
API_KEY = "49236c4c92084f3561bf827a2296f306"  # Replace with your actual API Key
WEATHER_URL = "http://api.openweathermap.org/data/2.5/weather"
KAFKA_BROKER = "spark-kafka-kafka-1:9092"  # Kafka broker address
TOPIC = "weather-data"

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
            print("Connected to Kafka Broker")
            return producer
        except Exception as e:
            print(f"Attempt {attempt}/{MAX_RETRIES}: Kafka connection failed: {e}")
            time.sleep(RETRY_DELAY)
    print("Failed to connect to Kafka after multiple attempts.")
    return None

producer = initialize_kafka_producer()

# Function to Fetch Weather Data
def fetch_weather(lat, lon):
    params = {
        'lat': lat,
        'lon': lon,
        'appid': API_KEY,
        'units': 'metric'  # Get temperature in Celsius
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(WEATHER_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt}/{MAX_RETRIES}: Failed to fetch weather data: {e}")
            time.sleep(RETRY_DELAY)
    print("Failed to fetch weather data after multiple attempts.")
    return None

# Function to Publish Weather Data to Kafka
def publish_weather_data(lat, lon):
    weather_data = fetch_weather(lat, lon)
    if weather_data:
        timestamp_str = datetime.utcfromtimestamp(weather_data["dt"]).strftime("%Y-%m-%d %H:%M")
        formatted_data = {
            "latitude": lat,
            "longitude": lon,
            "temperature": weather_data["main"]["temp"],
            "humidity": weather_data["main"]["humidity"],
            "wind_speed": weather_data["wind"]["speed"],
            "cloud_coverage": weather_data["clouds"]["all"],
            "pressure": weather_data["main"]["pressure"],
            "visibility": weather_data.get("visibility", "N/A"),
            "weather_main": weather_data["weather"][0]["main"],
            "weather_description": weather_data["weather"][0]["description"],
            "date": timestamp_str.split(" ")[0],
            "time": timestamp_str.split(" ")[1]
        }
        producer.send(TOPIC, value=formatted_data)
        print("Weather data sent to Kafka:", formatted_data)
    else:
        print("Failed to fetch or send weather data")

# Example Usage: Collect Weather Data Every 30 Seconds
if __name__ == "__main__":
    example_lat = 60.1524422143808
    example_lon = 24.91934076461503  
    while True:
        publish_weather_data(example_lat, example_lon)
        time.sleep(30)  # Fetch data every 30 seconds
