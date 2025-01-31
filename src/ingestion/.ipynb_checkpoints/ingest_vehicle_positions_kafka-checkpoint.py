from paho.mqtt import client as mqtt_client
from kafka import KafkaProducer
import json
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MQTT Parameters
MQTT_BROKER = 'mqtt.hsl.fi'
MQTT_PORT = 1883
MQTT_TOPIC = "/hfp/v2/journey/ongoing/vp/#"
MQTT_CLIENT_ID = "MQTT-Python-Client"

# Kafka Parameters
KAFKA_BROKER = 'spark-kafka-kafka-1:9092'
KAFKA_TOPIC = 'mqtt-data'

# Retry configuration
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

# Initialize Kafka Producer with retry mechanism
def initialize_kafka_producer():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            logging.info("Connected to Kafka Broker")
            return producer
        except Exception as e:
            logging.error(f"Attempt {attempt}/{MAX_RETRIES}: Kafka connection failed: {e}")
            time.sleep(RETRY_DELAY)
    logging.critical("Failed to connect to Kafka after multiple attempts.")
    return None

producer = initialize_kafka_producer()

def connect_mqtt():
    """Connect to the MQTT broker and return the client instance."""
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker successfully")
        else:
            logging.error(f"Failed to connect, return code {rc}")

    client = mqtt_client.Client(MQTT_CLIENT_ID)
    client.on_connect = on_connect
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
    return client

def parse_mqtt_message(topic, message):
    """Parses MQTT messages and structures them properly for Kafka."""
    try:
        raw_data = json.loads(message)
        vp_data = raw_data.get("VP", {})

        topic_parts = topic.split("/")
        vehicle_type = topic_parts[6] if len(topic_parts) > 6 else "unknown"

        structured_message = {
            "vehicle_id": vp_data.get("veh"),
            "vehicle_type": vehicle_type,
            "operator": vp_data.get("oper"),
            "route": vp_data.get("route"),
            "direction": vp_data.get("dir"),
            "start_date": vp_data.get("oday"),
            "start_time": vp_data.get("start"),
            "destination": vp_data.get("desi", "unknown"),
            "journey_id": vp_data.get("jrn"),
            "line": vp_data.get("line"),
            "timestamp": vp_data.get("tst"),
            "timestamp_seconds": vp_data.get("tsi"),
            "delay": vp_data.get("dl"),
            "speed": max(vp_data.get("spd", 0), 0),
            "location": {
                "latitude": vp_data.get("lat"),
                "longitude": vp_data.get("long"),
                "speed": max(vp_data.get("spd", 0), 0),
                "heading": vp_data.get("hdg"),
                "accuracy": max(vp_data.get("acc", 0), 0)
            },
            "occupancy": max(vp_data.get("occu", 0), 0),
            "stop": vp_data.get("stop"),
            "odometer": vp_data.get("odo"),
            "location_source": vp_data.get("loc"),
            "mqtt_topic": topic
        }
        return structured_message
    except Exception as e:
        logging.error(f"Error parsing MQTT message: {e}")
        return {}

def subscribe(client):
    """Subscribe to the MQTT topic and publish structured messages to Kafka."""
    def on_message(client, userdata, msg):
        try:
            message = msg.payload.decode()
            topic = msg.topic
            logging.info(f"Received MQTT message from `{topic}`")

            structured_message = parse_mqtt_message(topic, message)
            if structured_message and producer:
                producer.send(KAFKA_TOPIC, value=structured_message)
                logging.info(f"Published message to Kafka topic `{KAFKA_TOPIC}`")
            else:
                logging.error("Kafka producer is not initialized or message is empty.")
        except Exception as e:
            logging.error(f"Error processing MQTT message: {e}")

    client.subscribe(MQTT_TOPIC)
    client.on_message = on_message

def run():
    """Run the MQTT client loop to receive and publish messages."""
    client = connect_mqtt()
    subscribe(client)
    logging.info("Starting MQTT loop...")
    client.loop_forever()

if __name__ == "__main__":
    run()
