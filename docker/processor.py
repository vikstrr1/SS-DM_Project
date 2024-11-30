import json
import threading
import os
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# InfluxDB connection details
influxdb_url = "http://influxdb:8086"
bucket = "ticker_bucket"  # Ensure this matches your docker-compose setup
org = "ticker_org"  # Ensure this matches your docker-compose setup
token =os.getenv('INFLUXDB_TOKEN')   # Use valid token 
#os.getenv('INFLUXDB_TOKEN')
# Kafka topics
ticker_topic = "ticker_data"
ema_topic = "ema_data"
kafka_bootstrap_servers = ["kafka:9092"]

# Connect to InfluxDB
client = InfluxDBClient(url=influxdb_url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Function to write data to InfluxDB
def write_to_influxdb(bucket, measurement, data):
    try:
        point = Point(measurement)
        for key, value in data.items():
            # Use fields for numerical values and tags for strings
            if isinstance(value, (int, float)):
                point = point.field(key, value)
            else:
                point = point.tag(key, value)
        write_api.write(bucket=bucket, record=point)
        #print(f"Data written to InfluxDB: {data}")
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")

# Function to process ticker data
def consume_ticker():
    ticker_consumer = KafkaConsumer(
        ticker_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="ticker_consumer"
    )
    for msg in ticker_consumer:
        data = msg.value
        try:
            # Convert timestamp to ISO 8601
            iso_timestamp = datetime.strptime(data["f0"], "%d-%m-%Y %H:%M:%S.%f").isoformat() + "Z"
            influx_data = {
                "timestamp": iso_timestamp,
                "symbol": data["f1"],
                "sec_type": data["f2"],
                "arrival_time": data["f3"],
                "price": data["f4"]
            }
            write_to_influxdb(bucket, "ticker_data", influx_data)
        except Exception as e:
            print(f"Error processing ticker data: {e}")

# Function to process EMA data
def consume_ema():
    ema_consumer = KafkaConsumer(
        ema_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="ema_consumer"
    )
    for msg in ema_consumer:
        data = msg.value
        #print(f"Processing EMA data: {data}")
        try:
            # Convert advice_timestamp to ISO 8601
            iso_advice_timestamp = datetime.fromtimestamp((data["f2"]+data["f4"]) / 1000).isoformat() + "Z"
            iso_context_start = datetime.fromtimestamp(data["f1"] / 1000).isoformat() + "Z"
            iso_context_end = datetime.fromtimestamp(data["f2"] / 1000).isoformat() + "Z"
            influx_data = {
                "symbol": data["f0"],
                "context_start": iso_context_start ,
                "context_end": iso_context_end,
                "ema_38": data["f3"],
                "ema_100": data["f4"],
                "advice": data["f5"],
                "advice_timestamp": iso_advice_timestamp,
                "latency": data["f6"]
            }
            write_to_influxdb(bucket, "ema_data", influx_data)
        except Exception as e:
            print(f"Error processing EMA data: {e}")

# Start Kafka consumers in separate threads
if __name__ == "__main__":
    print("Starting Kafka to InfluxDB Processor...")

    ticker_thread = threading.Thread(target=consume_ticker, daemon=True)
    ema_thread = threading.Thread(target=consume_ema, daemon=True)

    ticker_thread.start()
    ema_thread.start()

    ticker_thread.join()
    ema_thread.join()
