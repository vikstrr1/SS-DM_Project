import os
import time
import csv
import json
from kafka import KafkaProducer, KafkaAdminClient
from datetime import datetime
# Kafka configurations
TOPIC_NAME = "financial_data"
BOOTSTRAP_SERVERS = "kafka:9092"

# Initialize Kafka producer
# Function to delete and recreate the topic if it exists, or create if it doesn't

# Function to read CSV and send to Kafka
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    linger_ms=5,  # Delay to batch messages
    #batch_size=32 * 1024
)

def read_csv(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):  # Ensure it's a CSV file
            file_path = os.path.join(directory_path, filename)
            with open(file_path) as f:
                # Skip comment and metadata lines until we find the actual header
                for line in f:
                    line = line.strip()
                    if 'ID' in line:  # Stop when a valid header is found
                        header_line = line
                        break
                else:
                    print(f"No valid header found in the CSV: {filename}")
                    continue

                # Initialize DictReader with the correct header
                reader = csv.DictReader(f, fieldnames=header_line.split(","))

                for row in reader:
                    if row.get('ID') and not row['ID'].startswith('#'):
                        try:
                            # Extract the timestamp (adjust field names to match your data)
                            trading_date = row.get('Date', '')
                            trading_time = row.get('Trading time', '')

                            # Combine and parse into a datetime object
                            timestamp_str = f"{trading_date} {trading_time}"
                            timestamp = int(datetime.strptime(timestamp_str, "%d-%m-%Y %H:%M:%S.%f").timestamp() * 1000)

                            # Add timestamp to the Kafka record
                            producer.send(
                                TOPIC_NAME,
                                value=row,
                                timestamp_ms=timestamp  # Kafka's event timestamp
                            )
                            producer.flush()
                            #time.sleep(0.1)
                        except Exception as e:
                            "something went wrong just dont want to print it!"
                            #print(f"Error processing row: {row}. Error: {e}")

if __name__ == "__main__":
    read_csv("/opt/flink/jobs/data/trading_data")
