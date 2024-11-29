import os
import csv
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Kafka configurations
TOPIC_NAME = "financial_data"
BOOTSTRAP_SERVERS = "kafka:9092"

# Initialize Kafka producer with optimized batching
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    linger_ms=50,  # Increase linger time for better batching
    batch_size=64 * 1024  # Increase batch size to 64KB
)

def read_csv(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):  # Ensure it's a CSV file
            file_path = os.path.join(directory_path, filename)
            with open(file_path, "r") as f:
                # Use a buffered reader for efficient reading
                header_found = False
                reader = csv.reader(f)
                
                for line in reader:
                    # Detect header line dynamically
                    if not header_found and 'ID' in line:
                        header = line
                        header_found = True
                        continue
                    if not header_found:
                        continue
                    
                    # Map CSV rows to the header
                    row = dict(zip(header, line))
                    if not row.get('ID') or row['ID'].startswith('#'):
                        continue  # Skip invalid or comment rows

                    try:
                        # Combine and parse timestamp fields
                        trading_date = row.get('Date', '')
                        trading_time = row.get('Trading time', '')
                        timestamp_str = f"{trading_date} {trading_time}"
                        timestamp = int(datetime.strptime(timestamp_str, "%d-%m-%Y %H:%M:%S.%f").timestamp() * 1000)

                        # Send to Kafka
                        producer.send(
                            TOPIC_NAME,
                            value=row,
                            timestamp_ms=timestamp
                        )
                        
                    except Exception:
                        pass  # Silently handle errors

    # Ensure any remaining messages in the buffer are sent
    producer.flush()

if __name__ == "__main__":
    # Adjust the path to your CSV directory
    read_csv("/opt/flink/jobs/data/trading_data")
