import os
import time
import csv
import json
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

# Kafka configurations
TOPIC_NAME = "financial_data"
BOOTSTRAP_SERVERS = "kafka:9092"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# Function to purge the topic
def purge_kafka_topic(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        # Delete the topic
        admin_client.delete_topics([topic_name])
        print(f"Topic '{topic_name}' deleted.")
        time.sleep(2)  # Wait for the deletion to propagate

        # Recreate the topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' recreated.")
    except Exception as e:
        print(f"Error purging topic: {e}")
    finally:
        admin_client.close()

def read_csv(directory_path):
    # Ensure the Kafka topic is empty
    purge_kafka_topic(TOPIC_NAME)

    # Loop through each file in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv"):  # Ensure it's a CSV file
            file_path = os.path.join(directory_path, filename)  # Correctly construct the file path
            with open(file_path) as f:
                # Skip comment and metadata lines until we find the actual header
                for line in f:
                    line = line.strip()
                    # Stop reading when we find a valid header (line that contains 'ID' for example)
                    if 'ID' in line:
                        header_line = line
                        break
                else:
                    print(f"No valid header found in the CSV: {filename}")
                    continue

                # Initialize DictReader with the correct header
                reader = csv.DictReader(f, fieldnames=header_line.split(","))

                for row in reader:
                    # Ignore rows that start with a '#' or are empty
                    if row.get('ID') and not row['ID'].startswith('#'):
                        # Send the raw row directly to Kafka
                        producer.send(TOPIC_NAME, value=row)
                        # time.sleep(0.1)  # Control the rate of sending

if __name__ == "__main__":
    read_csv("/opt/flink/jobs/data/trading_data")
