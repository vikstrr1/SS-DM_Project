import os
import time
import csv
import json
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers="kafka:9092",
                         value_serializer=lambda x: json.dumps(x).encode("utf-8"))

def read_csv(directory_path):
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
                    # Clean row to remove any rows that do not match the expected format
                    if not row or not any(value for value in row.values() if value):
                        continue
                    
                    # Strip leading/trailing whitespace from each key and value safely
                    cleaned_row = {}
                    for key, value in row.items():
                        if key is None:  # Skip if key is None
                            continue

                        cleaned_key = key.strip() if key else key  # Ensure key is stripped

                        if isinstance(value, str):  # Only strip if value is a string
                            cleaned_value = value.strip()
                        else:
                            cleaned_value = value  # Keep it unchanged if it's not a string

                        # Only add to cleaned_row if cleaned_key is not None
                        if cleaned_key is not None:
                            cleaned_row[cleaned_key] = cleaned_value

                    # Ignore rows that start with a '#' or are empty
                    if cleaned_row.get('ID') and not cleaned_row['ID'].startswith('#'):
                        # Optional: Skip rows with all empty values if necessary
                        if not all(value == '' for value in cleaned_row.values()):  
                            # print("Row:", cleaned_row)  # Print each valid row to verify
                            producer.send("financial_data", value=cleaned_row)
                            # print("Sent:", cleaned_row)  # Print the sent row for verification
                            time.sleep(0.1)  # Control the rate of sending

if __name__ == "__main__":
    read_csv("/opt/flink/jobs/data/trading_data")