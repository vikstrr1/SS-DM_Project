import os
from pyflink.common import Types, Row, Duration
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import SinkFunction, RuntimeContext
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.common import RestartStrategies
import logging
from datetime import datetime
from typing import Iterable, Optional, Dict, Tuple

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


import json
import requests

# Configure logging
logging.basicConfig(level=logging.WARNING)
TIMESTAMP_FORMAT = "%d-%m-%Y %H:%M:%S.%f"
smtp_user = os.getenv('SMTP_USER')
smtp_pass = os.getenv('SMTP_PASS')
smtp_recv = os.getenv('SMTP_RECV')
ticker_symbols = json.loads(os.getenv('TICKER_SYMBOLS', '[]'))

def send_email(subject, body):
    try:
        # Setup the MIME
        message = MIMEMultipart()
        message['From'] = smtp_user
        message['To'] = smtp_recv
        message['Subject'] = subject

        # Add body to email
        message.attach(MIMEText(body, 'plain'))

        # Connect to the SMTP server and send email
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Use TLS encryption
        server.login(smtp_user, smtp_pass)
        text = message.as_string()
        server.sendmail(smtp_user, smtp_recv, text)
        server.quit()

        #logging.info(f"Email sent to {smtp_recv} with subject: {subject}")

    except Exception as e:
        logging.error(f"Failed to send email: {e}")


def parse_event(record: dict) -> Optional[Row]:
    # Convert Kafka message to row format expected by Flink
    try:
        symbol = record.get("ID")  # Adjusted to use 'ID' as the symbol identifier
        sec_type = record.get("SecType")
        #Not really needed
        #timestamp_str = f"{record.get('Date')} {record.get('Time')}"
        last_price = record.get("Last")
        trading_time = record.get("Trading time")
        trading_date = record.get("Trading date") or record.get('Date')
        trading_timestamp_str = f"{trading_date} {trading_time}"
        if not all([trading_timestamp_str,symbol, sec_type, last_price]):
            return None
        last_price = float(last_price) if last_price else None
    except Exception as e:
        logging.error(f"Error parsing record: {e} for record: {record}")
        return None

    return Row(trading_timestamp_str, symbol, sec_type, datetime.now().isoformat(), last_price)




class SimpleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        if value is None:
            logging.warning("Received None as value, discarding event.")
            return -1

        iso_timestamp = value[0]
        if not iso_timestamp:
            logging.warning(f"iso_timestamp is None or empty: {iso_timestamp}, discarding event.")
            return -1

        try:
            dt = datetime.strptime(iso_timestamp, TIMESTAMP_FORMAT)
            logging.debug(f"Parsed timestamp: {dt}")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logging.error(f"Timestamp parsing failed for {iso_timestamp}: {e}")
            return -1

class EMAProcessFunction(ProcessWindowFunction[Row, Tuple[str, str, str, float, float, str, int, float], str, TimeWindow]):
    def __init__(self):
        self.ema_state_38 = None
        self.ema_state_100 = None

    def open(self, runtime_context):
        # Define ValueState for each EMA to persist across windows
        self.ema_state_38 = runtime_context.get_state(ValueStateDescriptor("ema_38", Types.FLOAT()))
        self.ema_state_100 = runtime_context.get_state(ValueStateDescriptor("ema_100", Types.FLOAT()))

    def process(self, key: str, context: ProcessWindowFunction.Context[TimeWindow], elements: Iterable[Row]) -> Iterable[Tuple[str, str, str, float, float, str, int, float]]:
        prices = [element[4] for element in elements if element[4] is not None]
        arrival_times = [element[3] for element in elements if element[3] is not None]

        if not prices:
            logging.warning(f"No valid prices for symbol {key} in this window, skipping EMA calculation.")
            return []

        # Get the last price and arrival time
        price = prices[-1]
        arrival_time = arrival_times[-1]

        # Calculate EMA smoothing factors
        alpha_38 = 2 / (38 + 1)
        alpha_100 = 2 / (100 + 1)

        # Retrieve previous EMA values from state
        previous_ema_38 = self.ema_state_38.value() or 0.0
        previous_ema_100 = self.ema_state_100.value() or 0.0

        # Calculate new EMA values
        ema_38 = self.calculate_ema(price, previous_ema_38, alpha_38)
        ema_100 = self.calculate_ema(price, previous_ema_100, alpha_100)

        # Check for breakout patterns
        advice_type, advice_timestamp = self.check_advice(previous_ema_38, previous_ema_100, ema_38, ema_100,key, context)

        # Calculate latency if advice is generated
        latency = None
        if advice_timestamp:
            last_arrival_time = datetime.strptime(arrival_time, "%Y-%m-%dT%H:%M:%S.%f").timestamp()
            latency = datetime.now().timestamp() - last_arrival_time

        # Update EMA state with the final values for this window
        self.ema_state_38.update(ema_38)
        self.ema_state_100.update(ema_100)

        # Calculate window start and end times
        start_time = datetime.fromtimestamp(context.window().start / 1000).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(context.window().end / 1000).strftime('%Y-%m-%d %H:%M:%S')

        # Return results
        return [(key, start_time, end_time, ema_38, ema_100, advice_type or "", advice_timestamp or -1, latency or -1)]


    def calculate_ema(self, current_price: float, previous_ema: float, alpha: float) -> float:
        return (alpha * current_price) + ((1 - alpha) * previous_ema)

    def check_advice(self, previous_ema_38: float, previous_ema_100: float, ema_38: float, ema_100: float, symbol: str, context: ProcessWindowFunction.Context[TimeWindow]) -> Tuple[str, Optional[int]]:
        advice_type = None
        advice_timestamp = None
        if previous_ema_38 is not None and previous_ema_100 is not None:
            # Bullish breakout: EMA38 crosses above EMA100
            if ema_38 > ema_100 and previous_ema_38 <= previous_ema_100:
                advice_type = "Buy"
                advice_timestamp = context.current_processing_time()
                logging.info(f"Bullish breakout detected - Buy advice generated.")
                #send_email(f"EMA Buy Signal for {symbol}", f"Symbol: {symbol}\nAdvice: Buy\nTimestamp: {advice_timestamp}")
            # Bearish breakout: EMA38 crosses below EMA100
            elif ema_100 > ema_38 and previous_ema_100 <= previous_ema_38:
                advice_type = "Sell"
                advice_timestamp = context.current_processing_time()
                logging.info(f"Bearish breakout detected - Sell advice generated.")
                #send_email(f"EMA Sell Signal for {symbol}", f"Symbol: {symbol}\nAdvice: Sell\nTimestamp: {advice_timestamp}")
        
        return advice_type, advice_timestamp
    
    

def insert_static_data(index_name, data):
    # Elasticsearch URL (adjust the host/port as needed)
    es_url = f"https://192.168.1.100:9200/{index_name}/_bulk"

    # Prepare the bulk request data
    bulk_data = ""
    for entry in data:
        document = {
            "timestamp": entry[0],
            "symbol": entry[1],
            "type": entry[2]
        }

        # Prepare the action and the document in the bulk request format
        action = json.dumps({
            "index": {
                "_index": index_name
            }
        })
        bulk_data += action + "\n" + json.dumps(document) + "\n"

    # Make a POST request to insert documents in bulk
    headers = {"Content-Type": "application/x-ndjson"}
    try:
        response = requests.post(es_url, data=bulk_data, auth=("elastic", "password123"), headers=headers, verify=False)
        
        if response.status_code == 200:
            print(f"Successfully inserted {len(data)} documents.")
        else:
            print(f"Failed to insert documents. Status code: {response.status_code}, Response: {response.text}")
    
    except requests.exceptions.RequestException as e:
        print(f"Error during request: {e}")

def send_ema_to_elasticsearch(value):
    es_url = "https://192.168.1.100:9200/ema_data/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}
    auth = ("elastic", "password123")  # Adjust with your authentication

    # Prepare the document for Elasticsearch
    document = {
        "timestamp": value[0],
        "symbol": value[1],
        "type": value[2],
        "ema_38": value[3],
        "ema_100": value[4],
        "advice": value[5],
        "advice_timestamp": value[6]
    }

    # Prepare the bulk request data
    action = json.dumps({
        "index": {
            "_index": "ema_data"
        }
    })
    bulk_data = f"{action}\n{json.dumps(document)}\n"

    # Send the bulk data to Elasticsearch via the POST request
    try:
        response = requests.post(es_url, data=bulk_data, headers=headers, auth=auth, verify=False)
        if response.status_code == 200:
            logging.info(f"Successfully inserted document: {document}")
        else:
            logging.error(f"Failed to insert document. Status code: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during request: {e}")

def send_ticker_to_elasticsearch(value, tickername):
    es_url = "https://192.168.1.100:9200/"+tickername+"/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}
    auth = ("elastic", "password123")  # Adjust with your authentication

    # Prepare the document for Elasticsearch
    document = {
        "timestamp": value[0],
        "last_price": value[1],
    }

    # Prepare the bulk request data
    action = json.dumps({
        "index": {
            "_index": tickername,
            "_id": f"{document['timestamp']}_{document['last_price']}"
        }
    })
    bulk_data = f"{action}\n{json.dumps(document)}\n"

    # Send the bulk data to Elasticsearch via the POST request
    try:
        response = requests.post(es_url, data=bulk_data, headers=headers, auth=auth, verify=False)
        if response.status_code == 200:
            logging.info(f"Successfully inserted document: {document}")
        else:
            logging.error(f"Failed to insert document. Status code: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during request: {e}")

def create_index_if_not_exists(index_name):
    es_url = f"https://192.168.1.100:9200/{index_name}"
    headers = {"Content-Type": "application/json"}
    auth = ("elastic", "password123")
    try:
        response = requests.head(es_url, headers=headers, auth=auth, verify=False)
        if response.status_code == 404:
            # Index does not exist, create it
            response = requests.put(es_url, headers=headers, auth=auth, verify=False)
            if response.status_code == 200:
                logging.info(f"Index '{index_name}' created successfully.")
            else:
                logging.error(f"Failed to create index '{index_name}'. Status code: {response.status_code}, Response: {response.text}")
        elif response.status_code == 200:
            logging.info(f"Index '{index_name}' already exists.")
        else:
            logging.error(f"Error checking index '{index_name}'. Status code: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error during request: {e}")

class KafkaSink(SinkFunction):
    def __init__(self, kafka_producer):
        self.kafka_producer = kafka_producer

    def invoke(self, value, context):
        # Send serialized data to Kafka
        self.kafka_producer.invoke(json.dumps(value))

# Function to send data to Kafka
def send_ticker_to_kafka(value, producer):
    print(f"Sending to Kafka: {value}")  # For debugging purposes
    value_dict = {
        "timestamp": value[0],  # timestamp
        "last_price": value[1],  # last_price
        "symbol": value[2]  # symbol
    }
    producer.invoke(json.dumps(value_dict))  # Send serialized JSON

# Setting up the Kafka producer
def setup_kafka_producer():
    # Define the serialization schema for the `Row` type
    row_type_info = Types.ROW([
        Types.STRING(),  # timestamp
        Types.STRING(),  # symbol
        Types.FLOAT()    # last_priceS
    ])
    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(row_type_info) \
        .build()
    # Return a Kafka producer with the JSON serialization schema
    return FlinkKafkaProducer(
        topic="ticker_data",
        serialization_schema=serialization_schema,
        producer_config={"bootstrap.servers": "kafka:9092"}
    )
def setup_ema_kafka_producer():
    # Define the serialization schema for the `Row` type specific to EMA
    row_type_info = Types.ROW([
        Types.STRING(),  # symbol
        Types.FLOAT(),  # EMA_38
        Types.FLOAT(),  # EMA_100
        Types.STRING(),  # advice
        Types.LONG(),  # advice timestamp
        Types.LONG()  # latency
    ])
    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(row_type_info) \
        .build()

    # Return a Kafka producer for EMA data, sending it to a different topic
    return FlinkKafkaProducer(
        topic="ema_data",  # This is the new topic for EMA data
        serialization_schema=serialization_schema,
        producer_config={"bootstrap.servers": "kafka:9092"}
    )

def main():
    #logging.warning("At least main is running successfully")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10000))
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(4)
    kafka_consumer = FlinkKafkaConsumer(
        topics="financial_data",
        properties={"bootstrap.servers": "kafka:9092", "group.id": "flink_consumer","max.poll.interval.ms": "600000","max.poll.records": "10000"},
        deserialization_schema=SimpleStringSchema() 
    )
   
    
    #Uncomment line for flink to take data from earliest instead of waiting for all to be loaded
    kafka_consumer.set_start_from_latest()
    parsed_stream = env.add_source(kafka_consumer).map(
        lambda record: parse_event(json.loads(record)),
        output_type=Types.ROW([
            Types.STRING(),  # timestamp_str
            Types.STRING(),  # symbol
            Types.STRING(),  # sec_type
            Types.STRING(),  # arrival_time
            Types.FLOAT()   # last_price
        ])
    ).filter(lambda x: x is not None)

    #env.set_parallelism(1)
    watermark_strategy =WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(SimpleTimestampAssigner()) \
        .with_idleness(Duration.of_seconds(5))
   
    #Sliding window
    #.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    #Tumbling window
    #.window(TumblingEventTimeWindows.of(Time.minutes(5)))
    windowed_stream = parsed_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[1]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
        .process(EMAProcessFunction(), Types.TUPLE([
            Types.STRING(),  # Symbol
            Types.STRING(),    # Window start
            Types.STRING(),    # Window end
            Types.FLOAT(),   # EMA_38 (or defaulted)
            Types.FLOAT(),   # EMA_100 (or defaulted)
            Types.STRING(),  # Breakout type (or "")
            Types.LONG(),     # Breakout timestamp (or -1)
            Types.LONG()
        ])).set_parallelism(4).filter(lambda x: x[6] != -1).set_parallelism(4)
    
    #env.set_parallelism(8)
    ema_kafka_producer = setup_ema_kafka_producer()
    ema_stream = windowed_stream.map(
        lambda x: Row(x[0], x[3], x[4], x[5], x[6], x[7]),  # symbol, EMA_38, EMA_100, advice, advice_timestamp, latency
        output_type=Types.ROW([
            Types.STRING(),  # symbol
            Types.FLOAT(),   # EMA_38
            Types.FLOAT(),   # EMA_100
            Types.STRING(),  # advice
            Types.LONG(),    # advice_timestamp
            Types.LONG()     # latency
        ])
    )
    ema_stream.add_sink(ema_kafka_producer)
   
    
    #env.set_parallelism(1)
    kafka_producer_ticker = setup_kafka_producer()
    for ticker in ticker_symbols:
        specific_ticker_symbol = ticker["symbol"]
        specific_ticker_symbol_as_index = ticker["index"]

        # Ensure the index exists
        #create_index_if_not_exists(specific_ticker_symbol_as_index)

        # Filter the stream for the specific ticker
        ticker_filtered_stream = parsed_stream.filter(lambda x: x[1] == specific_ticker_symbol)

        # Extract timestamp and last price for the ticker
        ticker_data_stream = ticker_filtered_stream.map(
            lambda x: Row(x[0], x[1], x[4]),  # Include timestamp_str, symbol, and last_price
            output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.FLOAT()])  # Define output schema
        )
        ticker_data_stream.add_sink(kafka_producer_ticker)
        #ticker_data_stream.map(
        #    lambda value: send_ticker_to_elasticsearch(value, specific_ticker_symbol_as_index)
        #)
        # Print the ticker data for debugging
        #ticker_data_stream.print().name(f"print {specific_ticker_symbol}")

        # Send the ticker data to Elasticsearch
        

    
    #logging.warning("Completed ticker streaming")


    #env.set_parallelism(6)
    
    # Uncomment to enable EMA value printing and storing in the elasticsearch index 
    
    #windowed_stream.print().name("print windows stream")
    
    #windowed_stream.map(send_ema_to_elasticsearch).set_parallelism(1)

    logging.warning("Completed ema streaming")
    
    # Execute the Flink job
    env.execute("Flink EMA Calculation and Breakout Detection")

if __name__ == '__main__':
    main()
