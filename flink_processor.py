import os
from pyflink.common import Types, Row, Duration
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.common.serialization import SimpleStringSchema
import logging
from datetime import datetime
from typing import Iterable, Optional, Dict, Tuple

import json
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)

def parse_event(record: dict) -> Optional[Row]:
    # Convert Kafka message to row format expected by Flink
    arrival_time = datetime.now().isoformat()
    try:
        symbol = record.get("ID")  # Adjusted to use 'ID' as the symbol identifier
        sec_type = record.get("SecType")
        timestamp_str = f"{record.get('Date')} {record.get('Time')}"
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

    return Row(trading_timestamp_str, symbol, sec_type, arrival_time, last_price)


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
            dt = datetime.strptime(iso_timestamp, "%d-%m-%Y %H:%M:%S.%f")
            logging.debug(f"Parsed timestamp: {dt}")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logging.error(f"Timestamp parsing failed for {iso_timestamp}: {e}")
            return -1

class EMAProcessFunction(ProcessWindowFunction[Row, Tuple[str, int, int, float, float, str, Optional[int]], str, TimeWindow]):
    def __init__(self):
        self.previous_ema: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

    def process(self, key: str, context: ProcessWindowFunction.Context[TimeWindow], elements: Iterable[Row]) -> Iterable[Tuple[str, int, int, Optional[float], Optional[float], str, Optional[int]]]:
        prices = [element[4] for element in elements if element[4] is not None]
        if not prices:
            logging.warning(f"No valid prices for symbol {key} in this window, skipping EMA calculation.")
            return []

        # Calculate EMA and handle None values
        alpha_38 = 2 / (38 + 1)
        alpha_100 = 2 / (100 + 1)

        previous_ema_38, previous_ema_100 = self.previous_ema.get(key, (None, None))

        ema_38 = self.calculate_ema(prices[-1], previous_ema_38, alpha_38)
        ema_100 = self.calculate_ema(prices[-1], previous_ema_100, alpha_100)
        
        # Check for breakout patterns and assign advice
        advice_type, advice_timestamp = self.check_advice(previous_ema_38, previous_ema_100, ema_38, ema_100, context)
        self.previous_ema[key] = (ema_38, ema_100)

        start_time = datetime.fromtimestamp(context.window().start / 1000).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(context.window().end / 1000).strftime('%Y-%m-%d %H:%M:%S')
        
        # Provide a default if any field might be None
        return [(key, start_time, end_time, ema_38 or 0.0, ema_100 or 0.0, advice_type or "", advice_timestamp or -1)]

    def calculate_ema(self, current_price: float, previous_ema: Optional[float], alpha: float) -> float:
        if previous_ema is None:
            logging.debug(f"No previous EMA, using current price for EMA calculation: {current_price}")
            return current_price
        return (alpha * current_price) + ((1 - alpha) * previous_ema)

    def check_advice(self, previous_ema_38: Optional[float], previous_ema_100: Optional[float], ema_38: float, ema_100: float, context: ProcessWindowFunction.Context[TimeWindow]) -> Tuple[str, Optional[int]]:
        advice_type = None
        advice_timestamp = None
        if previous_ema_38 is not None and previous_ema_100 is not None:
            # Bullish breakout: EMA38 crosses above EMA100
            if ema_38 > ema_100 and previous_ema_38 <= previous_ema_100:
                advice_type = "Buy"
                advice_timestamp = context.current_processing_time()
                logging.info(f"Bullish breakout detected for {context.window().start} - Buy advice generated.")
            # Bearish breakout: EMA38 crosses below EMA100
            elif ema_100 > ema_38 and previous_ema_100 <= previous_ema_38:
                advice_type = "Sell"
                advice_timestamp = context.current_processing_time()
                logging.info(f"Bearish breakout detected for {context.window().start} - Sell advice generated.")
        return advice_type, advice_timestamp


def insert_static_data(index_name, data):
    # Elasticsearch URL (adjust the host/port as needed)
    es_url = f"https://172.18.0.1:9200/{index_name}/_bulk"

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
    es_url = "https://172.18.0.1:9200/ema_data/_bulk"
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
    es_url = "https://172.18.0.1:9200/"+tickername+"/_bulk"
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
    es_url = f"https://172.18.0.1:9200/{index_name}"
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


def main():
    logging.warning("At least main is running successfully")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    kafka_consumer = FlinkKafkaConsumer(
        topics="financial_data",
        properties={"bootstrap.servers": "kafka:9092", "group.id": "flink_consumer"},
        deserialization_schema=SimpleStringSchema() 
    )
    kafka_consumer.set_start_from_earliest()
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
    
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
        .with_timestamp_assigner(SimpleTimestampAssigner())
    #.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    
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
            Types.LONG()     # Breakout timestamp (or -1)
        ])).set_parallelism(4).filter(lambda x: x[6] != -1)
    
    
    specific_ticker_symbol = "ALREW.FR"
    specific_ticker_symbol_as_index= "alrew.fr"

    create_index_if_not_exists(specific_ticker_symbol_as_index)
    ticker_filtered_stream = parsed_stream.filter(lambda x: x[1] == specific_ticker_symbol)

    ticker_data_stream = ticker_filtered_stream.map(
        lambda x: Row(x[0], x[4]),  # Only return timestamp_str and last_price
        output_type=Types.ROW([
            Types.STRING(),  # timestamp_str
            Types.FLOAT()     # last_price
        ])
    )
    ticker_data_stream.print().name("print tickers")

    ticker_data_stream.map(
        lambda value: send_ticker_to_elasticsearch(value, specific_ticker_symbol_as_index)
    ).set_parallelism(1)
    
    logging.warning("Completed ticker streaming")


    env.set_parallelism(1)
    
    # Uncomment to enable EMA value printing and storing in the elasticsearch index 
    
    windowed_stream.print().name("print windows stream")
    
    #windowed_stream.map(send_ema_to_elasticsearch).set_parallelism(1)

    logging.warning("Completed ema streaming")
    
    # Execute the Flink job
    env.execute("Flink EMA Calculation and Breakout Detection")

if __name__ == '__main__':
    main()
