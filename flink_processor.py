import os
from pyflink.common import Types, Row, Duration
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.time_characteristic import TimeCharacteristic
import logging
from datetime import datetime
from typing import Iterable, Optional, Dict, Tuple
from pyflink.datastream import RuntimeExecutionMode



from pyflink.common import Types
from pyflink.datastream.functions import MapFunction
import requests
import json

# Configure logging
logging.basicConfig(level=logging.INFO)

def parse_event(line: str) -> Optional[Row]:
    #logging.warning("Into parse_event")
    arrival_time = datetime.now().isoformat()
    try:
        if line.startswith("#") or not line.strip():
            #logging.debug(f"Ignoring comment or empty line: {line}")
            return None

        fields = line.split(",")
        if len(fields) < 39:
            #logging.warning(f"Incomplete line, skipping: {line}")
            return None

        symbol = fields[0].strip()
        sec_type = fields[1].strip()
        timestamp_str = f"{fields[2].strip()} {fields[3].strip()}"
        last_price = fields[21].strip()
        trading_time = fields[23].strip()
        trading_date = fields[26].strip()

        try:
            last_price = float(last_price) if last_price else None
        except ValueError:
            #logging.info(f"Invalid price format, skipping line: {line}")
            return None

        if not all([symbol, sec_type, trading_time, trading_date]) or last_price is None:
            #logging.warning(f"Missing mandatory fields in line: {line}")
            return None
    except Exception as e:
        #logging.error(f"Error parsing line: {e} for line: {line}")
        return None

    return Row(timestamp_str, symbol, sec_type, arrival_time, last_price, trading_time, trading_date)

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


def insert_data(index_name, data):
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



def main():
    logging.warning("At least main is running successfully")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    data_dir = "/opt/flink/jobs/data/trading_data/debs2022-gc-trading-day-08-11-21.csv"
    parsed_stream = env.read_text_file(data_dir) \
        .map(parse_event, output_type=Types.ROW([
            Types.STRING(),  # timestamp_str
            Types.STRING(),  # symbol
            Types.STRING(),  # sec_type
            Types.STRING(),  # arrival_time
            Types.FLOAT(),   # last_price
            Types.STRING(),  # trading_time
            Types.STRING()   # trading_date
        ])).filter(lambda x: x is not None)
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_minutes(1)) \
        .with_timestamp_assigner(SimpleTimestampAssigner())

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
        ])).set_parallelism(4)
    

    
    env.set_parallelism(1)
    windowed_stream.print().name("print windows stream")
    '''

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)  # Set execution mode

    temp_data = [
        ("2024-11-10T10:00:00", "AAPL", "Stock"),
        ("2024-11-10T10:01:00", "GOOG", "Stock"),
        ("2024-11-10T10:02:00", "MSFT", "Stock")
    ]

    insert_data("stock_data", temp_data)
    

    logging.warning("Completed mapped streaming")
    '''
    # Execute the Flink job
    env.execute("Flink EMA Calculation and Breakout Detection")

if __name__ == '__main__':
    main()
