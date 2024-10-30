import os
from pyflink.common import Types, Row, Duration
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.time_characteristic import TimeCharacteristic
import logging
from datetime import datetime
from typing import Iterable

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Function to parse each CSV row to extract necessary fields
def parse_event(line: str):
    arrival_time = datetime.now().isoformat()  # Record the arrival time
    try:
        if line.startswith("#") or not line.strip():
            return None

        fields = line.split(",")
        symbol, exchange  = fields[0].strip().split(".") # ID
        sec_type = fields[1].strip()  # SecType
        timestamp_str = f"{fields[2].strip()} {fields[3].strip()}"
        last_price = fields[22].strip()
        trading_time = fields[23].strip()
        trading_date = fields[27].strip()
        last_price = float(last_price) if last_price else None

        if any(value is None for value in [symbol, exchange, sec_type, last_price, trading_time, trading_date]):
            #logging.warning(f"Missing mandatory fields in line: {line}")
            return None
    except (ValueError, IndexError) as e:
        logging.error(f"Error unpacking line: {e} for line: {line}")
        return None

    return Row(symbol, exchange, sec_type, arrival_time, timestamp_str, last_price, trading_time, trading_date)

class SimpleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        iso_timestamp = value[3]
        if iso_timestamp is not None:
            try:
                dt = datetime.fromisoformat(str(iso_timestamp))
                return int(dt.timestamp() * 1000)
            except ValueError as e:
                logging.error(f"Timestamp parsing failed for {iso_timestamp}: {e}")
                return int(datetime.now().timestamp() * 1000)
        else:
            logging.warning("Timestamp is None, defaulting to current time.")
            return int(datetime.now().timestamp() * 1000)

class EMAProcessFunction(ProcessWindowFunction[Row, tuple, str, TimeWindow]):
    def __init__(self):
        self.previous_ema_38 = None
        self.previous_ema_100 = None

    def process(self, key: str, context: ProcessWindowFunction.Context[TimeWindow], elements: Iterable[Row]) -> Iterable[tuple]:
        prices = [element[5] for element in elements if element[5] is not None]  # Extract non-null last_price from each event
        if not prices:
            logging.warning("No valid prices in this window, skipping EMA calculation.")
            return []

        alpha_38 = 2 / (38 + 1)
        alpha_100 = 2 / (100 + 1)

        # Calculate EMA for current window based on previous EMA if available
        ema_38 = prices[0] if self.previous_ema_38 is None else (alpha_38 * prices[-1] + (1 - alpha_38) * self.previous_ema_38)
        ema_100 = prices[0] if self.previous_ema_100 is None else (alpha_100 * prices[-1] + (1 - alpha_100) * self.previous_ema_100)

        # Detect breakout pattern
        breakout_type = None
        breakout_timestamp = None
        if self.previous_ema_38 is not None and self.previous_ema_100 is not None:
            if ema_38 > ema_100 and self.previous_ema_38 <= self.previous_ema_100:
                breakout_type = "Bullish Breakout"
                breakout_timestamp = context.current_processing_time()  # Assign timestamp for breakout detection
            elif ema_100 > ema_38 and self.previous_ema_100 <= self.previous_ema_38:
                breakout_type = "Bearish Breakout"
                breakout_timestamp = context.current_processing_time()  # Assign timestamp for breakout detection

        # Update state
        self.previous_ema_38 = ema_38
        self.previous_ema_100 = ema_100

        # Log and return results
        print(f"Processing key: {key}, window: [{context.window().start}, {context.window().end}], EMA_38: {ema_38}, EMA_100: {ema_100}, Breakout: {breakout_type}, Breakout Timestamp: {breakout_timestamp}"))
        logging.info(f"Processing key: {key}, window: [{context.window().start}, {context.window().end}], EMA_38: {ema_38}, EMA_100: {ema_100}, Breakout: {breakout_type}, Breakout Timestamp: {breakout_timestamp}")
        return [(key, context.window().start, context.window().end, ema_38, ema_100, breakout_type, breakout_timestamp)]

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    # Reading and parsing multiple CSV files
    #Change path to correct
    data_dir = "/Users/rasmusvikstrom/SS-DM_Project/data/trading_data/"
    #data_dir = "/Users/rasmusvikstrom/SS-DM_Project/data/trading_data/"
    
    # Create a list to hold all parsed events
    parsed_events = []

    # Loop through each CSV file and parse it
    for filename in os.listdir(data_dir):
        with open(os.path.join(data_dir, filename)) as file:
            for line in file:
                event = parse_event(line)
                if event is not None:  # Only add non-None events
                    parsed_events.append(event)
    parsed_stream = env.from_collection(
        [(parse_event(line) for filename in os.listdir(data_dir) 
          for line in open(os.path.join(data_dir, filename)) if line.startswith("#") or not line.strip() is None)],
        Types.ROW(
            [
                Types.STRING(),  # symbol
                Types.STRING(),  # exchange
                Types.STRING(),  # sec_type
                Types.STRING(),  # arrival_time
                Types.STRING(),  # timestamp
                Types.FLOAT(),   # last_price
                Types.STRING(),  # trading_time
                Types.STRING()   # trading_date
            ]
        )
    ).filter(lambda x: x is not None)

    # Defining watermark strategy
    watermark_strategy = WatermarkStrategy \
        .for_monotonous_timestamps() \
        .with_timestamp_assigner(SimpleTimestampAssigner())

    # Applying the watermark strategy and processing function
    windowed_stream = parsed_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .process(EMAProcessFunction(), Types.TUPLE([Types.STRING(), Types.LONG(), Types.LONG(), Types.FLOAT(), Types.FLOAT(), Types.STRING(), Types.LONG()]))

    # Printing the results
    windowed_stream.print().name("Print Windowed Events")
    
    # Execute the Flink job
    env.execute("Flink EMA Calculation and Breakout Detection")

if __name__ == '__main__':
    main()
