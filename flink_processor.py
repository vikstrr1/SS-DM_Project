import os
from pyflink.common import Types, Row, Duration
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.window import TumblingEventTimeWindows, TimeWindow
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkaProducer
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import SinkFunction
from pyflink.datastream.formats.json import JsonRowSerializationSchema
import logging
from datetime import datetime
from typing import Iterable, Optional, Dict, Tuple

import json
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
smtp_user = os.getenv('SMTP_USER')
smtp_pass = os.getenv('SMTP_PASS')
smtp_recv = os.getenv('SMTP_RECV')

# Configure logging
logging.basicConfig(level=logging.WARNING)

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
        #uncomment for actual sending
        #server.sendmail(smtp_user, smtp_recv, text)
        server.quit()

        #logging.info(f"Email sent to {smtp_recv} with subject: {subject}")

    except Exception as e:
        logging.error(f"Failed to send email: {e}")

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
            #logging.warning("Received None as value, discarding event.")
            return -1

        iso_timestamp = value[0]
        if not iso_timestamp:
            #logging.warning(f"iso_timestamp is None or empty: {iso_timestamp}, discarding event.")
            return -1

        try:
            dt = datetime.strptime(iso_timestamp, "%d-%m-%Y %H:%M:%S.%f")
            #logging.debug(f"Parsed timestamp: {dt}")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logging.error(f"Timestamp parsing failed for {iso_timestamp}: {e}")
            return -1

class EMAProcessFunction(ProcessWindowFunction[Row, Tuple[str, int, int, float, float, str, Optional[int]], str, TimeWindow]):
    def __init__(self):
        self.previous_ema: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

    def process(self, key: str, context: ProcessWindowFunction.Context[TimeWindow], elements: Iterable[Row]) -> Iterable[Tuple[str, int, int, Optional[float], Optional[float], str, Optional[int]]]:
        prices = [element[4] for element in elements if element[4] is not None]
        dates = [element[3] for element in elements if element[4] is not None]
        latency = None
        if not prices:
            #logging.warning(f"No valid prices for symbol {key} in this window, skipping EMA calculation.")
            return []

        # Calculate EMA and handle None values
        alpha_38 = 2 / (38 + 1)
        alpha_100 = 2 / (100 + 1)

        previous_ema_38, previous_ema_100 = self.previous_ema.get(key, (None, None))

        ema_38 = self.calculate_ema(prices[-1], previous_ema_38, alpha_38)
        ema_100 = self.calculate_ema(prices[-1], previous_ema_100, alpha_100)
        
        # Check for breakout patterns and assign advice
        advice_type, advice_timestamp = self.check_advice(previous_ema_38, previous_ema_100, ema_38, ema_100,key, context)
        self.previous_ema[key] = (ema_38, ema_100)
        if advice_type:
            last_arrival_time = datetime.strptime(dates[-1], "%Y-%m-%dT%H:%M:%S.%f").timestamp()
            latency = datetime.now().timestamp() - last_arrival_time
            
        start_time = datetime.fromtimestamp(context.window().start / 1000).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.fromtimestamp(context.window().end / 1000).strftime('%Y-%m-%d %H:%M:%S')
        
        # Provide a default if any field might be None
        return [(key, start_time, end_time, ema_38 or 0.0, ema_100 or 0.0, advice_type or "", advice_timestamp or -1, latency)]

    def calculate_ema(self, current_price: float, previous_ema: Optional[float], alpha: float) -> float:
        if previous_ema is None:
            logging.debug(f"No previous EMA, using current price for EMA calculation: {current_price}")
            return current_price
        return (alpha * current_price) + ((1 - alpha) * previous_ema)

    def check_advice(self, previous_ema_38: Optional[float], previous_ema_100: Optional[float], ema_38: float, ema_100: float,symbol: str, context: ProcessWindowFunction.Context[TimeWindow]) -> Tuple[str, Optional[int]]:
        advice_type = None
        advice_timestamp = None
        if previous_ema_38 is not None and previous_ema_100 is not None:
            # Bullish breakout: EMA38 crosses above EMA100
            if ema_38 > ema_100 and previous_ema_38 <= previous_ema_100:
                advice_type = "Buy"
                advice_timestamp = context.current_processing_time()
                formatted_timestamp = datetime.fromtimestamp(advice_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                #if symbol in watch_list:
                send_email(f"EMA Sell Signal for {symbol}", f"Symbol: {symbol}\nAdvice: Sell\nTimestamp: {formatted_timestamp}")
                logging.info(f"Bullish breakout detected for {symbol} - Buy advice generated.")
            # Bearish breakout: EMA38 crosses below EMA100
            elif ema_100 > ema_38 and previous_ema_100 <= previous_ema_38:
                advice_type = "Sell"
                advice_timestamp = context.current_processing_time()
                formatted_timestamp = datetime.fromtimestamp(advice_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                #if symbol in watch_list:
                send_email(f"EMA Sell Signal for {symbol}", f"Symbol: {symbol}\nAdvice: Sell\nTimestamp: {formatted_timestamp}")
                logging.info(f"Bearish breakout detected for {symbol} - Sell advice generated.")
        return advice_type, advice_timestamp

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
        Types.STRING(),  # Symbol
        Types.STRING(),    # Window start
        Types.STRING(),    # Window end
        Types.FLOAT(),   # EMA_38 (or defaulted)
        Types.FLOAT(),   # EMA_100 (or defaulted)
        Types.STRING(),  # Breakout type (or "")
        Types.LONG(),     # Breakout timestamp (or -1)
        Types.LONG()        #Latency
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
    logging.warning("At least main is running successfully")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    #set parallism depending on resources available
    env.set_parallelism(4)
    kafka_consumer = FlinkKafkaConsumer(
        topics="financial_data",
        properties={"bootstrap.servers": "kafka:9092", "group.id": "flink_consumer","max.poll.interval.ms": "600000","max.poll.records": "10000"},
        deserialization_schema=SimpleStringSchema() 
    )
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
    
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
        .with_timestamp_assigner(SimpleTimestampAssigner()) \
        .with_idleness(Duration.of_seconds(5))
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
            Types.LONG(),     # Breakout timestamp (or -1)
            Types.LONG()        #Latency
        ])).filter(lambda x: x[6] != -1)
    
    
    ema_kafka_producer = setup_ema_kafka_producer()
    ema_stream = windowed_stream.map(
        lambda x: Row(x[0],x[1],x[2], x[3], x[4], x[5], x[6], x[7]),  # symbol, EMA_38, EMA_100, advice, advice_timestamp, latency
        output_type=Types.ROW([
            Types.STRING(),  # Symbol
            Types.STRING(),    # Window start
            Types.STRING(),    # Window end
            Types.FLOAT(),   # EMA_38 (or defaulted)
            Types.FLOAT(),   # EMA_100 (or defaulted)
            Types.STRING(),  # Breakout type (or "")
            Types.LONG(),     # Breakout timestamp (or -1)
            Types.LONG()        #Latency
        ])
    )

    ema_stream.add_sink(ema_kafka_producer)
    
    
    
    kafka_producer_ticker = setup_kafka_producer()
    # Send the ticker data to Kafka
    #ticker_filtered_stream = parsed_stream.filter(lambda x: x[1] )
    ticker_data_stream = parsed_stream.map(
            lambda x: Row(x[0], x[1], x[4]),  # Include timestamp_str, symbol, and last_price
            output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.FLOAT()])  # Define output schema
        )
    ticker_data_stream.add_sink(kafka_producer_ticker)

    #ticker_data_stream.map(
    #    lambda value: send_ticker_to_elasticsearch(value, specific_ticker_symbol_as_index)
    #)
    
    logging.warning("Completed ticker streaming")


   
    
    # Uncomment to enable EMA value printing and storing in the elasticsearch index 
    
    #windowed_stream.print().name("print windows stream")
    
    #windowed_stream.map(send_ema_to_elasticsearch).set_parallelism(1)

    logging.warning("Completed ema streaming")
    
    # Execute the Flink job
    env.execute("Flink EMA Calculation and Breakout Detection")

if __name__ == '__main__':
    main()
