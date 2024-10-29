from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, RestartStrategies, Row
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

# Function to parse each JSON event to extract necessary fields
def parse_event(event):
    arrival_time = datetime.now().isoformat()  # Record the arrival time
    try:
        # Attempt to load the JSON data
        data = json.loads(event)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e} for event: {event}")
        return None  # Return None for malformed JSON

    # Check if 'ID' is present and valid
    if 'ID' not in data or '.' not in data['ID']:
        logging.warning(f"Missing or invalid ID in event: {event}")
        return None  # Return None if ID is missing or malformed

    # Split the ID to extract symbol and exchange
    symbol, exchange = data['ID'].split('.')

    # Helper function to safely convert to float or return None
    def safe_float(value):
        if value in (None, ''):
            return None
        try:
            return float(value)
        except ValueError as e:
            logging.warning(f"Conversion error to float: {e} for value: {value}")
            return None  # Return None for conversion issues

    # Initialize parsed data with safe defaults
    parsed_data = {
        'symbol': symbol,
        'exchange': exchange,
        'sec_type': data.get('SecType', None),
        'arrival_time': arrival_time,
        'timestamp': None,
        'last_price': safe_float(data.get('Last', None)),
        'trading_time': data.get('Trading time', None),
        'trading_date': data.get('Trading date', None)
    }

    # Convert Date and Time to timestamp if both are present
    if 'Date' in data and 'Time' in data:
        try:
            parsed_data['timestamp'] = datetime.strptime(
                data['Date'] + ' ' + data['Time'], '%d-%m-%Y %H:%M:%S.%f'
            ).isoformat()
        except ValueError as e:
            logging.warning(f"Date/time parsing error: {e} for event: {event}")
            parsed_data['timestamp'] = None  # Handle parsing errors

    # Return as a Row; return None if mandatory fields are missing
    if any(value is None for value in [
        parsed_data['symbol'],
        parsed_data['exchange'],
        parsed_data['sec_type'],
        parsed_data['last_price'],
        parsed_data['timestamp'],
        parsed_data['arrival_time']
    ]):
        logging.warning(f"Missing mandatory fields in event: {event}")
        return None  # Ensure all mandatory fields are present before returning

    return Row(
        parsed_data['symbol'],
        parsed_data['exchange'],
        parsed_data['sec_type'],
        parsed_data['arrival_time'],
        parsed_data['timestamp'],
        parsed_data['last_price'],
        parsed_data['trading_time'],
        parsed_data['trading_date']
    )

# Main function to set up the Flink job
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10))
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka_2.12-1.14.0.jar")

    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'financial_data_group'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics='financial_data',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(kafka_consumer)

    # Map function to parse each event with relevant fields
    parsed_stream = stream.map(
        parse_event,
        output_type=Types.ROW(
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
    )

    # Filter out empty parsed data (ensure no None values)
    parsed_stream = parsed_stream.filter(lambda x: x is not None)

    # Print parsed data directly for monitoring
    parsed_stream.print().name("Print Parsed Events")

    env.execute("Flink Streaming of Financial Data")

if __name__ == '__main__':
    main()
