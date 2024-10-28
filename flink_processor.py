from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, RestartStrategies
from datetime import timedelta
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    restart_strategy = RestartStrategies.fixed_delay_restart(3, 10)  
    env.set_restart_strategy(restart_strategy)

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

    # Assuming incoming data is JSON formatted
    stream = stream.map(lambda x: json.loads(x), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Key the stream by symbol
    keyed_stream = stream.key_by(lambda x: x['ID'])

    # Output the results (for example, print to console)
    keyed_stream.print()

    env.execute("Flink Kafka Integration Example")

if __name__ == '__main__':
    main()
