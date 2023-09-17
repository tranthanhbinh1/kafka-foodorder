import logging
import sys
from datetime import datetime

from pyflink.common import Types, Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common import Row
from pyflink.datastream.functions import MapFunction


def read_from_kafka(env):
    class ParseTimestamp(MapFunction):
        def map(self, value: Row) -> Row:
            # Parse the "timestamp" field to a datetime object
            value[4] = datetime.fromisoformat(value[4])
            return value

    class CalculateLatency(MapFunction):
        def map(self, value: Row) -> Row:
            # Calculate the latency
            latency = datetime.now() - value[4]
            # Add the latency to the row
            return Row(value[0], value[1], value[2], value[3], value[4], latency)

    row_type_info = Types.ROW_NAMED(
        ["order_id", "user_id", "total_cost", "items", "timestamp", "latency"],
        [
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.SQL_TIMESTAMP(),
        ],
    )
    deserialization_schema = (
        JsonRowDeserializationSchema.Builder().type_info(row_type_info).build()
    )
    kafka_consumer = FlinkKafkaConsumer(
        topics="order_details",
        deserialization_schema=deserialization_schema,
        properties={"bootstrap.servers": "localhost:29092", "group.id": "test_group_1"},
    )
    kafka_consumer.set_start_from_latest()

    data_stream = env.add_source(kafka_consumer)
    data_stream = data_stream.map(ParseTimestamp(), output_type=row_type_info)
    data_stream = data_stream.map(CalculateLatency(), output_type=row_type_info)

    data_stream.print()

    env.execute()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    
    # Create a Configuration object
    config = Configuration()
    # config.set_string("state.backend", "hashmap")   # Default is RocksDB
    config.set_string("taskmanager.memory.managed.fraction", "0.35") # Default is 0.7
    config.set_string("pipeline.auto-watermark-interval", "100 ms") # Default is 200ms
    config.set_string("execution.buffer-timeout", "20 ms")  # Default is 100ms
    
    env = StreamExecutionEnvironment.get_execution_environment()
    read_from_kafka(env)
    
    print("start reading data from kafka")
