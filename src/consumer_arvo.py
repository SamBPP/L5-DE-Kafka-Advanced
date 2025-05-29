import os
import json
import argparse
from avro.io import DatumReader, BinaryDecoder
from avro.schema import Parse
import io
from kafka import KafkaConsumer
from dotenv import load_dotenv

STORAGE_FILE = "consumed_messages.json"

# Load Avro schema
schema_path = "order.avsc"
schema = Parse(open(schema_path, "r").read())

def deserialize_avro(bytes_obj, schema):
    bytes_reader = io.BytesIO(bytes_obj)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    return reader.read(decoder)

def load_stored_messages():
    if not os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "w") as f:
            json.dump({}, f)
    with open(STORAGE_FILE, "r") as f:
        return json.load(f)

def save_stored_messages(data):
    with open(STORAGE_FILE, "w") as f:
        json.dump(data, f, indent=2)
    return

def consume_messages(bootstrap_servers,
                     topic_name='orders',
                     group_id='order-consumer-group'):
    # Load existing messages
    stored_messages = load_stored_messages()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-256',
        sasl_plain_username=os.getenv("SASL_USERNAME"),
        sasl_plain_password=os.getenv("SASL_PASSWORD"),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None, # need to keep in as it won't be deserialized otherwise
        value_deserializer=lambda v: v  # Avro bytes are already in bytes format
    )
    print(f"Connecting to Kafka server at {bootstrap_servers}...")
    print(f"Consumer group ID: {group_id}")
    print(f"Subscribed to topic: {topic_name}. Waiting for messages...\n")
    # Consume messages
    try:
        for message in consumer:
            # Print message details
            message_key = message.key
            message_value = deserialize_avro(message.value, schema)
            print(f"Key: {message_key}")
            print(f"Order Data: {json.dumps(message_value, indent=2)}\n---\n")
            # Store message in the dictionary
            if message_key not in stored_messages:
                print(f"New Message Received:\nKey: {message_key}\n{json.dumps(message_value, indent=2)}\n---")
                stored_messages[message_key] = message_value
                save_stored_messages(stored_messages)
            else:
                print(f"Duplicate Key Skipped: {message_key}")

    except KeyboardInterrupt:
        # Handle keyboard interrupt
        print("\nConsumer interrupted.")
    finally:
        # Close the consumer connection
        consumer.close()
        print("Consumer connection closed.")

if __name__ == "__main__":
    load_dotenv()
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")

    parser = argparse.ArgumentParser(description="Consume order messages from Kafka (Redpanda)")
    parser.add_argument("--topic", "-t", type=str, default="orders", help="Kafka topic to consume from")
    parser.add_argument("--group", "-g", type=str, default="order-consumer-group", help="Consumer group ID")
    args = parser.parse_args()

    consume_messages(bootstrap_servers, topic_name=args.topic, group_id=args.group)