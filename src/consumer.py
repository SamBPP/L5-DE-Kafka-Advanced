import os
import json
import argparse
from kafka import KafkaConsumer
from dotenv import load_dotenv


def consume_messages(bootstrap_servers,
                     topic_name='orders',
                     group_id='order-consumer-group'):
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
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    print(f"Connecting to Kafka server at {bootstrap_servers}...")
    print(f"Consumer group ID: {group_id}")
    print(f"Subscribed to topic: {topic_name}. Waiting for messages...\n")
    # Consume messages
    try:
        for message in consumer:
            # Print message details
            print(f"Key: {message.key}")
            print(f"Order Data: {json.dumps(message.value, indent=2)}\n---\n")
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