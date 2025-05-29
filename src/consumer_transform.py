import os
import json
import argparse
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

def transform_messages(bootstrap_servers,
                       topic_name='orders',
                       group_id='order-consumer-group',
                       new_topic_name='orders_clean'):

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
    
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-256',
        sasl_plain_username=os.getenv("SASL_USERNAME"),
        sasl_plain_password=os.getenv("SASL_PASSWORD")
    )

    # Consume messages
    try:
        for message in consumer:
            # Print message details
            message_key = message.key
            message_value = message.value
            print(f"Key: {message_key}")
            print(f"Order Data: {json.dumps(message_value, indent=2)}\n---\n")

            order = json.loads(message_value)
            new_order = {
                "order_id": order["order_id"],
                "product": order["product"],
                "total_price": order["quantity"] * order["price"]
            }
            # Send transformed message to new topic
            producer.send(new_topic_name,
                          key=new_order["order_id"],
                          value=json.dumps(new_order))
            print(f"Sent Order ID: {new_order["order_id"]}")

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

    transform_messages(bootstrap_servers, topic_name=args.topic, group_id=args.group)