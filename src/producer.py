import os
import json
import time
import numpy as np
import random
import math
import argparse
from faker import Faker
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime


def create_messages(producer,
                    topic_name,
                    n=10):
    print(f"Producing {n} order messages...")
    faker = Faker()
    for i in range(n):
        curr_time = datetime.now()
        order = {
            "order_id": str(math.floor(curr_time.timestamp())),
            "customer_name": faker.name(),
            "product": faker.word(ext_word_list=["Laptop", "Phone", "Headphones", "Monitor", "Keyboard"]),
            "quantity": int(np.random.choice([1, 2, 3, 4], p=[0.4, 0.3, 0.2, 0.1])),
            "price": round(random.uniform(10, 100), 2),
            "order_date": curr_time.strftime("%Y-%m-%d"),
            "order_time": curr_time.strftime("%H:%M:%S")
        }

        producer.send(topic_name,
                      key=order["order_id"], 
                      value=json.dumps(order))
        print(f"Sent: {order}")
        time.sleep(1)

    producer.flush()

if __name__ == "__main__":
    try:
        # Load environment variables from .env file
        load_dotenv()
        # Set up command line argument parsing
        parser = argparse.ArgumentParser(description="Produce fake order messages to Kafka (Redpanda)")
        parser.add_argument("--count", "-n", type=int, default=10, help="Number of messages to send")
        args = parser.parse_args()
        n = args.count

        # create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol='SASL_SSL',
            sasl_mechanism='SCRAM-SHA-256',
            sasl_plain_username=os.getenv("SASL_USERNAME"),
            sasl_plain_password=os.getenv("SASL_PASSWORD")
        )
        # create messages
        create_messages(producer, 'orders', n)
    except KeyboardInterrupt:
        # Handle keyboard interrupt
        print("Producer interrupted.")
    except Exception as e:
        # Handle other exceptions
        print(f"Error: {e}")
    finally:
        # Clean up and close the producer
        print("Flushing messages...")
        producer.close()
        print("Producer closed.")