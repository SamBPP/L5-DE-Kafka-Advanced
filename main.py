from src.producer import QuixProducer
from src.consumer import QuixConsumer
import threading
import time

def run_producer():
    producer = QuixProducer()
    try:
        for i in range(10):
            producer.send_message(f"Hello Kafka {i}")
            time.sleep(1)
    finally:
        producer.close()

def run_consumer():
    consumer = QuixConsumer()
    consumer.consume_messages()

if __name__ == "__main__":
    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.start()

    time.sleep(2)  # Wait for consumer to start
    run_producer()
