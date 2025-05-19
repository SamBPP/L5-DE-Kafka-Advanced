from quixstreams import Application
from faker import Faker
import random
import time
import json
from datetime import datetime

class OrderProducer:
    def __init__(self, broker_address="localhost:8080", topic_name="orders"):
        self.app = Application(broker_address)
        self.topic = self.app.topic(topic_name)
        self.producer = self.app.get_producer()
        self.faker = Faker()

    def generate_order(self, n=1):
        curr_time = datetime.now()
        return {
            "order_id": n,
            "customer_name": self.faker.name(),
            "product": self.faker.word(ext_word_list=["Laptop", "Phone", "Headphones", "Monitor", "Keyboard"]),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(10, 100), 2),
            "order_date": curr_time.strftime("%Y-%m-%d"),
            "order_time": curr_time.strftime("%H:%M:%S")
        }

    def send_orders(self, count=10, delay=1):
        for i in range(count):
            if i == 0:
                order = self.generate_order()
            else:
                order = self.generate_order(i+1)
            message = json.dumps(order)
            self.producer.produce(self.topic, message.encode("utf-8"))
            print(f"Sent: {message}")
            time.sleep(delay)
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.app.close()


if __name__ == "__main__":
    producer = OrderProducer()
    print("Starting producer...")
    try:
        producer.send_orders(count=10, delay=1)
    finally:
        producer.close()
    print("Producer finished.")