from quixstreams import Application

class QuixConsumer:
    def __init__(self, broker_address="localhost:9092", topic_name="test-topic"):
        self.app = Application(broker_address)
        self.topic = self.app.topic(topic_name)
        self.consumer = self.app.get_consumer()

    def consume_messages(self):
        def callback(message):
            print(f"Received message: {message.value().decode('utf-8')}")

        self.consumer.subscribe(self.topic, callback)
        self.app.run()  # Blocking call; listens indefinitely

    def close(self):
        self.app.close()
