class KafkaConsumer:
    def __init__(self, topic, bootstrap_servers='localhost:9092'):
        from confluent_kafka import Consumer
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'my_group',
            'auto.offset.reset': 'earliest'
        })
        self.topic = topic
        self.consumer.subscribe([self.topic])

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Timeout of 1 second
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                print(f"Received message: {msg.value().decode('utf-8')}")
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self):
        self.consumer.close()