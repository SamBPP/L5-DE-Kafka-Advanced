class KafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        from confluent_kafka import Producer
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def send_message(self, topic, message):
        self.producer.produce(topic, message)
        self.producer.flush()

    def close(self):
        self.producer.flush()