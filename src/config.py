# File: /kafka-dev-env/kafka-dev-env/src/config.py

KAFKA_SERVER = 'localhost:9092'
KAFKA_TOPIC_CONFIG = {
    'num_partitions': 1,
    'replication_factor': 1,
    'cleanup_policy': 'delete'
}