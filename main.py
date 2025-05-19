import argparse
from src.create_topics import create_topics
from src.producer import run_producer
from src.consumer import run_consumer

def main():
    parser = argparse.ArgumentParser(description="Kafka Advanced Dev Server")
    parser.add_argument('--create-topics', action='store_true', help='Create Kafka topics')
    parser.add_argument('--produce', action='store_true', help='Run Kafka producer')
    parser.add_argument('--consume', action='store_true', help='Run Kafka consumer')
    parser.add_argument('--topic', type=str, default='test-topic', help='Kafka topic name')
    parser.add_argument('--num-messages', type=int, default=10, help='Number of messages to produce/consume')
    args = parser.parse_args()

    if args.create_topics:
        create_topics([args.topic])
    elif args.produce:
        run_producer(args.topic, args.num_messages)
    elif args.consume:
        run_consumer(args.topic, args.num_messages)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()