# Kafka Development Environment

This project sets up a local Kafka development environment using Docker. It includes scripts to create Kafka topics, produce messages, and consume messages.

## Project Structure

```
kafka-dev-env
├── src
│   ├── create_topics.py      # Script to create Kafka topics
│   ├── producer.py            # Kafka producer implementation
│   ├── consumer.py            # Kafka consumer implementation
│   └── config.py              # Configuration settings for Kafka connection
├── docker-compose.yml         # Docker configuration for Kafka and Zookeeper
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

## Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd kafka-dev-env
   ```

2. **Install dependencies**:
   Make sure you have Python and pip installed. Then run:
   ```bash
   pip install -r requirements.txt
   ```

3. **Start Kafka and Zookeeper**:
   Use Docker Compose to start the services:
   ```bash
   docker-compose up -d
   ```

4. **Create Topics**:
   Use the `create_topics.py` script to create the necessary Kafka topics. Modify the script as needed to specify your topic names.

5. **Produce Messages**:
   Use the `producer.py` script to send messages to your Kafka topics. You can customize the message content and topic name in the script.

6. **Consume Messages**:
   Use the `consumer.py` script to read messages from your Kafka topics. Adjust the topic name in the script to match the one you are consuming from.

## Usage Examples

- To create topics, run:
  ```bash
  python src/create_topics.py
  ```

- To produce messages, run:
  ```bash
  python src/producer.py
  ```

- To consume messages, run:
  ```bash
  python src/consumer.py
  ```

## Additional Information

- Ensure that Docker is installed and running on your machine.
- The Kafka server is configured to run on `localhost:9092` by default. Adjust the configuration in `src/config.py` if needed.
- For more details on Kafka, refer to the [Apache Kafka documentation](https://kafka.apache.org/documentation/).