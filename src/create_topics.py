def create_topics(topic_names):
    from confluent_kafka.admin import AdminClient, NewTopic

    # Create an AdminClient instance
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})

    # Create a list of NewTopic instances
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topic_names]

    # Create the topics
    fs = admin_client.create_topics(new_topics)

    # Wait for the results
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")