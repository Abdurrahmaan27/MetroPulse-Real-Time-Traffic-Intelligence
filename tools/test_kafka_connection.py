from kafka import KafkaProducer

print("Attempting connection to Kafka...")

try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:29092",
        request_timeout_ms=5000
    )
    future = producer.send("sensors.traffic.v1", b"test-message")
    record_metadata = future.get(timeout=10)

    print("✅ Successfully connected and sent a message!")
    print("Topic:", record_metadata.topic)
    print("Partition:", record_metadata.partition)
    print("Offset:", record_metadata.offset)

except Exception as e:
    print("❌ Failed to connect:")
    print(e)
