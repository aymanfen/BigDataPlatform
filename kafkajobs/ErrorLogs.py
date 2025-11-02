from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'ErrorLogs',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

print("Listening to topic...")

for message in consumer:
    print(f"Received: {message.value}")
