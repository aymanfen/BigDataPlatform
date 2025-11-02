import csv
import json
import time
from kafka import KafkaProducer

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

# Mapping of CSV files to Kafka topics
file_topic_map = {
    'DimAssure.csv': 'DimAssure',
    'DimAffilie.csv': 'DimAffilie',
    'FactDeclaration.csv': 'FactDeclaration'
}

# Loop over each file-topic pair
for filename, topic in file_topic_map.items():
    try:
        with open(filename, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Get the column names

            for row in reader:
                # Convert row to dictionary
                row_dict = dict(zip(header, row))

                # Serialize to JSON string
                jsonvalue = json.dumps(row_dict)

                # Send to Kafka
                producer.send(topic, value=jsonvalue)
                print(f"Sent to {topic}: {jsonvalue}")
                #time.sleep(0.1)  # optional throttling

    except Exception as e:
        print(f"Error processing {filename}: {e}")

# Ensure all messages are sent
producer.flush()
