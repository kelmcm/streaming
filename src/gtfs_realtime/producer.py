from confluent_kafka import Producer
import pandas as pd
import json

config = {
    'bootstrap.servers': 'localhost:9092', # User-specific properties that you must set
    'acks': 'all' # Fixed properties
}
producer = Producer(config)

def send_message(producer, topic, df: pd.DataFrame):
    """Sends a message to Kafka"""
    count = 0
    records = json.loads(df.to_json(orient="records"))
    for record in records:
        producer.produce(topic=topic, value=json.dumps(record).encode("utf-8"), key=record['entity_id'], callback=delivery_callback)
        count += 1

        # Trigger any outstanding delivery report callbacks.
        producer.poll(0)

    # Block until the messages are delivered.
    producer.flush()


def delivery_callback(err, msg):
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
