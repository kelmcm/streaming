from confluent_kafka import Producer
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)

@staticmethod
def _delivery_callback(err, msg):
    # Per-message delivery callback (triggered by poll() or flush()) when a message has been
    # successfully delivered or permanently failed delivery (after retries).
    if err:
        logger.error('ERROR: Message failed delivery: {}'.format(err))
    else:
        logger.info("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

class MessageProducer:
    """
    Generic message producer class that sends messages to a Kafka topic.
    Currently, receives the messages in a Dataframe, converts the dataframe to JSON and sends to Kafka.
    """
    def __init__(self, bootstrap_servers: str = 'localhost:9092', extra_config: dict = None):
        config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',
            **(extra_config or {}),
        }
        self.producer = Producer(config)

    def send_dataframe(self, topic: str, key_cols: list[str], df: pd.DataFrame):
        """Converts dataframe to JSON and sends a message to Kafka topic"""
        count = 0
        records = json.loads(df.to_json(orient="records"))
        for record in records:
            key = ":".join(str(record[col]) for col in key_cols)
            self.producer.produce(topic=topic, value=json.dumps(record).encode("utf-8"), key=key, callback=_delivery_callback)
            count += 1

            # Trigger any outstanding delivery report callbacks.
            self.producer.poll(0)

        # Block until the messages are delivered.
        self.producer.flush()
