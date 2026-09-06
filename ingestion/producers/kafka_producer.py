import json

from confluent_kafka import Producer

from config import KAFKA_BOOTSTRAP_SERVERS


class KafkaProducer:
    def __init__(self):
        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
            }
        )

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(
                f"Message delivered to "
                f"{msg.topic()} "
                f"[partition {msg.partition()}] "
                f"at offset {msg.offset()}"
            )

    def send_message(self, topic, key, value):
        self.producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(value),
            callback=self.delivery_report,
        )

        self.producer.poll(0)

    def flush(self):
        self.producer.flush()