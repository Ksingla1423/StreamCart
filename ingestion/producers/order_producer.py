from config import ORDERS_TOPIC
from generators.order_generator import generate_order


class OrderProducer:

    def __init__(self, kafka_producer):
        self.kafka_producer = kafka_producer

    def produce_order(self):
        order = generate_order()

        self.kafka_producer.send_message(
            topic=ORDERS_TOPIC,
            key=order["order_id"],
            value=order,
        )

        return order