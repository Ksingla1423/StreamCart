from config import PAYMENTS_TOPIC
from generators.payment_generator import generate_payment


class PaymentProducer:
    def __init__(self,kafka_producer):
        self.kafka_producer = kafka_producer
        
    def produce_payment(self, order):
        payment = generate_payment(order=order)

        self.kafka_producer.send_message(
            topic=PAYMENTS_TOPIC,
            key=payment["order_id"],
            value=payment,
        )

        return payment