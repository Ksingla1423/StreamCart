import time

from config import ORDERS_TOPIC, PAYMENTS_TOPIC
from generators.order_generator import generate_order

from kafka_producer import KafkaProducer
from payment_producer import PaymentProducer
from order_producer import OrderProducer


if __name__ == "__main__":

    # one shared kafka producer
    kafka_producer = KafkaProducer()
    
    #busincess_specific producers
    order_producer = OrderProducer(kafka_producer)
    payment_producer = PaymentProducer(kafka_producer)

    try:
        while True:

            order = order_producer.produce_order()
            
            print (
                f"Produced order : {order['order_id']}"
            )
            
            payment= payment_producer.produce_payment(order)
            
            
            print(
                f"Produced payment : {payment['payment_id']} for order : {order['order_id']}"
            )

            # Generate one order every second
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        kafka_producer.flush()
        print("Producer stopped.")