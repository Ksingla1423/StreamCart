import random
import uuid
from datetime import datetime, timezone


def generate_order() -> dict:
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(100, 5000), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),

        "order_id": f"ORD_{uuid.uuid4().hex[:8].upper()}",
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "product_id": f"PROD_{random.randint(100, 500)}",

        "quantity": quantity,
        "unit_price": unit_price,

        "currency": "INR",
    }