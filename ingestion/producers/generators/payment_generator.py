import random
import uuid
from datetime import datetime, timezone


def generate_payment(order: dict) -> dict:
    payment_status = random.choices(
        ["SUCCESS", "FAILED"],
        weights=[0.9, 0.1],
    )[0]

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "PAYMENT_PROCESSED",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),

        "payment_id": f"PAY_{uuid.uuid4().hex[:8].upper()}",

        # Link payment to a REAL order
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],

        "amount": round(
            order["quantity"] * order["unit_price"],
            2,
        ),

        "currency": order["currency"],

        "payment_method": random.choice(
            [
                "CREDIT_CARD",
                "UPI",
                "DEBIT_CARD",
                "WALLET",
            ]
        ),
    
        "payment_status": payment_status,
    }