from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

while True:

    transaction = {
        "transaction_id": random.randint(100000, 999999),
        "account_id": random.randint(1000, 9999),
        "type": random.choice(["TRANSFER", "PAYMENT", "CASH_OUT", "DEBIT"]),
        "amount": round(random.uniform(100, 100000), 2),
        "timestamp": datetime.now().isoformat()
    }

    producer.send("transactions", transaction)

    print("Sent:", transaction)

    time.sleep(2)