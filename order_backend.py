import json
import time
from datetime import datetime
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 20_000

producer = KafkaProducer(bootstrap_servers="localhost:29092")


for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i * 2,
        "items": "burger, sandwich",
        "timestamp": datetime.now().isoformat()
    }

    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )
    print(f"Done sending...{i}")
    time.sleep(0.25)
