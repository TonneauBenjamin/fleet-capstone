import json
import os
import random

from confluent_kafka import Producer
from faker import Faker

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "fleet_catalog"

REGIONS = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]
TYPES = ["van", "truck", "refrigerated_truck", "motorbike"]
PAYLOADS = [500, 1000, 1500, 3000]


def main():
    # fixed seeds -> same catalog every time we republish
    random.seed(42)
    Faker.seed(42)
    fake = Faker("fr_FR")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
    })

    for i in range(1, 41):
        vehicle_id = f"V{i:04d}"
        record = {
            "vehicle_id": vehicle_id,
            "plate": fake.license_plate(),
            "vehicle_type": random.choice(TYPES),
            "home_region": random.choice(REGIONS),
            "max_payload_kg": random.choice(PAYLOADS),
        }
        producer.produce(TOPIC, key=vehicle_id, value=json.dumps(record))

    producer.flush()
    print("40 vehicles published to fleet_catalog")


if __name__ == "__main__":
    main()
