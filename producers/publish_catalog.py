"""
Part A1 -- publish the fleet catalog to the compacted topic `fleet_catalog`.

One JSON record per vehicle, key = vehicle_id. The catalog is generated from a
fixed seed, so every run publishes exactly the same 40 records.
"""
import json
import random
import sys

from confluent_kafka import Producer
from faker import Faker

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fleet_catalog"
NUM_VEHICLES = 40
SEED = 42

REGIONS = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]
# Payloads allowed by the subject, matched to a plausible vehicle type.
PAYLOADS_BY_TYPE = {
    "motorbike": [500],
    "van": [1000, 1500],
    "truck": [3000],
    "refrigerated_truck": [1500, 3000],
}


def build_catalog():
    rng = random.Random(SEED)          # private generator with a fixed seed
    fake = Faker("fr_FR")              # French-style plates
    fake.seed_instance(SEED)
    catalog = []
    for i in range(1, NUM_VEHICLES + 1):
        vehicle_type = rng.choice(list(PAYLOADS_BY_TYPE))
        catalog.append({
            "vehicle_id": f"V{i:04d}",
            "plate": fake.license_plate(),
            "vehicle_type": vehicle_type,
            "home_region": REGIONS[(i - 1) % len(REGIONS)],   # 8 vehicles per region
            "max_payload_kg": rng.choice(PAYLOADS_BY_TYPE[vehicle_type]),
        })
    return catalog


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,    # implies acks=all and safe retries
    })
    results = {"ok": 0, "failed": 0}

    def on_delivery(err, msg):
        # Called once per message, when the broker has confirmed or rejected it.
        if err is not None:
            results["failed"] += 1
            print(f"FAILED {msg.key().decode()}: {err}", file=sys.stderr)
        else:
            results["ok"] += 1
            print(f"{msg.key().decode()} -> partition {msg.partition()} offset {msg.offset()}")

    for record in build_catalog():
        producer.produce(TOPIC, key=record["vehicle_id"], value=json.dumps(record),
                         on_delivery=on_delivery)

    not_sent = producer.flush(30)      # wait up to 30 s for every confirmation
    print(f"delivered={results['ok']} failed={results['failed']} not_sent={not_sent}")
    if results["failed"] or not_sent:
        sys.exit(1)                    # non-zero exit code = scripts/Airflow see a failure


if __name__ == "__main__":
    main()
