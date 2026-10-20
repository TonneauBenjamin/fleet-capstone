"""
Capstone Project -- telemetry simulator with INJECTED FAULTS.

Besides normal events, the simulator deliberately produces the problems a
real pipeline must survive:
  * ~3%  anomalies         speeding / harsh braking (these must raise alerts)
  * ~1%  duplicates        the exact same event_id sent twice
  * ~2%  late events       event_time 3-6 minutes in the past
  * ~1%  malformed events  broken JSON, missing fields, impossible values
  * optional hot vehicle   --hot-vehicle sends ~40% of all traffic from V0001
                           (key skew: one partition gets overloaded)

Usage:  python3 producers/produce_vehicle_events.py [--rate 20] [--hot-vehicle]
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "vehicle_events"
NUM_VEHICLES = 40

ANOMALY_RATE = 0.03
DUPLICATE_RATE = 0.01
LATE_RATE = 0.02
MALFORMED_RATE = 0.01


def make_event(vehicle_id: str, seq: int) -> dict:
    anomaly = random.random() < ANOMALY_RATE
    event_time = datetime.now(timezone.utc)
    if random.random() < LATE_RATE:
        event_time -= timedelta(minutes=random.uniform(3, 6))
    return {
        "event_id": f"{vehicle_id}-{seq}",
        "vehicle_id": vehicle_id,
        "event_time": event_time.isoformat(),
        "latitude": round(random.uniform(42.0, 51.0), 5),
        "longitude": round(random.uniform(-4.0, 8.0), 5),
        "speed_kmh": round(random.uniform(115, 140) if anomaly else random.uniform(10, 95), 1),
        "fuel_level_pct": round(random.uniform(5, 100), 1),
        "harsh_braking": anomaly and random.random() < 0.5,
    }


def corrupt(event: dict) -> str:
    kind = random.choice(["broken_json", "missing_vehicle", "bad_speed", "bad_time"])
    if kind == "broken_json":
        return json.dumps(event)[:-7]
    if kind == "missing_vehicle":
        event = dict(event)
        event.pop("vehicle_id")
    elif kind == "bad_speed":
        event = dict(event, speed_kmh=random.choice([-12.0, 999.0]))
    else:
        event = dict(event, event_time="not-a-timestamp")
    return json.dumps(event)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=20, help="events per second")
    parser.add_argument("--hot-vehicle", action="store_true", help="skew ~40%% of traffic onto V0001")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "linger.ms": 10,
        "batch.size": 32768,
    })
    vehicle_ids = [f"V{i:04d}" for i in range(1, NUM_VEHICLES + 1)]
    seq = 0
    print(f"streaming ~{args.rate} events/s (hot vehicle: {args.hot_vehicle}). Ctrl+C to stop.")
    try:
        while True:
            tick = time.time()
            for _ in range(args.rate):
                vehicle_id = "V0001" if args.hot_vehicle and random.random() < 0.4 else random.choice(vehicle_ids)
                seq += 1
                event = make_event(vehicle_id, seq)
                payload = corrupt(event) if random.random() < MALFORMED_RATE else json.dumps(event)
                producer.produce(TOPIC, key=vehicle_id, value=payload)
                if random.random() < DUPLICATE_RATE:
                    producer.produce(TOPIC, key=vehicle_id, value=payload)
            producer.poll(0)
            time.sleep(max(0.0, 1.0 - (time.time() - tick)))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
