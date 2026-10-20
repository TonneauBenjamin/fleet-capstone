"""
Demo only: one vehicle drives at 108-118 km/h for 2 minutes, 4 events/s,
so the sustained_speeding rule (mean > 100 km/h over >= 5 events in 5 min) fires.
4 events/s because the simulator keeps sending ~0.5 event/s at ~52 km/h for the same
vehicle, which pulls the mean down. Below 120 km/h on purpose: no point alerts.
The provided simulator is not modified.

Usage: python producers/scenario_sustained_speeding.py [--vehicle V0007] [--seconds 120] [--rate 4]
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--vehicle", default="V0007")
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--rate", type=int, default=4, help="events per second")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True})
    n = 0
    for _ in range(args.seconds):
        for _ in range(args.rate):
            n += 1
            event = {
                "event_id": f"SCN-{args.vehicle}-{int(time.time())}-{n}",
                "vehicle_id": args.vehicle,
                "event_time": datetime.now(timezone.utc).isoformat(),
                "latitude": round(random.uniform(42.0, 51.0), 5),
                "longitude": round(random.uniform(-4.0, 8.0), 5),
                "speed_kmh": round(random.uniform(108, 118), 1),
                "fuel_level_pct": round(random.uniform(5, 100), 1),
                "harsh_braking": False,
            }
            producer.produce("vehicle_events", key=args.vehicle, value=json.dumps(event))
        producer.poll(0)
        time.sleep(1)
    producer.flush()
    print(f"sent {n} events for {args.vehicle}")


if __name__ == "__main__":
    main()
