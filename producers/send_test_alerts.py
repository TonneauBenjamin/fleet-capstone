"""
Test helper -- send fake alerts to `alerts` so the notifier can be tested before the
Spark job exists. Same JSON format as docs/schemas.md. Key = vehicle_id, like Spark.

Usage: python3 producers/send_test_alerts.py --count 120 --rate 2 --dup-every 10
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "alerts"


def make_alert(run_id, seq):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "alert_id": f"TEST-{run_id}-{seq}:speeding",
        "alert_type": "speeding",
        "vehicle_id": f"V{random.randint(1, 40):04d}",
        "event_time": now,
        "detected_at": now,
        "details": {"speed_kmh": round(random.uniform(121, 140), 1)},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=120, help="number of alerts")
    parser.add_argument("--rate", type=float, default=2, help="alerts per second")
    parser.add_argument("--dup-every", type=int, default=10, help="send every Nth alert twice (0 = never)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True})
    run_id = int(time.time())
    for seq in range(1, args.count + 1):
        alert = make_alert(run_id, seq)
        payload = json.dumps(alert)
        producer.produce(TOPIC, key=alert["vehicle_id"], value=payload)
        if args.dup_every and seq % args.dup_every == 0:
            producer.produce(TOPIC, key=alert["vehicle_id"], value=payload)
            print(f"sent {alert['alert_id']} TWICE")
        producer.poll(0)
        time.sleep(1 / args.rate)
    producer.flush(30)
    print(f"done: {args.count} alerts sent")


if __name__ == "__main__":
    main()
