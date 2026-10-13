"""
Part A2 -- alerts notifier.

Consumer in group `alerts-notifier`:
  * cooperative-sticky assignment: a rebalance only moves the partitions that must move
  * manual commit AFTER the alert is handled: at-least-once (no loss, possible re-delivery)
  * skips alert_ids it has already handled, because delivery is at-least-once

Usage (one terminal per instance):  python3 -u consumers/alerts_notifier.py --name A
"""
import argparse
import json
import signal
from collections import OrderedDict
from datetime import datetime, timezone

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "alerts"
GROUP_ID = "alerts-notifier"
SEEN_MAX = 10_000

running = True


def stop(signum, frame):
    global running
    running = False


def now():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def log(text):
    """Print, but never crash if the output (terminal or tee) has gone away."""
    try:
        print(text, flush=True)
    except (BrokenPipeError, OSError):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="notifier", help="label printed in the logs")
    args = parser.parse_args()
    name = args.name
    owned = set()

    def on_assign(consumer, partitions):
        owned.update(p.partition for p in partitions)
        log(f"[{now()}] [{name}] ASSIGNED +{sorted(p.partition for p in partitions)} -> owns {sorted(owned)}")

    def on_revoke(consumer, partitions):
        owned.difference_update(p.partition for p in partitions)
        log(f"[{now()}] [{name}] REVOKED  -{sorted(p.partition for p in partitions)} -> owns {sorted(owned)}")

    def on_lost(consumer, partitions):
        owned.difference_update(p.partition for p in partitions)
        log(f"[{now()}] [{name}] LOST     -{sorted(p.partition for p in partitions)} -> owns {sorted(owned)}")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    })
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    seen = OrderedDict()
    log(f"[{now()}] [{name}] started, waiting for partitions...")
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log(f"[{now()}] [{name}] Kafka error: {msg.error()}")
                continue
            where = f"p{msg.partition()} o{msg.offset()}"
            try:
                alert = json.loads(msg.value())
                alert_id = str(alert["alert_id"])
                alert_type = str(alert.get("alert_type", "unknown"))
            except (ValueError, KeyError, TypeError):
                log(f"[{now()}] [{name}] {where} unreadable alert, skipped")
                consumer.commit(message=msg, asynchronous=False)
                continue

            if alert_id in seen:
                log(f"[{now()}] [{name}] {where} DUPLICATE {alert_id} skipped")
            else:
                log(f"[{now()}] [{name}] {where} {alert_type.upper()} "
                    f"vehicle={alert.get('vehicle_id')} details={alert.get('details')}")
                seen[alert_id] = True
                if len(seen) > SEEN_MAX:
                    seen.popitem(last=False)

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        log(f"[{now()}] [{name}] closed")


if __name__ == "__main__":
    main()
