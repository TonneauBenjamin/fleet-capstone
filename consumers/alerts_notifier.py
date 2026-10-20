import os

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
NAME = os.environ.get("NOTIFIER_NAME", "notifier")

assigned = set()


def on_assign(consumer, partitions):
    assigned.update(p.partition for p in partitions)
    print(f"[{NAME}] assigned {[p.partition for p in partitions]} -> partitions now {sorted(assigned)}")


def on_revoke(consumer, partitions):
    assigned.difference_update(p.partition for p in partitions)
    print(f"[{NAME}] revoked {[p.partition for p in partitions]} -> partitions now {sorted(assigned)}")


def handle_alert(msg):
    print(f"[{NAME}] ALERT p{msg.partition()} offset {msg.offset()}: {msg.value().decode()}")


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "alerts-notifier",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    })
    consumer.subscribe(["alerts"], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_revoke)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{NAME}] error: {msg.error()}")
                continue
            handle_alert(msg)
            # commit only once the alert is handled
            try:
                consumer.commit(message=msg, asynchronous=False)
            except KafkaException as e:
                # refused during a rebalance: the partition moved, its new owner will read this alert again
                print(f"[{NAME}] commit failed: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
