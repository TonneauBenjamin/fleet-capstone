import json
import os
from datetime import datetime, timezone

from airflow.decorators import dag, task
from confluent_kafka import Consumer, Producer, TopicPartition

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
GROUP = "alerts-notifier"
TOPIC = "alerts"
LAG_THRESHOLD = 50  # per partition, justified in the report


@dag(
    dag_id="kafka_lag_monitor",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
)
def kafka_lag_monitor():

    @task
    def measure_lag():
        # same group.id but no subscribe(): we only read its committed offsets,
        # so we never join the group and never trigger a rebalance of the notifiers
        consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP,
            "enable.auto.commit": False,
        })
        partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        committed = consumer.committed([TopicPartition(TOPIC, p) for p in partitions], timeout=10)

        lags = {}
        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            # no commit yet (offset < 0) -> everything still in the partition is unread
            start = tp.offset if tp.offset >= 0 else low
            lags[str(tp.partition)] = high - start
        consumer.close()
        print(f"lag of {GROUP} per partition: {lags}")
        return lags

    @task
    def alert_if_high(lags):
        high = {p: lag for p, lag in lags.items() if lag > LAG_THRESHOLD}
        if not high:
            print(f"lag ok (threshold {LAG_THRESHOLD})")
            return
        alert = {
            "alert_type": "consumer_lag_high",
            "group": GROUP,
            "lags": lags,
            "threshold": LAG_THRESHOLD,
            "measured_at": datetime.now(timezone.utc).isoformat(),
        }
        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True})
        producer.produce("alerts", key=GROUP, value=json.dumps(alert))
        if producer.flush(30) > 0:
            raise Exception("alert not delivered")
        print(f"consumer_lag_high published: {high}")

    alert_if_high(measure_lag())


kafka_lag_monitor()
