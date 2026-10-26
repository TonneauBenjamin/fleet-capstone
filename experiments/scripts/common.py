import json
import os
from datetime import datetime

import glob
import time

import pyarrow.dataset as ds
from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
FIELDS = ["event_id", "vehicle_id", "event_time", "latitude", "longitude",
          "speed_kmh", "fuel_level_pct", "harsh_braking"]


def read_topic(topic, since_ms=None, until_ms=None):
    """All messages of a topic (or only those with a Kafka timestamp in [since_ms, until_ms])."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP_SERVERS, "group.id": "experiments", "enable.auto.commit": False})
    partitions = c.list_topics(topic, timeout=10).topics[topic].partitions
    out = []
    for p in partitions:
        low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        start = low
        if since_ms is not None:
            start = c.offsets_for_times([TopicPartition(topic, p, since_ms)], timeout=10)[0].offset
            if start < 0:
                continue
        if start >= high:
            continue
        c.assign([TopicPartition(topic, p, start)])
        while True:
            msgs = c.consume(10000, timeout=5)
            if not msgs:
                break
            for m in msgs:
                if m.error():
                    continue
                if until_ms is None or m.timestamp()[1] <= until_ms:
                    out.append(m)
            if msgs[-1].offset() >= high - 1:
                break
    c.close()
    return out


def invalid_reason(raw):
    """Same rules as the streaming job, rewritten in plain Python to check it."""
    try:
        d = json.loads(raw)
    except Exception:
        return "invalid_json"
    for f in FIELDS:
        if d.get(f) is None:
            return "missing_" + f
    try:
        datetime.fromisoformat(d["event_time"])
    except Exception:
        return "bad_timestamp"
    if not 0 <= d["speed_kmh"] <= 250:
        return "speed_out_of_range"
    return None


def to_epoch(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def to_ms(iso):
    """event_time truncated to the millisecond (Spark writes ms in the alerts JSON)"""
    return int(round(to_epoch(iso) * 1e6)) // 1000


def parse_time(s):
    """'2026-10-13T10:00:00' (UTC) -> epoch ms"""
    return int(datetime.fromisoformat(s + "+00:00").timestamp() * 1000)


def read_events(columns):
    """Parquet written by the streaming job. Only the finished files (not Spark's .spark-staging
    folders), and we retry if Spark replaced a file while we were reading (job still running)."""
    for attempt in range(5):
        try:
            files = glob.glob("data/events/event_date=*/batch_id=*/*.parquet")
            dataset = ds.dataset(files, format="parquet", partitioning="hive", partition_base_dir="data/events")
            return dataset.to_table(columns=columns).to_pandas()
        except FileNotFoundError:
            time.sleep(2)
    raise Exception("could not read data/events")
