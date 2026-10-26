"""
E1: compare what entered Kafka with what the streaming job produced.
  - Parquet: duplicated / missing events (an event = (event_id, event_time))
  - alerts: point alerts published more than once
  - DLQ: more messages than invalid inputs
Usage: python experiments/scripts/check_pipeline.py
"""
import json
from collections import Counter

import pandas as pd

from common import read_topic, invalid_reason, to_epoch, to_ms, read_events

events = read_topic("vehicle_events")
valid, invalid = {}, 0
for m in events:
    raw = m.value().decode()
    if invalid_reason(raw):
        invalid += 1
        continue
    d = json.loads(raw)
    key = (d["event_id"], to_ms(d["event_time"]))
    delay = m.timestamp()[1] / 1000 - to_epoch(d["event_time"])
    valid[key] = (d, delay)

print(f"vehicle_events: {len(events)} messages, {invalid} invalid, {len(valid)} distinct valid events")

t = read_events(["event_id", "event_time", "batch_id"])
micros = (t.event_time - pd.Timestamp(0)) // pd.Timedelta(microseconds=1)
t["key"] = list(zip(t.event_id, micros // 1000))
rows = len(t)
distinct = t.key.nunique()
in_parquet = set(t.key)
missing = [k for k in valid if k not in in_parquet]
late_missing = sum(1 for k in missing if valid[k][1] > 120)
print(f"parquet: {rows} rows, {distinct} distinct events, {rows - distinct} duplicated rows")
print(f"missing from parquet: {len(missing)} ({late_missing} of them were sent > 2 min late)")
if rows != distinct:
    dup = t[t.key.duplicated(keep=False)]
    print("batches holding the duplicates:", sorted(dup.batch_id.unique().tolist()))

alerts = [json.loads(m.value()) for m in read_topic("alerts")]
point = [a for a in alerts if a.get("alert_type") in ("speeding", "harsh_braking")]
c = Counter((a["event_id"], to_ms(a["event_time"])) for a in point)
print(f"point alerts: {len(point)} published, {len(c)} distinct, {sum(v - 1 for v in c.values())} duplicates")
expected = [k for k, (d, delay) in valid.items()
            if (d["speed_kmh"] > 120 or d["harsh_braking"]) and k in in_parquet]
print(f"events that should have raised a point alert: {len(expected)}, without alert: {len(set(expected) - set(c))}")
print("alerts by type:", dict(Counter(a.get("alert_type") for a in alerts)))

dlq = read_topic("vehicle_events_dlq")
print(f"dlq: {len(dlq)} messages for {invalid} invalid inputs -> {len(dlq) - invalid} extra")
