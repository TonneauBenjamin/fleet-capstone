"""
One-off analysis for final_check_observations.md: the valid events of 2026-10-13 (up to 13:31 UTC)
that are missing from the Parquet without being late. Output: experiments/missing_events.txt
Usage: python experiments/scripts/missing_events.py
"""
import glob, json, sys
sys.path.insert(0, "experiments/scripts")
from collections import Counter
import pandas as pd
import pyarrow.dataset as ds
from confluent_kafka import Consumer, TopicPartition
from common import invalid_reason, to_epoch, to_ms

UNTIL = int(pd.Timestamp("2026-10-13T13:32:00Z").timestamp() * 1000)
c = Consumer({"bootstrap.servers": "kafka:29092", "group.id": "experiments", "enable.auto.commit": False})
valid = {}
for p in range(6):
    c.assign([TopicPartition("vehicle_events", p, 0)])
    done = False
    while not done:
        for m in c.consume(10000, timeout=5):
            if m.error():
                continue
            if m.timestamp()[1] > UNTIL:
                done = True
                break
            raw = m.value().decode()
            if invalid_reason(raw):
                continue
            d = json.loads(raw)
            valid[(d["event_id"], to_ms(d["event_time"]))] = (m.timestamp()[1] / 1000 - to_epoch(d["event_time"]), m.timestamp()[1])
c.close()
files = glob.glob("data/events/event_date=2026-10-13/batch_id=*/*.parquet")
t = ds.dataset(files, format="parquet", partitioning="hive", partition_base_dir="data/events").to_table(columns=["event_id", "event_time"]).to_pandas()
micros = (t.event_time - pd.Timestamp(0)) // pd.Timedelta(microseconds=1)
inp = set(zip(t.event_id, micros // 1000))
miss = [(k, v) for k, v in valid.items() if k not in inp and v[0] <= 120 and k[1] < UNTIL - 60000]
print("valid events up to 13:31 UTC:", len(valid), "missing and not late:", len(miss))
print("by minute (kafka ts):", Counter(str(pd.Timestamp(v[1], unit="ms"))[:16] for k, v in miss).most_common(12))
ids_time = {}
for eid, ms in inp:
    ids_time.setdefault(eid, []).append(ms)
gaps = sorted(min(abs(k[1] - x) for x in ids_time[k[0]]) / 1000 for k, v in miss if k[0] in ids_time)
print("of them, event_id present in the Parquet with another event_time:", len(gaps))
if gaps:
    print("gap to that other event (s): min %.0f median %.0f max %.0f" % (gaps[0], gaps[len(gaps) // 2], gaps[-1]))
