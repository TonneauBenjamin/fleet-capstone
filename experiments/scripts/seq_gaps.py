"""
E3: did the simulator lose messages? The simulator numbers its events (event_id = V0001-<seq>),
seq +1 per event, so a missing seq = a message that never reached Kafka.
The Kafka timestamp is the producer's CreateTime: a message created while the broker was down keeps
its creation time even if it is written later.
Usage: python experiments/scripts/seq_gaps.py --since 2026-10-13T10:00:00 [--until ...]   (UTC)
"""
import argparse
import re
import time

from common import read_topic, parse_time

parser = argparse.ArgumentParser()
parser.add_argument("--since", required=True)
parser.add_argument("--until")
args = parser.parse_args()

# default: stop 10 s ago, the newest messages may not be in every partition yet
until = parse_time(args.until) if args.until else int((time.time() - 10) * 1000)
msgs = read_topic("vehicle_events", parse_time(args.since), until)
seqs = set()
for m in msgs:
    raw = m.value().decode()
    s = re.search(r'"event_id": "V\d{4}-(\d+)"', raw)
    if s:
        seqs.add(int(s.group(1)))

lo, hi = min(seqs), max(seqs)
missing = sorted(set(range(lo, hi + 1)) - seqs)
print(f"{len(msgs)} messages, seq {lo} -> {hi}, missing seq: {len(missing)} {missing[:20]}")
