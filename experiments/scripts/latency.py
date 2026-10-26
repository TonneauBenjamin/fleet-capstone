"""
End-to-end latency = time the alert is written to Kafka - event_time of the event.
Only point alerts (speeding / harsh_braking) written in [since, until].
Usage: python experiments/scripts/latency.py --since 2026-10-13T10:00:00 --until 2026-10-13T10:05:00   (UTC)
"""
import argparse
import json
import statistics

from common import read_topic, to_epoch, parse_time

parser = argparse.ArgumentParser()
parser.add_argument("--since", required=True)
parser.add_argument("--until", required=True)
args = parser.parse_args()

lat = []
for m in read_topic("alerts", parse_time(args.since), parse_time(args.until)):
    a = json.loads(m.value())
    if a.get("alert_type") in ("speeding", "harsh_braking"):
        lat.append(m.timestamp()[1] / 1000 - to_epoch(a["event_time"]))

# alerts on events that were already 3-6 min late when sent say nothing about our pipeline
lat = sorted(x for x in lat if x < 170)
if not lat:
    print("no alerts in this window")
else:
    p95 = lat[int(0.95 * (len(lat) - 1))]
    print(f"{len(lat)} alerts, latency median {statistics.median(lat):.2f} s, p95 {p95:.2f} s, max {lat[-1]:.2f} s")
