"""
Batches of one streaming query, read from its checkpoint:
offsets/N is written when batch N starts, commits/N when it ends.
Usage: python experiments/scripts/stream_stats.py --checkpoint data/checkpoints/q1_events [--last 20]
"""
import argparse
import json
import os
import statistics

parser = argparse.ArgumentParser()
parser.add_argument("--checkpoint", default="data/checkpoints/q1_events")
parser.add_argument("--last", type=int, default=0, help="only print the last N batches")
args = parser.parse_args()


def offsets(n):
    with open(f"{args.checkpoint}/offsets/{n}") as f:
        lines = f.read().splitlines()
    return sum(json.loads(lines[2])["vehicle_events"].values())


batches = sorted(int(b) for b in os.listdir(f"{args.checkpoint}/commits") if b.isdigit())
rows = []
for n in batches:
    try:
        start = os.path.getmtime(f"{args.checkpoint}/offsets/{n}")
        end = os.path.getmtime(f"{args.checkpoint}/commits/{n}")
        records = offsets(n) - (offsets(n - 1) if n > 0 else 0)
    except FileNotFoundError:
        continue  # Spark only keeps the last ~100 batches in the checkpoint
    rows.append((n, start, end, records))

if not rows:
    raise SystemExit("no finished batch yet")

print("batch  duration_s  records")
for n, start, end, records in rows[-args.last:]:
    print(f"{n:>5}  {end - start:>10.2f}  {records:>7}")

durations = [end - start for _, start, end, _ in rows]
total = sum(r for *_, r in rows)
span = rows[-1][2] - rows[0][1]
print(f"{len(rows)} batches, {total} records in {span:.0f} s -> {total / span:.0f} records/s")
print(f"batch duration: mean {statistics.mean(durations):.2f} s, max {max(durations):.2f} s")
