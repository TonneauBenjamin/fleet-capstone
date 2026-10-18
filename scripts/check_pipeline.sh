#!/usr/bin/env bash
# Snapshot of the whole pipeline: what is in each topic, what Spark wrote, how far the notifiers are.
# Usage: ./scripts/check_pipeline.sh            (save it: ./scripts/check_pipeline.sh | tee raw_outputs/check.txt)
cd "$(dirname "$0")/.."
KAFKA="docker exec capstone-kafka /opt/kafka/bin"
BS="--bootstrap-server localhost:9092"

total() { $KAFKA/kafka-get-offsets.sh $BS --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'; }
read_topic() { $KAFKA/kafka-console-consumer.sh $BS --topic "$1" --from-beginning --timeout-ms 8000 2>/dev/null; }

echo "== $(date -u '+%Y-%m-%d %H:%M:%S') UTC"
echo "== Messages written to each topic (sum of end offsets)"
for t in vehicle_events vehicle_events_dlq alerts fleet_catalog; do
  printf "  %-20s %s\n" "$t" "$(total $t)"
done

echo "== Dead-letter queue: rejected messages per reason"
read_topic vehicle_events_dlq | grep -o '"reason":"[^"]*"' | sort | uniq -c | sed 's/^/  /'

echo "== Alerts per type"
read_topic alerts | grep -o '"alert_type":"[^"]*"' | sort | uniq -c | sed 's/^/  /'

echo "== Parquet written by Query 1 (data/events)"
python3 - <<'PY'
from pathlib import Path
try:
    import pyarrow.dataset as ds
except ImportError:
    raise SystemExit("  pyarrow not available: activate the venv")
root = Path("data/events")
if not root.exists():
    raise SystemExit("  no Parquet yet (is Spark running?)")
d = ds.dataset(str(root), format="parquet", partitioning="hive")
t = d.to_table(columns=["event_id", "region", "event_date", "batch_id"]).to_pandas()
print(f"  rows={len(t)}  distinct event_id={t.event_id.nunique()}  "
      f"micro-batches={t.batch_id.nunique()}  days={sorted(map(str, t.event_date.unique()))}")
print("  rows per region:", t.region.value_counts().to_dict())
PY

echo "== Consumer group alerts-notifier (LAG = alerts not handled yet)"
$KAFKA/kafka-consumer-groups.sh $BS --describe --group alerts-notifier 2>/dev/null | sed 's/^/  /'
