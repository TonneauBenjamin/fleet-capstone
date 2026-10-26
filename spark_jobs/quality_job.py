import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = os.environ.get("DATA_DIR", "data")

# thresholds (justified in the report)
MIN_ROWS = 1000           # less than ~1 min of traffic at 20 events/s: the stream was basically down
MAX_DUPLICATE_PCT = 1.0   # Spark already dedups, so this only tolerates ids reused after a simulator restart
MAX_UNKNOWN_PCT = 5.0     # 1 unknown vehicle = 2.5% of the fleet, more than 2 = catalog problem

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="YYYY-MM-DD")
args = parser.parse_args()

spark = (SparkSession.builder
         .appName(f"quality-{args.date}")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

df = spark.read.parquet(f"{DATA_DIR}/events").filter(F.col("event_date") == args.date)

stats = df.agg(
    F.count("*").alias("rows"),
    F.countDistinct("event_id").alias("distinct_ids"),
    F.sum(F.when(F.col("region") == "UNKNOWN", 1).otherwise(0)).alias("unknown_rows"),
).first()

rows = stats["rows"]
duplicated_event_ids = rows - stats["distinct_ids"]
duplicate_pct = 100 * duplicated_event_ids / rows if rows else 0.0
unknown_pct = 100 * (stats["unknown_rows"] or 0) / rows if rows else 0.0

reasons = []
if rows < MIN_ROWS:
    reasons.append(f"only {rows} rows (min {MIN_ROWS})")
if duplicate_pct > MAX_DUPLICATE_PCT:
    reasons.append(f"{duplicate_pct:.2f}% duplicated event_ids (max {MAX_DUPLICATE_PCT}%)")
if unknown_pct > MAX_UNKNOWN_PCT:
    reasons.append(f"{unknown_pct:.2f}% rows with region UNKNOWN (max {MAX_UNKNOWN_PCT}%)")

verdict = {
    "date": args.date,
    "passed": len(reasons) == 0,
    "reasons": reasons,
    "rows": rows,
    "duplicated_event_ids": duplicated_event_ids,
    "duplicate_pct": round(duplicate_pct, 2),
    "unknown_pct": round(unknown_pct, 2),
}

os.makedirs(f"{DATA_DIR}/quality", exist_ok=True)
with open(f"{DATA_DIR}/quality/{args.date}.json", "w") as f:
    json.dump(verdict, f, indent=2)

print(json.dumps(verdict, indent=2))
spark.stop()
