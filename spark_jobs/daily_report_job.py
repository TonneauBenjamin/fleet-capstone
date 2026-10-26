import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = os.environ.get("DATA_DIR", "data")

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="YYYY-MM-DD")
args = parser.parse_args()

spark = (SparkSession.builder
         .appName(f"daily-report-{args.date}")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

df = spark.read.parquet(f"{DATA_DIR}/events").filter(F.col("event_date") == args.date)

# proof of partition pruning: look for "PartitionFilters" in the FileScan line
df.explain()

metrics = [
    F.count("*").alias("events"),
    F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
    F.max("speed_kmh").alias("max_speed_kmh"),
    F.sum(F.when(F.col("speed_kmh") > 120, 1).otherwise(0)).alias("speeding_events"),
    F.sum(F.when(F.col("harsh_braking"), 1).otherwise(0)).alias("harsh_braking_events"),
]

per_vehicle = df.groupBy("vehicle_id").agg(*metrics).orderBy("vehicle_id")
per_region = df.groupBy("region").agg(*metrics).orderBy("region")

# worst = highest share of incidents (speeding + harsh braking) among its events.
# a rate and not a count, otherwise a vehicle that just sends more events looks worse
worst = (per_vehicle
         .withColumn("incidents", F.col("speeding_events") + F.col("harsh_braking_events"))
         .withColumn("incident_rate_pct", F.round(100 * F.col("incidents") / F.col("events"), 2))
         .orderBy(F.col("incident_rate_pct").desc(), F.col("max_speed_kmh").desc())
         .limit(5))

report = {
    "date": args.date,
    "per_region": [r.asDict() for r in per_region.collect()],
    "per_vehicle": [r.asDict() for r in per_vehicle.collect()],
    "worst_vehicles": [r.asDict() for r in worst.collect()],
}

os.makedirs(f"{DATA_DIR}/reports", exist_ok=True)
with open(f"{DATA_DIR}/reports/{args.date}.json", "w") as f:
    json.dump(report, f, indent=2)

print(json.dumps(report["per_region"], indent=2))
print(json.dumps(report["worst_vehicles"], indent=2))
spark.stop()
