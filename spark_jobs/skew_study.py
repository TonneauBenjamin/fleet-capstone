"""
Skew study (Part C.3), on data produced with the simulator option --hot-vehicle.
Same per-vehicle aggregation as the daily report, run 4 ways in one application:
  1. report     : plain groupBy -> partial aggregation before the shuffle (like a combiner)
  2. naive      : repartition("vehicle_id") first -> all rows of V0001 go to one task, AQE off
  3. naive_aqe  : same as 2 with AQE on
  4. salted     : two-stage aggregation on (vehicle_id, salt), AQE off
Compare the task durations of the aggregation stage in the Spark UI (Stages tab, Summary Metrics).
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = os.environ.get("DATA_DIR", "data")

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True, help="YYYY-MM-DD")
parser.add_argument("--salt", type=int, default=8)
parser.add_argument("--copies", type=int, default=1, help="repeat every row N times to emulate a bigger day")
parser.add_argument("--keep-ui", action="store_true", help="keep the app (and the Spark UI) alive at the end")
args = parser.parse_args()

spark = (SparkSession.builder
         .appName(f"skew-study-{args.date}")
         .config("spark.sql.shuffle.partitions", str(args.salt))  # few tasks, easier to read in the UI
         .getOrCreate())

df = (spark.read.parquet(f"{DATA_DIR}/events")
      .filter(F.col("event_date") == args.date)
      .select("vehicle_id", "speed_kmh", "harsh_braking"))
if args.copies > 1:
    df = df.crossJoin(spark.range(args.copies)).drop("id")
# read once, so the timings below only measure the aggregation
df.cache()
total = df.count()

print(f"{total} rows, top vehicles:")
df.groupBy("vehicle_id").count().orderBy(F.col("count").desc()).show(3)

speeding = F.when(F.col("speed_kmh") > 120, 1).otherwise(0)
harsh = F.when(F.col("harsh_braking"), 1).otherwise(0)


def report_agg(d):
    return d.groupBy("vehicle_id").agg(
        F.count("*").alias("events"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        F.max("speed_kmh").alias("max_speed_kmh"),
        F.sum(speeding).alias("speeding_events"),
        F.sum(harsh).alias("harsh_braking_events"))


def salted_agg(d):
    # stage 1: the hot vehicle is split into args.salt groups
    partial = (d.withColumn("salt", (F.rand() * args.salt).cast("int"))
               .repartition("vehicle_id", "salt")
               .groupBy("vehicle_id", "salt")
               .agg(F.count("*").alias("n"),
                    F.sum("speed_kmh").alias("sum_speed"),
                    F.max("speed_kmh").alias("max_speed"),
                    F.sum(speeding).alias("speeding"),
                    F.sum(harsh).alias("harsh")))
    # stage 2: merge the groups (only args.salt rows per vehicle)
    return partial.groupBy("vehicle_id").agg(
        F.sum("n").alias("events"),
        (F.sum("sum_speed") / F.sum("n")).alias("avg_speed_kmh"),
        F.max("max_speed").alias("max_speed_kmh"),
        F.sum("speeding").alias("speeding_events"),
        F.sum("harsh").alias("harsh_braking_events"))


def run(name, aqe, result_df):
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    spark.sparkContext.setJobDescription(name)
    start = time.time()
    result_df.collect()
    print(f"{name:<10} AQE={aqe:<5} {time.time() - start:.2f} s")


report_agg(df).collect()  # warm-up (JVM/JIT), not timed
run("report", "false", report_agg(df))
run("naive", "false", report_agg(df.repartition("vehicle_id")))
run("naive_aqe", "true", report_agg(df.repartition("vehicle_id")))
run("salted", "false", salted_agg(df))

if args.keep_ui:
    print("Spark UI still up, Ctrl+C to stop")
    while True:
        time.sleep(60)
spark.stop()
