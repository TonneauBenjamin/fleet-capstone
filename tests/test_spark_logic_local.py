"""Local test of the Spark logic of spark_jobs/streaming_app.py, WITHOUT Kafka.

Messages are generated with the real simulator functions (producers/produce_vehicle_events.py),
written to JSON files with the same columns as the Kafka source (key, value, partition, offset,
timestamp) and streamed by Spark through the SAME functions as the real application:
validate -> DLQ / valid -> deduplicate -> enrich -> Query 1 batch writer (Parquet + alerts).
Kafka sinks are replaced by memory / file sinks. It checks, with numbers:
  * every message gets exactly one verdict (DLQ + valid = input), DLQ reasons = injected faults
  * no duplicated event_id in Parquet; late events dropped by the 2-minute watermark
  * region = latest catalog record (update and tombstone included), UNKNOWN otherwise
  * one alert per speeding / harsh-braking event, deterministic alert_id
  * replaying a micro-batch with the same batch_id does not add rows (idempotent Parquet write)

Run from the project root, venv active (no Kafka needed):  python3 tests/test_spark_logic_local.py
"""
import json, random, shutil, sys, time, zlib
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
APP_DIR, SIM_DIR, WORK = ROOT / "spark_jobs", ROOT / "producers", ROOT / "data" / "test_run"
sys.path.insert(0, str(APP_DIR)); sys.path.insert(0, str(SIM_DIR))
import streaming_app as app
import produce_vehicle_events as sim

from pyspark.sql import SparkSession, functions as F, types as T

shutil.rmtree(WORK, ignore_errors=True); WORK.mkdir(parents=True)
src, parquet, ck = WORK / "src", WORK / "parquet", WORK / "ck"
src.mkdir()

spark = (SparkSession.builder.master("local[4]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "6")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------------- catalog: duplicates per key, one update, one tombstone ----------------
cat_rows, off = [], 0
def cat(vid, region, offset_):
    return {"key": vid, "value": None if region is None else json.dumps(
        {"vehicle_id": vid, "plate": "AA-000-AA", "vehicle_type": "van", "home_region": region,
         "max_payload_kg": 1000}), "offset": offset_}
regions = ["Paris", "Lyon", "Marseille", "Lille", "Toulouse"]
for rnd in range(3):                               # published 3 times (like Step 1)
    for i in range(1, 41):
        cat_rows.append(cat(f"V{i:04d}", regions[(i - 1) % 5], off)); off += 1
cat_rows.append(cat("V0002", "Toulouse", off)); off += 1   # V0002 moved Lyon -> Toulouse (latest wins)
cat_rows.append(cat("V0003", None, off)); off += 1         # V0003 deleted (tombstone)
cat_schema = T.StructType([T.StructField("key", T.StringType()), T.StructField("value", T.StringType()),
                           T.StructField("offset", T.LongType())])
catalog = app.latest_catalog(spark.createDataFrame(cat_rows, cat_schema)).cache()
cat_map = {r.vehicle_id: r.home_region for r in catalog.collect()}
print("CATALOG rows:", len(cat_map), "| V0002 ->", cat_map.get("V0002"), "| V0003 present:", "V0003" in cat_map)
expected_region = lambda v: cat_map.get(v, "UNKNOWN")

# ---------------- messages from the real simulator ----------------
random.seed(7)
truth = []            # one entry per Kafka message
offsets = Counter()
def emit(vehicle_id, payload, meta):
    p = zlib.crc32(vehicle_id.encode()) % 6
    truth.append(dict(meta, key=vehicle_id, value=payload, partition=p, offset=offsets[p],
                      timestamp=datetime.now(timezone.utc).isoformat()))
    offsets[p] += 1

def generate(n, force_late=0):
    vids = [f"V{i:04d}" for i in range(1, 41)]
    for _ in range(n):
        vid = random.choice(sim.UNKNOWN_VEHICLES) if random.random() < 0.02 else random.choice(vids)
        ev = sim.make_event(vid, len(truth), False)
        late = (datetime.now(timezone.utc) - datetime.fromisoformat(ev["event_time"])) > timedelta(minutes=2.5)
        if random.random() < 0.03:                     # more corruption than real, to test every kind
            payload = sim.corrupt(dict(ev))
            if payload.endswith("}") is False: kind = "broken_json"
            else:
                d = json.loads(payload)
                missing = [f for f in ("vehicle_id", "event_time", "speed_kmh") if f not in d]
                kind = (f"missing_field:{missing[0]}" if missing else
                        "bad_timestamp" if d["event_time"] == "not-a-timestamp" else "speed_out_of_range")
            emit(vid, payload, dict(kind=kind, event_id=ev["event_id"], late=late, dup=False))
        else:
            emit(vid, json.dumps(ev), dict(kind="VALID", event_id=ev["event_id"], late=late, dup=False, ev=ev))
            if random.random() < 0.02:
                emit(vid, json.dumps(ev), dict(kind="VALID", event_id=ev["event_id"], late=late, dup=True, ev=ev))
    for _ in range(force_late):                       # late events, 3-6 minutes old
        vid = random.choice(vids)
        ev = sim.make_event(vid, len(truth), False)
        ev["event_time"] = (datetime.now(timezone.utc) - timedelta(minutes=random.uniform(3, 6))).isoformat(timespec="milliseconds")
        emit(vid, json.dumps(ev), dict(kind="VALID", event_id=ev["event_id"], late=True, dup=False, ev=ev))

def write_file(name, rows):
    with open(src / f"{name}.json", "w") as f:
        for r in rows:
            f.write(json.dumps({k: r[k] for k in ("key", "value", "partition", "offset", "timestamp")}) + "\n")

# ---------------- streaming pipeline (same functions as the real app) ----------------
kafka_like = T.StructType([T.StructField("key", T.StringType()), T.StructField("value", T.StringType()),
                           T.StructField("partition", T.IntegerType()), T.StructField("offset", T.LongType()),
                           T.StructField("timestamp", T.TimestampType())])
raw = spark.readStream.schema(kafka_like).option("maxFilesPerTrigger", 1).json(str(src))
validated = app.validate(raw)
alerts_out = WORK / "alerts"
def file_alert_sink(df):  # replaces the Kafka write in the test
    df.write.mode("append").json(str(alerts_out))

q_dlq = (app.dlq_messages(validated).writeStream.format("memory").queryName("dlq_out")
         .option("checkpointLocation", str(ck / "dlq")).start())
q_valid = (app.valid_events(validated).writeStream.format("memory").queryName("valid_out")
           .option("checkpointLocation", str(ck / "valid")).start())
enriched = app.enrich(app.deduplicate(app.valid_events(validated)), catalog)
q1 = (enriched.writeStream.outputMode("append")
      .foreachBatch(app.make_batch_writer(parquet, file_alert_sink))
      .option("checkpointLocation", str(ck / "query1")).start())
queries = [q_dlq, q_valid, q1]

generate(1500)
batch1 = list(truth)
write_file("b1", batch1)
for q in queries: q.processAllAvailable()
n1 = len(truth)
# batch 2: new events + late events + duplicates of batch-1 events (within the watermark)
generate(500, force_late=40)
for r in random.sample([t for t in batch1 if t["kind"] == "VALID" and not t["late"]], 30):
    emit(r["key"], r["value"], dict(r, dup=True))
write_file("b2", truth[n1:])
for q in queries: q.processAllAvailable()
for p in q1.recentProgress:
    print("    progress batch", p["batchId"], "rows", p["numInputRows"], [(op["numRowsTotal"], op["numRowsDroppedByWatermark"]) for op in p.get("stateOperators", [])])
late_dropped = sum(op["numRowsDroppedByWatermark"] for p in q1.recentProgress for op in p.get("stateOperators", []))
for q in queries: q.stop()

# ---------------- checks ----------------
ok = True
def check(label, got, exp):
    global ok
    good = got == exp
    ok &= good
    print(f"{'OK ' if good else 'BAD'} {label}: got {got}, expected {exp}")

n_in = len(truth)
dlq = spark.sql("select * from dlq_out").collect()
valid_n = spark.sql("select count(*) c from valid_out").collect()[0].c
check("every message gets exactly one verdict (DLQ + valid = input)", len(dlq) + valid_n, n_in)
got_reasons = Counter(json.loads(r.value)["reason"] for r in dlq)
exp_reasons = Counter(t["kind"] for t in truth if t["kind"] != "VALID")
check("DLQ reasons match the injected corruption", dict(sorted(got_reasons.items())), dict(sorted(exp_reasons.items())))
check("DLQ keeps the original key", all(r.key is not None for r in dlq), True)

pq = spark.read.parquet(str(parquet))
rows = pq.collect()
ids = [r.event_id for r in rows]
check("no duplicated event_id in Parquet", len(ids), len(set(ids)))
valid_truth = [t for t in truth if t["kind"] == "VALID"]
# the FIRST micro-batch has no watermark yet (it is computed at the end of a batch), so late events
# of batch 1 are kept; from batch 2 on, events older than (max event_time - 2 min) are dropped
b1_ids = {t["event_id"] for t in truth[:n1]}
exp_ids = {t["event_id"] for t in valid_truth if not t["late"] or t["event_id"] in b1_ids}
got_ids = set(ids)
check("Parquet = valid, deduplicated, non-late events", got_ids == exp_ids, True)
late_ids = {t["event_id"] for t in valid_truth if t["late"]}
print(f"    late events sent: {len(late_ids)} (batch 1: {len(late_ids & b1_ids)}), late events in Parquet: {len(late_ids & got_ids)}, "
      f"dropped by watermark (Spark metric): {late_dropped}")
bad_region = [(r.vehicle_id, r.region) for r in rows if r.region != expected_region(r.vehicle_id)]
check("region from latest catalog record, UNKNOWN otherwise", bad_region, [])
print("    regions:", dict(Counter(r.region for r in rows)))
print("    V0002 region(s):", {r.region for r in rows if r.vehicle_id == "V0002"},
      "| V0003:", {r.region for r in rows if r.vehicle_id == "V0003"},
      "| V0041-45:", {r.region for r in rows if r.vehicle_id >= "V0041"})
print("    partitions:", sorted({(str(r.event_date), r.batch_id) for r in rows}))

alerts = [json.loads(l) for f in sorted(alerts_out.glob("*.json")) for l in open(f)]
alert_vals = [json.loads(a["value"]) for a in alerts]
exp_alerts = sum(1 for r in rows if r.speed_kmh > 120) + sum(1 for r in rows if r.harsh_braking)
check("one alert per speeding / harsh-braking event", len(alert_vals), exp_alerts)
check("alert_ids unique and deterministic", len({a["alert_id"] for a in alert_vals}), len(alert_vals))
print("    alert types:", dict(Counter(a["alert_type"] for a in alert_vals)), "| example:", alerts[0]["key"], alert_vals[0])

# ---------------- idempotence of the Parquet write on replay ----------------
before = spark.read.parquet(str(parquet)).count()
last_id = spark.read.parquet(str(parquet)).agg(F.max("batch_id")).collect()[0][0]
replay = spark.read.parquet(str(parquet)).filter(F.col("batch_id") == last_id).drop("event_date", "batch_id")
n_b1 = replay.count()
writer = app.make_batch_writer(parquet, lambda df: None)
writer(replay, last_id)                                 # replay of the last batch (same batch_id)
after_replay = spark.read.parquet(str(parquet)).count()
print(f"    replayed batch_id={last_id} with {n_b1} rows")
check("replaying a batch (same batch_id) does not add rows", after_replay, before)
writer(replay, 999)                                     # same rows under a NEW batch_id
after_new = spark.read.parquet(str(parquet)).count()
check("same rows under a new batch_id are added (it is the batch_id that makes it idempotent)", after_new, before + n_b1)
print("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED")
