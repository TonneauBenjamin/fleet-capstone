"""
Part B -- Spark Structured Streaming application: ONE application, several streaming queries.

What it does today (Steps 3-6 = B1 to B4):
  B1  read `vehicle_events` from Kafka, parse with an explicit schema, validate every message;
      invalid ones go to `vehicle_events_dlq` with a reason            -> query "dlq"
  B2  drop duplicated event_ids within a 2-minute watermark
  B3  add each vehicle's region from the catalog (`fleet_catalog`); unknown vehicles get "UNKNOWN"
  B4  Query 1 (foreachBatch): write each micro-batch as Parquet partitioned by event_date and
      batch_id (idempotent on replay) and publish point alerts to `alerts` -> query "query1"
Still to come (after 21 October): Query 2 (regional KPIs) and Query 3 (sustained speeding).

Every 30 s the application prints one progress line per query (rows read, rows/s, state size,
late rows dropped by the watermark): that is the evidence we keep in raw_outputs/.

Run from the project root (Kafka running, topics created, catalog published):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_jobs/streaming_app.py
Options: --debug (adds a console table: number of messages per verdict), --trigger SECONDS
"""
import argparse
import time
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType

BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPIC = "vehicle_events"
DLQ_TOPIC = "vehicle_events_dlq"
CATALOG_TOPIC = "fleet_catalog"
ALERTS_TOPIC = "alerts"

PROJECT_DIR = Path(__file__).resolve().parent.parent
CHECKPOINT_DIR = PROJECT_DIR / "checkpoints"
EVENTS_PARQUET_DIR = PROJECT_DIR / "data" / "events"     # hand-off to the daily batch jobs (Part C)

# ---------------------------------------------------------------------------------------------
# Message formats
# ---------------------------------------------------------------------------------------------
# Explicit schema: each field is read with the type we expect. event_time is read as TEXT and
# converted afterwards with the exact pattern of the provided README, so an unparsable date
# becomes NULL and is reported as "bad_timestamp" instead of silently disappearing.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("event_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("fuel_level_pct", DoubleType()),
    StructField("harsh_braking", BooleanType()),
])
FIELDS = [f.name for f in EVENT_SCHEMA.fields]
EVENT_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"      # e.g. 2026-10-13T09:16:00.123+00:00
MIN_SPEED, MAX_SPEED = 0.0, 250.0

CATALOG_SCHEMA = StructType([
    StructField("vehicle_id", StringType()),
    StructField("plate", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("home_region", StringType()),
    StructField("max_payload_kg", IntegerType()),
])

WATERMARK = "2 minutes"          # B2: how late an event may arrive and still be deduplicated
SPEEDING_KMH = 120.0             # B4: point alert when speed is above this, or on harsh braking


# ---------------------------------------------------------------------------------------------
# Spark session and Kafka source
# ---------------------------------------------------------------------------------------------
def build_spark():
    spark = (SparkSession.builder
             .appName("fleet-streaming")
             .config("spark.sql.session.timeZone", "UTC")                    # event_date = UTC day, like Airflow
             .config("spark.sql.shuffle.partitions", "6")                    # default 200 is absurd for 20 ev/s
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # overwrite only the partitions written
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark, starting_offsets):
    """Kafka source. Spark keeps the offsets it has read in each query's checkpoint, not in Kafka."""
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("subscribe", SOURCE_TOPIC)
            .option("startingOffsets", starting_offsets)    # only used the FIRST time a query starts
            .load())


# ---------------------------------------------------------------------------------------------
# B1 -- validation and dead-letter queue
# ---------------------------------------------------------------------------------------------
def validate(raw):
    """One row per Kafka message. Column `reason` is NULL when the message is valid, otherwise it
    says why it is invalid. Every message gets exactly one verdict."""
    df = (raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"))
          .withColumn("j", F.from_json("raw_value", EVENT_SCHEMA))
          .withColumn("event_ts", F.to_timestamp("j.event_time", EVENT_TIME_PATTERN)))

    # Broken JSON: Spark returns either a NULL struct or a struct whose fields are all NULL.
    all_fields_null = reduce(lambda a, b: a & b, [F.col(f"j.{n}").isNull() for n in FIELDS])

    # Checks in a fixed order; the FIRST failing check gives the reason.
    reason = F.when(F.col("j").isNull() | all_fields_null, F.lit("broken_json"))
    for name in FIELDS:
        reason = reason.when(F.col(f"j.{name}").isNull(), F.lit(f"missing_field:{name}"))
    reason = (reason
              .when(F.col("event_ts").isNull(), F.lit("bad_timestamp"))
              .when((F.col("j.speed_kmh") < MIN_SPEED) | (F.col("j.speed_kmh") > MAX_SPEED),
                    F.lit("speed_out_of_range")))
    # No .otherwise(): a message that passes every check keeps reason = NULL, i.e. VALID.
    # Routing uses `reason IS NULL` / `reason IS NOT NULL`, which are never NULL themselves, so a
    # message cannot fall between the two outputs (the NULL trap of the subject).
    return df.withColumn("reason", reason)


def dlq_messages(validated):
    """Invalid messages for the DLQ: original key + JSON value with the reason and the raw text."""
    payload = F.to_json(F.struct(
        F.col("reason"),
        F.col("raw_value"),
        F.col("kafka_partition").alias("source_partition"),
        F.col("kafka_offset").alias("source_offset"),
        F.col("kafka_timestamp").alias("source_timestamp"),
        F.current_timestamp().alias("detected_at")))
    return (validated.filter(F.col("reason").isNotNull())
            .select(F.col("kafka_key").alias("key"), payload.alias("value")))


def valid_events(validated):
    """Valid events with typed columns; event_time becomes a real timestamp."""
    return (validated.filter(F.col("reason").isNull())
            .select("j.*", "event_ts", "kafka_partition", "kafka_offset")
            .drop("event_time")
            .withColumnRenamed("event_ts", "event_time"))


# ---------------------------------------------------------------------------------------------
# B2 -- deduplication
# ---------------------------------------------------------------------------------------------
def deduplicate(events):
    """Keep the first copy of each event_id. The watermark (latest event_time seen - 2 minutes)
    bounds the state: an event_id is forgotten once the watermark has passed its event_time.
    Rows older than the watermark are dropped too (counted as numRowsDroppedByWatermark)."""
    return (events
            .withWatermark("event_time", WATERMARK)
            .dropDuplicatesWithinWatermark(["event_id"]))


# ---------------------------------------------------------------------------------------------
# B3 -- enrichment with the catalog
# ---------------------------------------------------------------------------------------------
def latest_catalog(catalog_messages):
    """Kafka records of fleet_catalog -> one row per vehicle, the record with the highest offset.
    Needed because compaction does not guarantee that old versions are already gone (Part A1).
    A tombstone (empty value) as latest record means the vehicle was removed from the catalog."""
    latest = (catalog_messages
              .filter(F.col("key").isNotNull())
              .groupBy(F.col("key").cast("string").alias("vehicle_id"))
              .agg(F.max_by(F.col("value").cast("string"), F.col("offset")).alias("value")))
    return (latest
            .filter(F.col("value").isNotNull())
            .select(F.from_json("value", CATALOG_SCHEMA).alias("c"))
            .select("c.vehicle_id", "c.home_region", "c.vehicle_type"))


def load_catalog(spark):
    """Read the whole catalog topic ONCE (batch read, earliest -> latest) and keep it in memory.
    Limitation: changes published while the job runs are not seen until the job restarts."""
    messages = (spark.read.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", CATALOG_TOPIC)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load())
    catalog = latest_catalog(messages).cache()
    print(f"catalog loaded: {catalog.count()} vehicles")
    return catalog


def enrich(events, catalog):
    """Stream-static LEFT join with a broadcast catalog: unknown vehicles are kept, region = UNKNOWN."""
    return (events.join(F.broadcast(catalog), on="vehicle_id", how="left")
            .withColumn("region", F.coalesce(F.col("home_region"), F.lit("UNKNOWN")))
            .drop("home_region"))


# ---------------------------------------------------------------------------------------------
# B4 -- Query 1: Parquet hand-off + point alerts (foreachBatch)
# ---------------------------------------------------------------------------------------------
def point_alerts(batch_df):
    """One alert per (event, type): speeding (speed > 120) and/or harsh_braking.
    alert_id is deterministic (<event_id>:<type>), so a replayed alert has the SAME id and the
    notifier can skip it. Key = vehicle_id (same partition as the vehicle's other alerts)."""
    def alerts_of(alert_type, condition):
        return (batch_df.filter(condition)
                .select(
                    F.col("vehicle_id").alias("key"),
                    F.to_json(F.struct(
                        F.concat_ws(":", F.col("event_id"), F.lit(alert_type)).alias("alert_id"),
                        F.lit(alert_type).alias("alert_type"),
                        F.col("vehicle_id"),
                        F.col("event_time"),
                        F.current_timestamp().alias("detected_at"),
                        F.struct(F.col("speed_kmh"), F.col("harsh_braking"), F.col("region"),
                                 F.col("event_id")).alias("details"),
                    )).alias("value")))
    return (alerts_of("speeding", F.col("speed_kmh") > SPEEDING_KMH)
            .unionByName(alerts_of("harsh_braking", F.col("harsh_braking"))))


def kafka_alert_sink(alerts_df):
    (alerts_df.write.format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
     .option("topic", ALERTS_TOPIC)
     .save())


def make_batch_writer(parquet_dir, alert_sink):
    """Returns the foreachBatch function of Query 1.

    Idempotence: after a crash Spark replays a batch with the SAME batch_id and the SAME offsets
    (they were written to the checkpoint before the batch ran). The Parquet write overwrites
    exactly the folders event_date=*/batch_id=<id> of that batch (dynamic partition overwrite),
    so a replay replaces the files instead of adding a second copy.
    The Kafka write of the alerts is NOT idempotent: Kafka only appends, so a replayed batch
    publishes its alerts a second time (same alert_id -> the notifier skips them)."""
    def write_batch(batch_df, batch_id):
        batch_df.persist()                      # used twice: Parquet + alerts
        try:
            (batch_df
             .withColumn("event_date", F.to_date("event_time"))
             .withColumn("batch_id", F.lit(batch_id))
             .write.mode("overwrite")
             .partitionBy("event_date", "batch_id")
             .parquet(str(parquet_dir)))
            alert_sink(point_alerts(batch_df))
        finally:
            batch_df.unpersist()
    return write_batch


# ---------------------------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------------------------
def start_dlq_query(validated):
    return (dlq_messages(validated).writeStream
            .queryName("dlq")
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("topic", DLQ_TOPIC)
            .option("checkpointLocation", str(CHECKPOINT_DIR / "dlq"))
            .start())


def start_query1(enriched, trigger_seconds):
    return (enriched.writeStream
            .queryName("query1")
            .outputMode("append")
            .foreachBatch(make_batch_writer(EVENTS_PARQUET_DIR, kafka_alert_sink))
            .option("checkpointLocation", str(CHECKPOINT_DIR / "query1"))
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start())


def start_debug_stats(validated):
    """Console table: number of messages per verdict (VALID or the DLQ reason) since the start."""
    return (validated
            .groupBy(F.coalesce(F.col("reason"), F.lit("VALID")).alias("verdict"))
            .count()
            .writeStream.queryName("debug_stats")
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .trigger(processingTime="30 seconds")
            .start())


def print_progress(queries):
    """One line per query from its last progress report (what the Spark UI shows)."""
    stamp = time.strftime("%H:%M:%S", time.gmtime())
    for q in queries:
        p = q.lastProgress
        if not p:
            print(f"[{stamp} UTC] {q.name}: waiting for the first batch")
            continue
        line = (f"[{stamp} UTC] {q.name}: batch={p['batchId']} rows={p['numInputRows']} "
                f"in={p.get('inputRowsPerSecond', 0):.1f}/s done={p.get('processedRowsPerSecond', 0):.1f}/s "
                f"duration_ms={p['durationMs'].get('triggerExecution', 0)}")
        for op in p.get("stateOperators", []):
            line += (f" | state_rows={op.get('numRowsTotal')} "
                     f"late_dropped={op.get('numRowsDroppedByWatermark', 0)}")
        print(line, flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                        help="where a query starts the first time; afterwards its checkpoint decides")
    parser.add_argument("--trigger", type=int, default=10, help="Query 1 micro-batch interval (seconds)")
    parser.add_argument("--debug", action="store_true", help="also print a table of messages per verdict")
    args = parser.parse_args()

    spark = build_spark()
    catalog = load_catalog(spark)
    validated = validate(read_events(spark, args.starting_offsets))
    enriched = enrich(deduplicate(valid_events(validated)), catalog)

    queries = [start_dlq_query(validated), start_query1(enriched, args.trigger)]
    if args.debug:
        queries.append(start_debug_stats(validated))
    print(f"started queries: {[q.name for q in queries]} -- Spark UI: http://localhost:4040", flush=True)

    while all(q.isActive for q in queries):
        time.sleep(30)
        print_progress(queries)
    for q in queries:                   # one query stopped: show why, then stop the others
        if q.exception():
            print(f"query {q.name} failed: {q.exception()}")
    for q in queries:
        q.stop()


if __name__ == "__main__":
    main()
