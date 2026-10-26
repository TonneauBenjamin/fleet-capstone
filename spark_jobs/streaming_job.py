import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR = os.environ.get("DATA_DIR", "data")
EVENTS_PATH = f"{DATA_DIR}/events"
CHECKPOINTS = f"{DATA_DIR}/checkpoints"

# only used for the performance study (not set = Spark defaults)
TRIGGER_SECONDS = os.environ.get("TRIGGER_SECONDS")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER")

EVENT_FIELDS = ["event_id", "vehicle_id", "event_time", "latitude", "longitude",
                "speed_kmh", "fuel_level_pct", "harsh_braking"]

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("event_time", StringType()),  # string here, converted after to catch bad timestamps
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("fuel_level_pct", DoubleType()),
    StructField("harsh_braking", BooleanType()),
    StructField("_corrupt_record", StringType()),  # filled by from_json when the JSON is broken
])

CATALOG_SCHEMA = StructType([
    StructField("vehicle_id", StringType()),
    StructField("plate", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("home_region", StringType()),
    StructField("max_payload_kg", IntegerType()),
])

spark = (SparkSession.builder
         .appName("fleet-streaming")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def with_trigger(writer):
    if TRIGGER_SECONDS:
        return writer.trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    return writer


def kafka_sink(df, topic, checkpoint, mode, name):
    writer = (df.writeStream
              .queryName(name)
              .format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
              .option("topic", topic)
              .option("checkpointLocation", f"{CHECKPOINTS}/{checkpoint}")
              .outputMode(mode))
    return with_trigger(writer).start()


# ---------- 1. parsing + validation ----------
reader = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
          .option("subscribe", "vehicle_events")
          .option("startingOffsets", "earliest"))
if MAX_OFFSETS_PER_TRIGGER:
    reader = reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
raw = reader.load()

parsed = (raw.select(F.col("key").cast("string").alias("key"),
                     F.col("value").cast("string").alias("raw"))
          .withColumn("data", F.from_json("raw", EVENT_SCHEMA, {"columnNameOfCorruptRecord": "_corrupt_record"}))
          .withColumn("ts", F.to_timestamp("data.event_time")))

# first matching rule gives the reason. Nulls are tested first: a comparison with NULL
# gives NULL, so "speed < 0" alone would let a NULL speed through as valid
reason = F.when(F.col("data._corrupt_record").isNotNull(), "invalid_json")
for field in EVENT_FIELDS:
    reason = reason.when(F.col(f"data.{field}").isNull(), f"missing_{field}")
reason = (reason.when(F.col("ts").isNull(), "bad_timestamp")
          .when((F.col("data.speed_kmh") < 0) | (F.col("data.speed_kmh") > 250), "speed_out_of_range"))

checked = parsed.withColumn("reason", reason)

# reason is never NULL-ambiguous: every row goes to exactly one of the two branches
invalid = checked.filter(F.col("reason").isNotNull())
valid = checked.filter(F.col("reason").isNull())

dlq = invalid.select(
    F.col("key"),
    F.to_json(F.struct("raw", "reason")).alias("value"))

# ---------- 2. deduplication ----------
events = valid.select(
    "data.event_id", "data.vehicle_id", F.col("ts").alias("event_time"),
    "data.latitude", "data.longitude", "data.speed_kmh", "data.fuel_level_pct", "data.harsh_braking")

deduped = (events
           .withWatermark("event_time", "2 minutes")
           .dropDuplicatesWithinWatermark(["event_id"]))

# ---------- 3. enrichment with the catalog ----------
catalog_raw = (spark.read
               .format("kafka")
               .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
               .option("subscribe", "fleet_catalog")
               .option("startingOffsets", "earliest")
               .load())

# a compacted topic can still contain several versions of a key: keep the last one
last = Window.partitionBy("key").orderBy(F.col("offset").desc())
catalog = (catalog_raw
           .withColumn("rn", F.row_number().over(last))
           .filter("rn = 1")
           .filter(F.col("value").isNotNull())  # tombstone = vehicle removed from the catalog
           .select(F.from_json(F.col("value").cast("string"), CATALOG_SCHEMA).alias("c"))
           .select("c.*"))

enriched = (deduped
            .join(F.broadcast(catalog), "vehicle_id", "left")
            .withColumn("region", F.coalesce("home_region", F.lit("UNKNOWN"))))


# ---------- 4. Query 1: per event ----------
def write_events_and_alerts(batch_df, batch_id):
    batch_df.persist()

    # overwrite + dynamic: a replayed batch only rewrites its own batch_id folder
    (batch_df
     .withColumn("event_date", F.to_date("event_time"))
     .withColumn("batch_id", F.lit(batch_id))
     .write
     .mode("overwrite")
     .option("partitionOverwriteMode", "dynamic")
     .partitionBy("event_date", "batch_id")
     .parquet(EVENTS_PATH))

    alerts = (batch_df
              .filter((F.col("speed_kmh") > 120) | F.col("harsh_braking"))
              .withColumn("alert_type", F.when(F.col("speed_kmh") > 120, "speeding").otherwise("harsh_braking")))
    (alerts
     .select(F.col("vehicle_id").alias("key"),
             F.to_json(F.struct("alert_type", "vehicle_id", "region", "event_time",
                                "speed_kmh", "harsh_braking", "event_id")).alias("value"))
     .write
     .format("kafka")
     .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
     .option("topic", "alerts")
     .save())

    batch_df.unpersist()


# ---------- 5. Query 2: 1-minute tumbling window per region ----------
region_kpis = (enriched
               .groupBy(F.window("event_time", "1 minute"), "region")
               .agg(F.count("*").alias("events"),
                    F.avg("speed_kmh").alias("avg_speed_kmh"),
                    F.max("speed_kmh").alias("max_speed_kmh"),
                    F.sum(F.when(F.col("speed_kmh") > 120, 1).otherwise(0)).alias("speeding_events")))

region_kpis_out = region_kpis.select(
    F.col("region").alias("key"),
    F.to_json(F.struct(F.col("window.start").alias("window_start"),
                       F.col("window.end").alias("window_end"),
                       "region", "events", "avg_speed_kmh", "max_speed_kmh", "speeding_events")).alias("value"))

# ---------- 6. Query 3: 5-minute sliding window per vehicle ----------
sustained = (enriched
             .groupBy(F.window("event_time", "5 minutes", "1 minute"), "vehicle_id", "region")
             .agg(F.count("*").alias("events"), F.avg("speed_kmh").alias("avg_speed_kmh"))
             .filter((F.col("events") >= 5) & (F.col("avg_speed_kmh") > 100)))

sustained_out = sustained.select(
    F.col("vehicle_id").alias("key"),
    F.to_json(F.struct(F.lit("sustained_speeding").alias("alert_type"), "vehicle_id", "region",
                       F.col("window.start").alias("window_start"),
                       F.col("window.end").alias("window_end"),
                       "events", "avg_speed_kmh")).alias("value"))

# ---------- start the four queries ----------
kafka_sink(dlq, "vehicle_events_dlq", "dlq", "append", "dlq")

with_trigger(enriched.writeStream
             .queryName("q1_events")
             .foreachBatch(write_events_and_alerts)
             .option("checkpointLocation", f"{CHECKPOINTS}/q1_events")).start()

kafka_sink(region_kpis_out, "region_kpis_1min", "q2_region_kpis", "update", "q2_region_kpis")
kafka_sink(sustained_out, "alerts", "q3_sustained", "update", "q3_sustained")

spark.streams.awaitAnyTermination()
