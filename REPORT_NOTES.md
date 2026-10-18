# Report notes (raw material for the final report)

## A1 - Catalog and compaction
- Catalog: 40 vehicles, fixed seed, 8 per region (rotation), key = vehicle_id.
- Producer: enable.idempotence=True (acks=all; broker refuses an already-written batch sequence number,
  so a retry after a lost ack cannot duplicate). Scope = one producer session.
- Guaranteed: latest value per key kept; order within a partition kept; offsets never change.
- Not guaranteed: WHEN old values are removed; that a reader sees one value per key; active segment never compacted.
- Defaults: segment.ms=7 days, segment.bytes=1 GB, min.cleanable.dirty.ratio=0.5, delete.retention.ms=24 h.
- Evidence: fresh topic, 2 publications -> 80 readable messages, nothing removed. Forced compaction
  (segment.ms=10000, min.cleanable.dirty.ratio=0.01, 3rd publication): 120 offsets, 80 readable. Restored.
- Consequence for Spark (B3): keep only the highest offset per vehicle_id.

## A2 - Alerts notifier (consumer group alerts-notifier)
- Config: enable.auto.commit=False, synchronous commit AFTER handling (at-least-once),
  partition.assignment.strategy=cooperative-sticky, auto.offset.reset=earliest.
- Dedup on alert_id (bounded memory: 10,000 ids). Works because duplicates share the key -> same partition -> same instance.
- Two instances: A owns [1,2], B owns [0]. Rebalance in TWO rounds (cooperative): round 1 A revokes only [0],
  round 2 B is assigned [0]. A never stopped reading 1 and 2. B start -> B owns a partition: 6 s.
- Clean stop (Ctrl+C -> consumer.close() = LeaveGroup): B revoked/closed at 08:39:01, A assigned [0] at 08:39:01 -> < 1 s.
- Hard kill (kill -9 at 08:33:25, close() never runs): partition 0 stayed assigned to the dead member,
  lag 12 then 20; A assigned [0] at 08:34:13 -> 48 s = session.timeout.ms (45 s) + rebalance.
  A resumed exactly at the committed offset 20, handled o20-o41 (22 alerts) in 1 s. No loss, no re-print
  (B had committed o19 before dying). Duplicates still skipped during the catch-up.
- Bookmark belongs to the group: with no member, offsets 118/94/111 kept; B restarted alone and resumed exactly there.
- Limits: dedup memory lost on restart and not shared between instances (a rebalance can let a duplicate through);
  re-print possible if a crash happens between print and commit; one synchronous commit per alert costs a
  round-trip each (fine at 2 alerts/s, would limit throughput at high rates).
- Trade-off: lower session.timeout.ms = faster failover but false "dead" verdicts on slow consumers -> rebalance storms.
- Bug found and fixed: Ctrl+C also killed `tee`, so print() raised BrokenPipeError inside consumer.close()
  (on_revoke prints) -> clean leave not guaranteed. Fix: log() ignores a closed output + `tee -i`.
- Poison message: an alert with valid JSON but no alert_type used to crash the notifier BEFORE the commit,
  so every restart re-read it and crashed again (one bad message blocks its partition forever).
  Fix: alert_type read inside the try with a default; unreadable alerts are logged, committed and skipped.

## A3 - Keys and partition counts (counts fixed by the provided create_topics.py; justified here)
Rule used: partitions >= max(throughput needed / throughput of one reader, number of parallel readers wanted),
and never more than the number of distinct keys. Can be increased later but never decreased, and increasing
changes hash(key) mod N, so a key moves to another partition and per-key order breaks -> chosen up front.

| Topic | Partitions | Key | Justification |
|---|---|---|---|
| vehicle_events | 6 | vehicle_id | Highest volume (20 ev/s, x10 in E4). 6 = up to 6 parallel Spark read tasks; divisible by 2 and 3 (even split for 2 or 3 readers). Key keeps each vehicle's events ordered in one partition. |
| fleet_catalog | 3 | vehicle_id | Compaction needs a key (latest card per vehicle). 40 records, read once: 1 would suffice, 3 is harmless. |
| alerts | 3 | vehicle_id | Up to 3 notifier instances (A2 needs 2). Same key = an alert and its duplicate land in the same partition -> same instance -> in-memory dedup works. |
| region_kpis_1min | 3 | region | Successive updates of a region's window stay ordered. Limit: 6 keys (5 regions + UNKNOWN) on 3 partitions -> uneven; more than 6 partitions would be useless. |
| vehicle_events_dlq | 3 | original key | ~1% of traffic; key lets us trace a rejected message back to its vehicle. 1 would suffice. |
| daily_reports | 1 | date | One message per day; a single partition gives total order of the reports. |

- "3" for small topics is also the usual production convention (3 brokers -> one partition leader per broker).
  Honest limit: our setup has ONE broker, so this argument does not apply locally.
- Measured cost of keying by vehicle (Step 0): uneven partitions even without hot vehicle (578 vs 1219 messages:
  40 keys hashed over 6 partitions = 4/5/7/8/8/8 vehicles); with --hot-vehicle partition 2 got 50.1% of traffic
  (expected 0.40 + 0.60 x 8/40 = 52%), 8x partition 1.
- Random key on vehicle_events: per-vehicle order lost (a vehicle's events spread over 6 partitions) for any consumer
  that processes a vehicle sequentially. Spark event-time windows are NOT affected (they use event_time + watermark,
  not arrival order). Benefit: the hot-vehicle skew disappears. Same mistake on `alerts` would break our dedup
  (an alert and its duplicate could reach different notifier instances); on fleet_catalog, compaction is impossible.
- One partition: one Spark read task and one active consumer per group (extra instances stay idle as standbys),
  throughput limited to one partition on one broker; gain = total order across all vehicles.

## Provided material v2 (starter kit, updated 2026-10-18)
- The teacher's updated starter kit replaced the first version of the provided files. Changes that matter:
  event_id now contains a run id (no false duplicates when the simulator restarts -- a problem we had
  noticed in v1: seq restarted at 1, ~1/40 of reused ids collided); event_time always has millisecond
  precision (pattern yyyy-MM-dd'T'HH:mm:ss.SSSXXX); anomalies = half speed > 120, half harsh braking;
  sustained-speeding episodes (101-118 km/h for 6-9 min) are injected, so Query 3 can fire (with v1 a
  vehicle's 5-min mean stayed ~54 km/h and could never exceed 100); missing field can be vehicle_id,
  event_time or speed_kmh; --unknown-vehicles sends V0041-V0045 (not in the catalog -> UNKNOWN).
- docker-compose v2: KAFKA_LOG_DIRS=/var/lib/kafka/data. In v1 the broker wrote to /tmp inside the
  container and the kafka-data volume was unused: data survived `docker compose stop/start` but not
  `docker compose down`. Also KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 (default 3000 ms): part of the
  6 s we measured for B to join the group in A2 was this initial delay; to re-measure with v2.
- Steps 0-2 measurements were taken with v1; their conclusions (keys, compaction, rebalancing) are unchanged.

## A4 - Delivery semantics per hop
| Hop | Semantics | Why |
|---|---|---|
| producer -> broker | no duplicate from retries (exactly-once per write, per producer session) | enable.idempotence=true: PID + per-partition sequence numbers, acks=all. The simulator's INJECTED duplicates are two separate writes, so both are stored -> handled by Spark (B2). Messages still in a producer's local buffer are lost if the process is killed (-9); v2 simulator flushes on Ctrl+C/SIGTERM. |
| broker -> Spark | no loss, no double processing of state | Spark writes the offsets of batch N to checkpoint/offsets BEFORE running it and to commits/ after; it does not use Kafka consumer-group commits. After a crash the batch is re-read from the same offsets (Kafka still has them). |
| Spark -> Parquet | effectively exactly-once | dynamic partition overwrite of event_date=*/batch_id=<id>: a replay replaces its own folders (tested). |
| Spark -> alerts / DLQ (Kafka sink) | at-least-once | Kafka only appends: a replayed batch publishes its messages again. alert_id is deterministic (<event_id>:<type>), so copies are recognisable. |
| alerts -> notifier | at-least-once | commit after handling; a crash between print and commit re-delivers; dedup on alert_id (per instance, in memory). |
End to end for alerts: at-least-once, duplicates mostly filtered by the notifier.

## B1-B4 - Spark streaming application (spark_jobs/streaming_app.py)
Tested without Kafka (tests/test_spark_logic_local.py, output raw_outputs/b_local_test.txt): messages from the
real v2 simulator functions streamed through the SAME functions as the real job. Real run on Kafka: see raw_outputs.
- B1: explicit StructType; event_time read as string then to_timestamp with yyyy-MM-dd'T'HH:mm:ss.SSSXXX.
  One `reason` column (checks in order: broken_json, missing_field:<name>, bad_timestamp, speed_out_of_range);
  routing on reason IS NULL / IS NOT NULL (never NULL itself) -> no message can fall between the two outputs.
  DLQ value = JSON {reason, raw_value, source_partition, source_offset, source_timestamp, detected_at}, key = original key.
  Test: 2111 messages in = DLQ + valid exactly; DLQ reasons = injected faults (17 bad_timestamp, 9 broken_json,
  10 speed_out_of_range, 9 missing_field).
- B2: withWatermark(event_time, 2 min).dropDuplicatesWithinWatermark(event_id). State bounded: an id is evicted once
  the watermark (max event_time seen - 2 min) has passed it. A duplicate arriving after that carries the same old
  event_time, so it is older than the watermark and is dropped as late data (not written, not counted as a duplicate).
  Side effect: the simulator's late events (3-6 min) are dropped too -> no Parquet row, no alert for them.
  Test: 49/49 late events of batch 2 dropped; the 30 late events of batch 1 were KEPT, because the first micro-batch
  has no watermark yet (it is computed at the end of each batch). Spark metric numRowsDroppedByWatermark said 98 for
  those 49 rows -> we count late rows by comparing sent vs stored, not with this metric.
  Spark also runs empty "no-data" batches to advance the watermark and evict state -> gaps in batch_id folders.
- B3: catalog read ONCE at start (batch Kafka read earliest->latest), latest record per vehicle_id = max_by(value,
  offset) (compaction does not guarantee old versions are gone, A1); tombstone as latest = vehicle removed.
  Cached + broadcast LEFT join; region = coalesce(home_region, 'UNKNOWN').
  Test: V0002 updated Lyon->Toulouse -> Toulouse; V0003 tombstoned -> UNKNOWN; V0041-V0045 -> UNKNOWN.
  Limitation: catalog changes published while the job runs are invisible until restart (we chose a cached snapshot;
  re-reading every micro-batch would cost a Kafka read per batch).
- B4: foreachBatch: persist (used twice) -> Parquet partitioned by event_date (UTC) and batch_id with mode overwrite +
  spark.sql.sources.partitionOverwriteMode=dynamic -> point alerts (speed > 120 -> speeding, harsh_braking ->
  harsh_braking; both if both) to `alerts`, key = vehicle_id. Trigger 10 s.
  Test: replaying the last batch (same batch_id, 478 rows) left the row count unchanged (1946); the same rows under a
  new batch_id were added (2424) -> it is the batch_id that makes the write idempotent. 53 dangerous events -> 53 alerts,
  unique alert_ids. Alerts are NOT idempotent (Kafka append): replay = alerts published twice, same alert_id.
- Session config: spark.sql.session.timeZone=UTC (event_date = UTC day, like Airflow), shuffle.partitions=6.
