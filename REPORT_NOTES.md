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
