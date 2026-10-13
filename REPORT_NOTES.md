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
