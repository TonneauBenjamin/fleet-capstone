# Report notes (raw material for the final report)

## A1 - Catalog and compaction
- Catalog: 40 vehicles, fixed seed, 8 per region (rotation), key = vehicle_id.
- Producer: enable.idempotence=True (acks=all; broker refuses an already-written batch sequence number,
  so a retry after a lost ack cannot duplicate). Scope = one producer session.
- Guaranteed: latest value per key kept; order within a partition kept; offsets never change.
- Not guaranteed: WHEN old values are removed; that a reader sees one value per key; active segment never compacted.
- Defaults: segment.ms=7 days, segment.bytes=1 GB, min.cleanable.dirty.ratio=0.5, delete.retention.ms=24 h.
- Evidence: 3 publications -> 120 readable messages, nothing removed. Forced compaction
  (segment.ms=10000, min.cleanable.dirty.ratio=0.01, 4th publication): 160 offsets, 80 readable. Restored.
- Consequence for Spark (B3): keep only the highest offset per vehicle_id.
