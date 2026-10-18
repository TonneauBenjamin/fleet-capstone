# Work log
| Date | Who | Step | What happened | Problem → fix |
|---|---|---|---|---|
| 2026-10-13 | Benjamin | 0 | Kafka up, 6 topics, simulator running, faults observed (bad timestamps, impossible speeds, missing vehicle_id, broken JSON, ~0.8% duplicates) | spark-submit resolved to Anaconda → `conda deactivate`, re-activate venv, `rehash` |
| 2026-10-13 | Benjamin | 0 | Partition sizes uneven without hot vehicle (578 vs 1219): 40 keys hashed over 6 partitions | none (observation for A3) |
| 2026-10-13 | Benjamin | 0 | --hot-vehicle for ~2 min: partition 2 received 50% of traffic (expected 52%), 8x partition 1 | none (evidence for A3 and C3) |
| 2026-10-13 | Benjamin | 1 | Catalog publisher (idempotent producer, 40 records, key=vehicle_id) | none |
| 2026-10-13 | Benjamin | 1 | fresh topic: 2 publications = 80 messages, no compaction; forced compaction (3rd publication): 120 offsets, 80 readable | default segment.ms=7d → lowered temporarily, then restored |
| 2026-10-13 | Benjamin | 2 | Test alert sender + alert format (docs/schemas.md) | none |
| 2026-10-13 | Benjamin | 2 | Notifier, 2 instances: cooperative 2-round rebalance; clean stop handover < 1 s; kill -9 handover 48 s, resumed at committed offset, no loss | Ctrl+C killed `tee` → BrokenPipeError inside consumer.close() → log() helper + `tee -i` |
| 2026-10-13 | Benjamin | 2 | Review: alert without alert_type crashed the notifier before commit → poison message blocking a partition | alert_type read inside the try with a default |
| 2026-10-18 | Benjamin | A3 | Keys and partition counts justified (REPORT_NOTES A3), using Step 0 measurements | none |
