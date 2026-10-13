# Work log
| Date | Who | Step | What happened | Problem → fix |
|---|---|---|---|---|
| 2026-10-13 | Benjamin | 0 | Kafka up, 6 topics, simulator running, faults observed (bad timestamps, impossible speeds, missing vehicle_id, broken JSON, ~0.8% duplicates) | spark-submit resolved to Anaconda → `conda deactivate`, re-activate venv, `rehash` |
| 2026-10-13 | Benjamin | 0 | Partition sizes uneven without hot vehicle (578 vs 1219): 40 keys hashed over 6 partitions | none (observation for A3) |
| 2026-10-13 | Benjamin | 0 | --hot-vehicle for ~2 min: partition 2 received 50% of traffic (expected 52%), 8x partition 1 | none (evidence for A3 and C3) |
