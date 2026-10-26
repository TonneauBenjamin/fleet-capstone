# E4 - observations

Raw outputs: `run.log`, `q1_batches_baseline_20eps.txt`, `q1_batches_burst.txt`,
`latency_baseline_20eps.txt`, `latency_burst_200eps.txt`, screenshots
`screenshots/E4_q1_statistics_during_burst.png`, `screenshots/E4_q2_statistics_during_burst.png`,
and the 1000 events/s run used for the skew data (`screenshots/C_q1_statistics_at_1000eps.png`).

| load | q1 records per batch | q1 batch duration | process rate vs input rate | alert latency median / p95 |
|---|---|---|---|---|
| 20 events/s (baseline) | 140 - 220 | 7.4 - 9.7 s | equal (no backlog) | 13.7 s / 18.7 s |
| 200 events/s (burst, 5 min) | 1400 - 1620 | 6.8 - 7.8 s | equal (~200/s) | 11.2 s / 15.0 s |
| 1000 events/s (15 min, skew data) | ~8000 | 6.7 - 7.7 s | equal (~1100/s) | not measured |

- 10x more events did not change the batch duration: a batch takes ~7 s whether it holds 150 or
  8000 records. The latency did not get worse (the difference 13.7 -> 11.2 s is noise between two
  3-minute windows).
- Kafka and the producer: no error, no lag. The notifiers printed ~10x more alerts without lag.
- In the Spark UI (operation duration), almost all the time of a batch is in `addBatch` (running
  the batch itself), not in planning or offset commits; and it does not depend on the number of
  rows: it is a fixed cost per batch (4 queries sharing one local driver, the catalog read from
  Kafka again at every batch, the dedup/aggregation state stores, the Parquet write and the Kafka
  write in q1).

Explanation and bottleneck:
- Hypothesis partly right: Kafka is not the bottleneck and the job keeps up. Wrong on "batches get
  longer": at these rates the volume is negligible next to the fixed cost of a micro-batch.
- The bottleneck is this fixed per-batch cost of the Spark job: it sets the latency floor
  (an event waits on average for the end of the current batch plus a full batch, ~1.5 x 7 s, which
  is what we measure) and it caps the reachable latency, not the throughput.
- What we would change first: reduce the fixed cost of a batch, not add partitions. In order:
  read the catalog once and refresh it every few minutes instead of at every batch, run q1 (the
  alerts) in its own application so it does not share the cores with q2/q3, and keep
  `spark.sql.shuffle.partitions` low (see the performance study). More Kafka partitions or
  consumers would not help: reading is not the slow part.
