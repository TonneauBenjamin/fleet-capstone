# Performance study - plan and hypothesis (written before the runs)

Parameters (2 x 2 = 4 runs):
- `spark.sql.shuffle.partitions`: 6 (= Kafka partitions, our default) vs 200 (Spark default)
- trigger interval: default (next batch as soon as the previous one ends) vs 10 seconds

Fixed for all runs: `maxOffsetsPerTrigger` = 100000. The backlog is ~1M events after the other
experiments; without a limit the first batch would take all of it at once (memory risk with the
default 1 GB driver, and a single batch says nothing about the steady rate).

Each run (`run_perf.ps1`): the main streaming job is stopped, a copy of the job starts with its
own empty checkpoints (DATA_DIR=data/perf/<run>), so it first has to drain the whole
`vehicle_events` backlog (startingOffsets earliest) while the simulator keeps producing at
20 events/s.
- Throughput = records processed per second during this catch-up (q1 checkpoint, batches of
  100000 records).
- Then ~3 min of steady state at 20 events/s: end-to-end latency (event_time -> alert written in
  Kafka) for the point alerts, median and p95 (`latency.py`).
- Side effect, on purpose: each run republishes the alerts, KPIs and DLQ messages of the whole
  history (new checkpoints = new queries). The global check of E1-E4 is done before.

Hypothesis:
- 200 shuffle partitions: every stateful operator runs 200 tasks and 200 state stores per batch,
  for a few hundred rows. Much more fixed cost per batch -> longer batches, worse latency; on the
  big catch-up batches the difference is smaller.
- 10 s trigger: adds on average ~5 s of waiting before an event is processed -> higher latency.
  During the catch-up it should not matter much, as long as a 100000-record batch takes more
  than 10 s (the next batch starts right away when the previous one is late).
- Expected best for alerts: 6 partitions + default trigger.
