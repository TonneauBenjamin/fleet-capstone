# Performance study - results

Raw outputs: `run.log`, `<run>_batches.txt` (q1 batches from the checkpoint), `<run>_latency.txt`.
Backlog at the start of each run: ~1.08M events in `vehicle_events` (the whole history, read with
startingOffsets earliest), `maxOffsetsPerTrigger` = 100000 for all runs, simulator at 20 events/s.

| run | shuffle partitions | trigger | catch-up throughput (q1) | q1 batch in steady state | alert latency median / p95 |
|---|---|---|---|---|---|
| p6_default | 6 | default | **7503 records/s** (1.08M in 145 s of batches) | 6.7 - 8.1 s | 12.4 s / 17.0 s |
| p6_10s | 6 | 10 s | **8585 records/s** (1.09M in 127 s) | 5.6 - 6.8 s | **10.7 s / 15.0 s** |
| p200_default | 200 | default | **229 records/s** (3 batches of 100k in 22 min, ~7 min each) | never reached | not measurable: backlog never drained |
| p200_10s | 200 | 10 s | cancelled | | |

How the numbers are computed: throughput = records of the catch-up batches / sum of their durations
(the first batch, 25 s, includes the start of the job). Latency = Kafka write time of the point
alert - event_time, over ~3 min after the catch-up. For p200_default the script stopped waiting
after its 25 min limit (the "caught up" line in `run.log` at 21:43:36 is that timeout); no fresh
event got an alert in the following window, so the latency is not defined (it grows without limit).
We cancelled p200_10s: with 200 partitions the job cannot even drain the backlog, a 10 s trigger
would not change that, and the run would have taken another 30 min.

## Explanation
- **200 shuffle partitions is 33x slower** than 6 here. It was expected to be worse, not by that
  much. In p200 the queries q2 and q3 (aggregations written to Kafka, with 200 state stores each)
  drained the same backlog in ~4 min; only q1 was stuck. The difference: q1 writes Parquet with
  `partitionBy(event_date, batch_id)` after a 200-partition shuffle, so each batch writes 200
  Parquet files per date (+ 200 `.crc` checksum files, counted in `data/perf/p200_default`)
  through the Windows <-> Docker bind mount, plus a staging folder and a rename per file (dynamic
  partition overwrite). One small file costs about a second on this mount: ~400 files = the ~7 min
  we see. With 6 partitions it is 6 Parquet files (12 with the checksums) per batch.
  So `spark.sql.shuffle.partitions` is not only "how many tasks": in a foreachBatch that writes
  files, it is also the number of files per batch (small files problem, Ch.2 HDFS block logic).
- **10 s trigger was better than the default**, the opposite of our hypothesis. With the default
  trigger the 4 queries start a new batch as soon as the previous one ends, so they are always
  competing for the same local executor; with 10 s they leave idle time and each q1 batch is
  shorter (~6 s instead of ~7.5 s). The average wait for the next trigger (~5 s) is about the same
  as waiting for the end of the current back-to-back batch (~half of 7.5 s), so the latency ends up
  slightly lower. The catch-up is a bit faster for the same reason. Differences of ~1.5 s measured
  on one 3-minute window each: same order of magnitude, the conclusion "10 s is not worse" is
  solid, "10 s is clearly better" is not.
- In every config, the alert latency (~11-12 s) is dominated by the fixed cost of a micro-batch
  (see E4), not by the number of events.

## Recommended configuration
`spark.sql.shuffle.partitions = 6` (= number of Kafka partitions; our default) with a
**10 s trigger**:
- same or better alert latency than the default trigger on this machine;
- fewer batches -> fewer `batch_id` folders and fewer small Parquet files (-> faster daily batch
  jobs, which spend most of their time listing/opening these files);
- predictable load: one batch every 10 s instead of continuous competition between the 4 queries.
Never the Spark default of 200 for this job: it multiplies the number of state stores and of
output files per batch for a few hundred rows.
`maxOffsetsPerTrigger` (100000 here) is worth keeping in production to bound the size of a
catch-up batch after a crash (E1, E3).
