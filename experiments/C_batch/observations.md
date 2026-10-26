# Part C - batch jobs on the laptop (i7-13700H, 20 threads)

## Quality and daily report (run by `fleet_daily_pipeline` for 2026-10-13)
- Verdict (`data/quality/2026-10-13.json`): 980701 rows, 6134 duplicated event_ids (0.63 %),
  0.01 % UNKNOWN -> passed. The 6134 "duplicates" are different events that got the same id
  because the simulator was restarted several times that day (seq starts again at 1); Spark's
  dedup removed every real duplicate (0 in `final_check.txt`). It stays under our 1 % threshold.
- The run took 8 min (quality 2 min, report 5 min) on ~3300 small Parquet files (562 `batch_id`
  folders, up to 6 files each). See "limits" below.

## Partition pruning (`report_2026-10-11_pruning.log`, `report_partition_pruning_explain.txt`)
Report job run for 2026-10-11 (a test day with one file) while 2026-10-13 holds ~980k rows:
- physical plan: `PartitionFilters: [isnotnull(event_date#13), (event_date#13 = 2026-10-11)]`
- Spark log: `Selected 1 partitions out of 589, pruned 99.83% partitions.`
- `FileScanRDD: Reading File path` appears only for that one file (3 times: the job runs 3 actions).
- But `It took 24929 ms to list leaf files`: before pruning, Spark still lists every folder of
  `data/events` to discover the partitions. Pruning avoids reading the data, not listing it.

## Skew (`skew_study.log`, `skew_stages.txt`, `skew_study_x10*.log`, `skew_stages_x10*.txt`, screenshots `C_skew_*`)
Data: 15 min at 1000 events/s with `--hot-vehicle` -> 964528 rows for the day, V0001 = 355794 (37 %).

Aggregation stage, 8 tasks (task duration and shuffle-read records, min / median / max):

| variant | 1x data (965k rows) | 10x data (9.7M rows, `--copies 10`) |
|---|---|---|
| report (partial aggregation) | 34/48/53 ms, 251/419/754 rows | 34/45/52 ms, 273/455/822 rows |
| naive (repartition by vehicle) | 271/279/394 ms, 47k/78k/**402k** rows | 407/599/**1879** ms, 472k/790k/**4.03M** rows |
| naive + AQE | 4 tasks, 88/171/206 ms, hot task still 402k | 7 tasks, 458/671/**1824** ms, hot task still 4.03M |
| salted (8 salts, 2 stages) | 427/542/558 ms, 60k/155k/175k rows | 800/1206/**1325** ms, 613k/1.56M/1.76M rows |

Stage durations at 10x (run 1 / run 2): naive 1888 / 1977 ms, naive+AQE 1836 / 1764 ms,
salted 1338 / 1432 ms (+ 52 / 69 ms for the small 2nd stage).
Whole query: report 0.95 / 1.01 s, naive 2.99 / 3.30 s, naive+AQE 2.59 / 2.40 s, salted 3.50 / 3.25 s.

- The skew is clear in the naive version: the task holding V0001 reads 5x the median rows.
- Salting balances it (max/median 1.1 instead of 5) and, at 10x, the aggregation stage is ~28 %
  faster (hot task 1.9 s -> 1.3 s).
- AQE alone does not fix it: it merges small partitions (8 -> 4 or 7 tasks) but never splits the
  hot one (still 402k / 4.03M rows). Its skew handling only applies to joins and `rebalance()`.
- With 1x data the gain is invisible: the hot task lasts 0.4 s, per-task overhead dominates and
  the salted version (extra shuffle, extra column, second aggregation) is slower overall.
  At 10x the whole query is still not faster with salting (the extra map work eats the gain of the
  aggregation stage). Salting pays off when the hot task dominates the job; on our real volume
  (~1.7M rows per day at 20 events/s) the skew does not hurt, and the report itself is protected
  by the partial aggregation.
- `--copies 10` repeats each row 10 times in memory: we did not have 10 h of 1000 events/s data,
  it is only to see how the stage behaves with more rows per key.
