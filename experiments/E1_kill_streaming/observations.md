# E1 - observations

Raw outputs: `run.log`, `check_before.txt`, `check_after.txt`, `q1_batches_after_restart.txt`.

- `docker kill spark-streaming` at 12:19:33 UTC, restart at 12:20:25 (52 s down, simulator still at 20 events/s).
- At the kill, each of the 4 queries had a batch in progress (offsets written, no commit):
  dlq 1220/1219, q1 188/187, q2 489/488, q3 480/479. No `batch_id=188` folder in the Parquet yet.
- After the restart, q1 replayed batch 188 with exactly the same 140 records (its "duration" of 106 s
  includes the downtime), then batch 189 took the 2164 records that waited in Kafka (12 s), then back
  to normal 7-10 s batches.
- Check after the restart (simulator stopped first so that everything is processed):

| | before the kill (stream running) | after restart + drain |
|---|---|---|
| Parquet rows / distinct events | 29538 / 29538 | 33806 / 33806 -> 0 duplicate |
| events missing from Parquet | 603 (568 late, the rest not yet processed) | 664, **all** sent > 2 min late (watermark) |
| point alerts published / distinct | 828 / 828 | 937 / 937 -> 0 duplicate, 0 missing |
| DLQ messages / invalid inputs | 324 / 312 (read while running) | 353 / 353 -> 0 extra |

Explanation:
- Intact: no event lost (the 52 s of events waited in Kafka, Spark restarted from the offsets in
  its checkpoint) and no duplicate in the Parquet.
- Hypothesis partly wrong: we expected a few duplicated alerts/DLQ messages and got none. The kill
  happened while batch 188 had only been planned: its foreachBatch had not written anything yet
  (no Parquet folder, no alert sent), so replaying it published everything only once.
  Duplicates only appear if the kill falls between "alerts sent" and "commit written" (window of a
  few hundred ms per batch). We saw that case in our earlier test (forced replay of an already
  committed batch: same Parquet rows, but its 4 alerts published a second time). So the guarantee
  stays at-least-once for Kafka outputs, exactly-once for the Parquet.
- The 52 s downtime is shorter than the 2 min watermark: no extra event dropped as late
  (the missing events are only the simulator's 3-6 min late ones, as before).
