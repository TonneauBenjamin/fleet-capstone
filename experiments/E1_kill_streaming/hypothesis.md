# E1 - kill -9 of the streaming job, then restart (hypothesis written before the run)

Procedure: platform running at 20 events/s, `docker kill spark-streaming` (SIGKILL, no clean shutdown),
wait ~1 min (the simulator keeps producing), `docker start spark-streaming`, let it catch up,
then compare Kafka with the outputs (`experiments/scripts/check_pipeline.py`, run before and after).

Hypothesis:
- Spark restarts from the checkpoints. Committed batches are not run again; the batch that was
  running (offsets/N written, commits/N missing) is replayed with exactly the same offsets.
- Parquet: intact. No loss (the events sent while the job was down wait in Kafka) and no duplicate
  (the replayed batch overwrites its own `batch_id=N` folder, dynamic partition overwrite).
- Point alerts: the alerts of the replayed batch that were already sent before the kill are sent
  again -> a few duplicates (at most one batch worth, ~0 to 10). No alert lost.
- DLQ, region_kpis_1min, sustained alerts: same story (Kafka sink = at-least-once), a few
  duplicates possible, nothing lost.
- Downtime of 1 min < 2 min watermark: the events produced during the downtime are newer than the
  watermark stored in the checkpoint, so they are not dropped as late.
- The first batch after the restart is bigger (1 min of backlog) so latency spikes once, then
  goes back to normal.
