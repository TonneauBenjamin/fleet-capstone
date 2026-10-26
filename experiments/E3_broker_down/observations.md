# E3 - observations

Raw outputs: `run.log`, `seq_gaps.txt`, `simulator.log`, `spark_streaming_full.log`,
`notifier1.log`, `notifier2.log`.

- Broker stopped 12:24:31, restarted 12:25:36 UTC (65 s).
- Simulator: errors `Failed to resolve 'kafka:29092'` (a stopped container disappears from Docker's
  DNS) then `Connection refused` while the broker restarts, then nothing. No message lost:
  - the 1305 messages created while the broker was down (seq 641 -> 1940) are all in Kafka;
  - the whole simulator run, seq 1 -> 10280, has no hole.
  The producer kept them in its local queue and sent them when the broker came back
  (message.timeout.ms = 5 min > 65 s). Hypothesis confirmed.
- Spark: **hypothesis wrong, the job died.** At 12:25:30, 59 s after the stop and 6 s before the
  broker came back, a task of the `dlq` query failed with
  `TimeoutException: Timeout of 60000ms expired before the position for partition vehicle_events-4
  could be determined` (Kafka client `default.api.timeout.ms` = 60 s). In local mode Spark does not
  retry a failed task, so the query stopped with a `StreamingQueryException`;
  `awaitAnyTermination()` returned and the whole application (the 4 queries) exited (container
  `Exited (1)`). Restarted by hand at ~12:27: it resumed from its checkpoints, nothing lost
  (see the final check).
- Notifiers: `Consumer group session timed out ... after 45033 ms` (the group coordinator is the
  broker), they dropped their partitions, reconnected by themselves and got them back at 12:26:27,
  51 s after the broker restart. No crash, no alert lost (commit after processing).

Settings that control it:
- producer: `message.timeout.ms` (time a message may wait before being dropped), `retries`,
  `retry.backoff.ms`, `reconnect.backoff.ms`, `queue.buffering.max.messages`;
- Spark Kafka source: `kafka.default.api.timeout.ms` / `kafka.request.timeout.ms` (60 s here, the
  outage was 65 s), `fetchOffset.numRetries`; task retries (`spark.task.maxFailures`, 1 in local
  mode, `local[*,4]` would allow 4 attempts);
- consumers: `session.timeout.ms` (45 s), `reconnect.backoff.ms`.
What we would change: a longer `kafka.default.api.timeout.ms` (or task retries) and a
`restart: on-failure` policy on the Spark container, since the checkpoints make a restart safe.
With one broker (RF = 1) the topics are simply unavailable during the outage: Kafka chooses
consistency over availability here (CP), it does not accept writes it cannot store.
