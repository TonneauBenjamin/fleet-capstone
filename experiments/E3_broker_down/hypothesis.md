# E3 - Kafka broker stopped for 1 minute during production (hypothesis written before the run)

Procedure: `docker stop capstone-kafka`, 60 s, `docker start capstone-kafka`. Then check the
simulator sequence numbers for holes (`experiments/scripts/seq_gaps.py`) and the logs of the
simulator, the Spark job and the notifiers.

Hypothesis:
- Single broker, RF = 1: while it is down every topic is unavailable (no other replica to take
  over). Data already written is on the `kafka-data` volume, so nothing stored is lost.
- Simulator (librdkafka producer): cannot connect, keeps the messages in its local queue
  (20/s x 60 s = 1200 messages, far below `queue.buffering.max.messages` = 100000) and retries.
  `message.timeout.ms` defaults to 300000 (5 min) > 60 s, so everything is delivered after the
  restart: no hole in the sequence numbers, ~60 s extra delay for those messages. Idempotence
  keeps them without duplicates and in order.
- Spark: the Kafka source cannot fetch the latest offsets. The Kafka client retries until its
  timeouts (`kafka.default.api.timeout.ms`, 60 s by default, and `fetchOffset.numRetries`), so we
  expect the queries to wait and continue. If the outage were longer than these retries, the query
  would fail and the job would stop (it would then restart from its checkpoint without loss).
- Notifiers: the group coordinator is on the broker. They log connection errors, reconnect alone
  and continue from their committed offsets; possibly a rebalance and a few re-read alerts.
- Settings that control this: producer `message.timeout.ms`, `retries`, `retry.backoff.ms`,
  `reconnect.backoff.ms`, `queue.buffering.max.messages`; Spark `kafka.default.api.timeout.ms`,
  `kafka.request.timeout.ms`, `fetchOffset.numRetries`, `fetchOffset.retryIntervalMs`;
  consumer `session.timeout.ms`, `reconnect.backoff.ms`.
