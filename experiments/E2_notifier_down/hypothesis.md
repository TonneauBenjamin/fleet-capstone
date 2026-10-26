# E2 - alerts notifier stopped for 10 minutes (hypothesis written before the run)

Procedure: `kafka_lag_monitor` unpaused (every 5 min, threshold 50 per partition), both notifiers
stopped for 10 min, lag recorded every minute with kafka-consumer-groups, then restart.

Hypothesis:
- Spark keeps publishing alerts (~30/min measured before, ~10/min per partition), nobody commits:
  the lag grows linearly, about +10 per partition per minute -> ~100 per partition after 10 min.
- The monitor run around +5 min sees ~50 (around the threshold, may or may not alert); the runs
  after that see > 50 and publish `consumer_lag_high`, one alert per run.
- Nothing is lost: alerts stay in Kafka (7-day retention) and the committed offsets of the group
  are kept by the broker even when the group has no member.
- On restart, the notifiers rejoin, resume from their committed offsets and read the backlog in a
  few seconds (printing is fast); lag back to 0, including the lag alert itself. The next monitor
  run reports "lag ok".
