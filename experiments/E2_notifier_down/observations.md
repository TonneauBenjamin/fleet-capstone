# E2 - observations

Raw outputs: `lag_timeline.log` (lag per partition every minute), `lag_monitor_runs.log` (logs of
the DAG), screenshot `screenshots/D_lag_monitor_grid.png`.

- Notifiers stopped 12:04:30 -> restarted 12:15:02 UTC.
- Lag (partition 0 / 1 / 2): +1 min 9/8/25, +3 min 29/36/47, +5 min 44/59/72, +8 min 71/97/102,
  +10 min 93/127/118. Linear growth, ~10-12 per partition per minute, partition 2 faster at the
  start (it holds V0007, whose sustained_speeding alerts kept coming after the demo scenario).
- `kafka_lag_monitor`:
  - run at 12:05 (logical date 12:00): lag 6/3/12 -> "lag ok";
  - run at 12:10 (logical date 12:05): lag 47/63/75 -> `consumer_lag_high` published (partitions 1, 2);
  - run at 12:15 (logical date 12:10): lag 95/129/119 -> `consumer_lag_high` published. It ran 3 s
    after the restart of the notifiers, before they had read anything.
- Catch-up: at 12:15:12 (10 s after the restart) the lag is 0 on all partitions. The notifiers
  printed the whole backlog, including the two `consumer_lag_high` alerts.
- The monitor never caused a rebalance of the notifiers (it reads the committed offsets without
  joining the group).

Explanation: matches the hypothesis. Nothing is lost because the alerts stay in the topic and the
committed offsets stay on the broker while the group is empty. Two remarks for the report:
the monitor only samples every 5 min, so it can alert on a lag that is already being absorbed
(12:15 run); and the alert goes into the same topic that is lagging, so it is only seen once the
notifiers are back (a real system would send it to another channel).
