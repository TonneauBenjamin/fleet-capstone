# Part E - experiments (run on the laptop: i7-13700H 14 cores / 20 threads, 32 GB, Docker Desktop 16 GB)

Each folder: `hypothesis.md` (written before the run), the raw outputs (logs, measurements),
`observations.md` (results and explanation). UI screenshots are in `screenshots/`.

| folder | what | result in one line |
|---|---|---|
| `A_kafka/` | notifier partition assignment, kill one of two instances | partitions move after the 45 s session timeout; found and fixed a crash on commit during a rebalance |
| `B_streaming/` | DLQ, KPIs, sustained speeding, unknown vehicle | all rules fire; tombstone -> UNKNOWN at the next micro-batch |
| `C_batch/` | quality, report, partition pruning, skew | 1 of 589 partitions read; salting balances the hot task, AQE alone does not |
| `D_airflow/` | both DAGs, failing branch, idempotence | both branches end in success; same verdict hash when run twice |
| `E1_kill_streaming/` | `kill -9` of the streaming job | nothing lost, nothing duplicated (Parquet, DLQ, alerts) |
| `E2_notifier_down/` | notifiers stopped 10 min | lag ~+11/min/partition, 2 `consumer_lag_high`, caught up in 10 s |
| `E3_broker_down/` | broker stopped 65 s | producer lost nothing; the Spark job crashed (60 s Kafka timeout) and had to be restarted |
| `E4_burst/` | 10x the normal rate | no bottleneck at 200 or 1000 events/s; ~7 s fixed cost per batch sets the latency |
| `perf_study/` | shuffle partitions x trigger interval | see `perf_study/results.md` |
| `final_check.txt` | global accounting after E1-E4 | see `final_check_observations.md` |

Scripts in `scripts/` (run inside the project image, see the main README).
`scripts/missing_events.py` was a one-off analysis for `final_check_observations.md`.
