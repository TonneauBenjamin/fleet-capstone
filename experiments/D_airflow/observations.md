# Part D - Airflow on the laptop

Screenshots: `D_dags_list`, `D_daily_grid`, `D_daily_graph_passed`, `D_daily_graph_failed`,
`D_daily_gantt_passed`, `D_lag_monitor_grid`.

- `fleet_daily_pipeline`, logical date 2026-10-13 (manual trigger at 13:15): sensor OK at once,
  check_topics OK, run_quality 2m11, check_verdict -> run_report (5 min) -> publish_report, the
  alert branch skipped, end success. Report message received in `daily_reports`.
- Logical date 2026-10-11 (one Parquet file copied there on purpose): verdict failed
  ("only N rows"), run_report and publish_report skipped, publish_quality_alert success,
  end success thanks to `none_failed_min_one_success`. The notifiers printed the
  `data_quality_failed` alert.
- Scheduled run 2026-10-12 (created when the DAG was unpaused, catchup=False): no files for that
  day, the sensor stays `up_for_reschedule` and frees its slot between checks (the other runs and
  the lag monitor ran normally meanwhile); it fails when the 2 h timeout is reached.
- `kafka_lag_monitor`: see E2 (alerts published at 12:10 and 12:15, "lag ok" otherwise).
  The run of logical date 12:10 executed at 12:15: Airflow runs an interval after it ends.
- Idempotence (`idempotence.log`): simulator stopped (day frozen), `run_quality` cleared twice:
  same verdict file, same sha256 (AC6C5A19...), attempts 2 and 3 in the Airflow logs.
