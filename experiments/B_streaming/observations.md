# Part B - streaming job demos (on the laptop)

Raw outputs: `dlq_sample.txt`, `region_kpis_sample.txt`, `sustained_alerts.txt`,
`unknown_V0040.txt`, `demo_times.txt`, `scenario.log`; screenshot
`screenshots/B_spark_streaming_queries.png` (the 4 queries running); global accounting in
`../final_check.txt`.

- DLQ: every invalid input is in `vehicle_events_dlq` with its reason (`invalid_json`,
  `missing_vehicle_id`, `bad_timestamp`, `speed_out_of_range`); E1 check: 353 DLQ messages for
  353 invalid inputs.
- Dedup: 0 real duplicate in the Parquet over the whole day (`final_check.txt`, see note there).
- Region KPIs: one message per (region, 1-min window), updated while the window is open.
- Sustained speeding: scenario on V0007 (480 events at 108-118 km/h in 2 min) -> alerts for the
  5-min windows that contain it (one per window and per micro-batch where it changes, update mode).
- Unknown vehicle: tombstone for V0040 sent at 12:00:59. V0040 events are `Lyon` up to
  12:00:51 and `UNKNOWN` from 12:00:54 (the first micro-batch planned after the tombstone, which
  also contained a few events from just before it), 73 rows; no other vehicle is UNKNOWN.
  Catalog republished at 12:03 (V0040 back).
