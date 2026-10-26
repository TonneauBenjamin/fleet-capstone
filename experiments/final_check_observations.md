# Global check after E1-E4 (`final_check.txt`, 2026-10-13 13:32 UTC)

Everything that entered `vehicle_events` since the start of the platform, compared with the outputs
(simulator stopped 45 s before so that the job had processed everything):

- 1035521 messages, 10240 invalid -> 10240 messages in the DLQ, 0 extra, 0 missing.
- 1015205 distinct valid events, 995007 distinct events in the Parquet.
- 30 "duplicated rows": not a pipeline duplicate, it is the Parquet file we copied by hand from
  `batch_id=100` into `event_date=2026-10-11/batch_id=0` to test the failing Airflow branch.
- 20198 events missing from the Parquet:
  - 19995 were sent more than 2 min late by the simulator (its 3-6 min late events): dropped by the
    2-min watermark, as expected.
  - the other 203 (`scripts/missing_events.py`, output `missing_events.txt`): **all** have an `event_id` that exists in the
    Parquet with another event_time, 9 to 251 s away. They all arrived at 12:35-12:36 and 12:46,
    the two moments where we stopped the normal simulator and started another one at high rate
    (E4 burst, skew data). A new simulator process starts its sequence at 1 again, so it reuses
    the ids of events sent a few minutes before by the previous one; those ids were still in the
    dedup state (2-min watermark), so Spark dropped the new events as duplicates.
    Our dedup does what the subject asks (dedup on `event_id`); the problem is that the simulator's
    ids are not unique across restarts. Fix in a real system: ids unique per producer instance
    (UUID, or producer id + seq).
- Point alerts: 26844 published, 26844 distinct, 0 duplicate, 0 event without its alert.
- Across E1 (kill -9) and E3 (broker down, job crashed and restarted): no loss and no duplicate in
  the Parquet, the DLQ or the point alerts.
