# Real-Time Fleet Monitoring Platform — Capstone (Big Data & Distributed Frameworks, EFREI)

Team: Benjamin Tonneau, Nathan Chevauche.
40 delivery vehicles send telemetry. Kafka carries every message, Spark validates and processes them,
Airflow (to come) orchestrates the daily jobs. The feed is dirty on purpose (duplicates, late,
corrupted messages, one hot vehicle); the platform must survive it and we measure how.

The teacher's original instructions for the provided files are kept in `docs/PROVIDED_README.md`.

## Architecture

```
 Simulator ──key=vehicle_id──► vehicle_events (6 partitions)
                                     │
 publish_catalog.py ──► fleet_catalog (3, compacted) ──read once──┐
                                     │                            │
                        Spark Structured Streaming (spark_jobs/streaming_app.py)
                        validate ──invalid──► vehicle_events_dlq (3)       [query "dlq"]
                           │valid
                        deduplicate (event_id, 2-min watermark)
                           │
                        enrich (broadcast LEFT join, unknown → region UNKNOWN)
                           │
                        Query 1 (foreachBatch) ──► Parquet data/events/event_date=…/batch_id=…
                                               └──► alerts (3)  speeding / harsh_braking
                                                       │
                                     alerts_notifier.py × 2 (group alerts-notifier)

 To come: Query 2 → region_kpis_1min, Query 3 → sustained_speeding alerts,
          Spark batch (quality, daily report), Airflow DAGs → daily_reports, lag monitor.
```

## Status

| Part | What | State |
|---|---|---|
| A1 | Catalog publisher, compaction shown | done |
| A2 | Alerts notifier, 2 instances, failover measured | done |
| A3 | Keys / partitions justified | done (`REPORT_NOTES.md`) |
| A4 | Delivery semantics per hop | done (`REPORT_NOTES.md`) |
| B1 | Validation + DLQ | done |
| B2 | Deduplication with watermark | done |
| B3 | Catalog enrichment, UNKNOWN region | done |
| B4 | Query 1: idempotent Parquet + point alerts | done |
| B5–B7, C, D, E | Query 2/3, batch jobs, Airflow, experiments, report | to do |

## Prerequisites (macOS / Linux)

* Docker Desktop running · Python 3.11 · Java 11 or 17 (`java -version`)
* Every new terminal tab for this project starts with:

```bash
cd ~/fleet-capstone
conda deactivate            # only if Anaconda is installed
source .venv/bin/activate
```

First time only: `python3.11 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`

## Run the pipeline (one tab per line)

| Tab | Command | What you should see |
|---|---|---|
| 1 | `./scripts/start_platform.sh` | `created …` (6 topics) then `delivered=40 failed=0` |
| 2 | `python3 producers/produce_vehicle_events.py --rate 20 --unknown-vehicles` | `run <id>: streaming ~20 events/s …` |
| 3 | `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_jobs/streaming_app.py` | `catalog loaded: 40 vehicles`, `started queries: ['dlq', 'query1']`, then a progress line per query every 30 s |
| 4 | `python3 -u consumers/alerts_notifier.py --name A` | `ASSIGNED …`, then `SPEEDING …` / `HARSH_BRAKING …` lines |
| 5 | `python3 -u consumers/alerts_notifier.py --name B` | A gives up one partition, B gets it |
| 6 | `./scripts/check_pipeline.sh` (after 2 minutes) | messages per topic, DLQ reasons, alerts per type, Parquet rows per region, notifier lag |

* Spark UI: <http://localhost:4040> (tabs *Jobs* and *Structured Streaming*).
* Add `--debug` to the Spark command to also print a table "messages per verdict" every 30 s.
* Keep evidence: append `| tee -i raw_outputs/<name>.txt` to a command.

### Demo: notifier failover (Part A2)
With tabs 4 and 5 running: Ctrl+C in tab 5 → A gets the partition back in < 1 s.
Brutal stop instead: `pkill -9 -f "alerts_notifier.py --name B"` → A gets it after ≈ 45 s (`session.timeout.ms`).

## Stop / reset

* Stop: Ctrl+C in each tab, then `docker compose down` (Kafka data is kept in the `kafka-data` volume).
* Full reset: `docker compose down -v && rm -rf checkpoints data`
  Always delete `checkpoints/` when Kafka is reset: checkpoints store Kafka offsets that no longer exist.

## Test of the Spark logic without Kafka

`python3 tests/test_spark_logic_local.py` streams simulator-generated messages through the same
functions as the real job and checks DLQ routing, deduplication, enrichment, alerts and the
idempotent Parquet write (output kept in `raw_outputs/b_local_test.txt`).

## Folder layout

```
producers/        publish_catalog.py (A1), send_test_alerts.py (test helper), produce_vehicle_events.py (provided)
consumers/        alerts_notifier.py (A2)
spark_jobs/       streaming_app.py (B1-B4)
scripts/          create_topics.py (provided), start_platform.sh, check_pipeline.sh
tests/            test_spark_logic_local.py
docs/             schemas.md (message formats), PROJECT_EXPLAINED.md, PROVIDED_README.md
raw_outputs/      evidence of our runs (logs, group descriptions, test output)
REPORT_NOTES.md   raw material for the report · LOG.md work log (who, what, problems)
```

Code written with the assistance of Claude (Anthropic), reviewed and run by the team.
