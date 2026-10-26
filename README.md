# Real-Time Fleet Monitoring Platform

Big Data & Distributed Frameworks capstone. Everything runs in Docker, only Docker Desktop is needed.

## Start

    docker compose build
    docker compose up -d

This starts:
* `kafka`: one broker in KRaft mode (`localhost:9092` from the host, `kafka:29092` from the containers)
* `init`: creates the six topics and publishes the 40 vehicles to `fleet_catalog`, then exits
* `simulator`: the provided telemetry simulator at 20 events/s
* `notifier1`, `notifier2`: two instances of the alerts notifier (group `alerts-notifier`)
* `spark-streaming`: the Spark Structured Streaming job (Spark UI on http://localhost:4040)
* `airflow`: Airflow 2.9.3 in standalone mode (UI on http://localhost:8080, user `admin`)

Spark outputs go to `data/` (`data/events` = Parquet hand-off files, `data/checkpoints` = one checkpoint per query).

Stop everything with `docker compose down` (add `-v` to erase Kafka data).

## Look at a topic

    docker exec capstone-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fleet_catalog --from-beginning --property print.key=true

## Part A: notifier partition assignment

    docker logs notifier1
    docker logs notifier2
    docker kill notifier2
    docker logs -f notifier1     # after ~45 s (session timeout) notifier1 gets the partitions of notifier2
    docker start notifier2       # only the moved partition goes back (cooperative-sticky)

Committed offsets and lag of the group:

    docker exec capstone-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group alerts-notifier

## Part B: streaming job

Four queries in one application (`spark_jobs/streaming_job.py`): `dlq`, `q1_events`, `q2_region_kpis`, `q3_sustained`.

    docker logs -f spark-streaming

Read the outputs:

    docker exec capstone-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vehicle_events_dlq --from-beginning --property print.key=true
    docker exec capstone-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic region_kpis_1min --from-beginning --property print.key=true
    docker exec capstone-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts --from-beginning

Sustained speeding demo (the simulator alone almost never triggers it):

    docker compose run --rm --no-deps simulator python producers/scenario_sustained_speeding.py --vehicle V0007

Unknown vehicle demo: delete V0040 from the catalog with a tombstone (null value) while the job runs,
its next events get the region `UNKNOWN`:

    echo "V0040:NULL" | docker exec -i capstone-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic fleet_catalog --property parse.key=true --property key.separator=: --property null.marker=NULL

## Part C: batch jobs

They read the Parquet files written by the streaming job, for one date (UTC). Run them in a one-off container:

    docker compose run --rm --no-deps spark-streaming spark-submit spark_jobs/quality_job.py --date 2026-10-13
    docker compose run --rm --no-deps spark-streaming spark-submit spark_jobs/daily_report_job.py --date 2026-10-13

* quality verdict: `data/quality/<date>.json`
* daily report: `data/reports/<date>.json` (the physical plan printed by the job shows `PartitionFilters` on `event_date`)

Skew study: produce enough skewed data first (about 15 min), then run the study and open its Spark UI on http://localhost:4041:

    docker compose stop simulator
    docker compose run -d --no-deps --name simulator-hot simulator python producers/produce_vehicle_events.py --rate 1000 --hot-vehicle
    docker rm -f simulator-hot          # after ~15 minutes
    docker compose start simulator
    docker compose run --rm --no-deps -p 4041:4040 spark-streaming spark-submit spark_jobs/skew_study.py --date 2026-10-13 --keep-ui

Same study with every row repeated 10 times (more rows per vehicle, needs more driver memory):

    docker compose run --rm --no-deps -p 4041:4040 spark-streaming spark-submit --driver-memory 4g spark_jobs/skew_study.py --date 2026-10-13 --copies 10 --keep-ui

## Part D: Airflow

Password of the `admin` user (generated at the first start):

    docker exec airflow cat /opt/airflow/standalone_admin_password.txt

DAGs are paused at creation, unpause them (or use the toggle in the UI):

    docker exec airflow airflow dags unpause fleet_daily_pipeline
    docker exec airflow airflow dags unpause kafka_lag_monitor

`fleet_daily_pipeline` processes the day before its run (logical date). To run it on today's data:

    docker exec airflow airflow dags trigger fleet_daily_pipeline -e 2026-10-13

Run one task again for the same date (idempotence check), then compare the verdict file:

    docker exec airflow airflow tasks clear fleet_daily_pipeline -t "^run_quality$" -s 2026-10-13 -e 2026-10-13 -y

Lag monitor demo: stop the notifiers for 10 minutes, `kafka_lag_monitor` publishes a `consumer_lag_high` alert, then restart them:

    docker stop notifier1 notifier2
    docker start notifier1 notifier2

Start again from scratch (topics, Parquet and checkpoints):

    docker compose down -v
    rm -rf data          # PowerShell: Remove-Item -Recurse -Force data

## Part E: experiments

Hypotheses, raw outputs (logs, measurements, Spark/Airflow UI screenshots) and observations are in
`experiments/` (one folder per experiment, see `experiments/README.md`). The measurement scripts are
in `experiments/scripts/` and run in a container of the project image, for example:

    docker compose run --rm --no-deps -T simulator python experiments/scripts/check_pipeline.py
    docker compose run --rm --no-deps -T simulator python experiments/scripts/stream_stats.py --last 10
    docker compose run --rm --no-deps -T simulator python experiments/scripts/latency.py --since 2026-10-13T12:31:44 --until 2026-10-13T12:34:44

Screenshots need a headless browser image (only for the experiments):

    docker build -t capstone-screens -f experiments/scripts/Dockerfile.screens .
    docker run --rm --network capstone_default -v "${PWD}:/app" -w /app capstone-screens python experiments/scripts/screenshots.py experiments/screenshots name=http://spark-streaming:4040/StreamingQuery/

## Layout

    producers/      simulator (provided), catalog publisher, sustained speeding scenario
    consumers/      alerts notifier
    spark_jobs/     streaming job, quality job, daily report job, skew study
    airflow_dags/   fleet_daily_pipeline, kafka_lag_monitor
    scripts/        topic creation (provided)
    experiments/    Part E: hypotheses, raw outputs, observations, measurement scripts
    report.pdf      the report
    CONTRIBUTIONS.md  contribution statement of each student
