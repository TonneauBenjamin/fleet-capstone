# Real-Time Fleet Monitoring Platform (V1)

Big Data & Distributed Frameworks capstone, first version: infrastructure, Kafka layer and Spark Structured Streaming.
Everything runs in Docker, only Docker Desktop is needed. Progress review: `V1_progress_review.pdf`.

## Start

    docker compose build
    docker compose up -d

This starts:
* `kafka`: one broker in KRaft mode (`localhost:9092` from the host, `kafka:29092` from the containers)
* `init`: creates the topics and publishes the 40 vehicles to `fleet_catalog`, then exits
* `simulator`: the provided telemetry simulator at 20 events/s
* `notifier1`, `notifier2`: two instances of the alerts notifier (group `alerts-notifier`)
* `spark-streaming`: the Spark Structured Streaming job (Spark UI on http://localhost:4040)

Spark outputs go to `data/` (`data/events` = Parquet files, `data/checkpoints` = one checkpoint per query).

Stop everything with `docker compose down` (add `-v` to erase Kafka data).

## Look at a topic

    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fleet_catalog --from-beginning --property print.key=true

## Kafka: notifier partition assignment

    docker compose logs notifier1
    docker compose logs notifier2
    docker compose kill notifier2
    docker compose logs -f notifier1     # after ~45 s (session timeout) notifier1 gets the partitions of notifier2
    docker compose start notifier2       # only the moved partition goes back (cooperative-sticky)

Committed offsets and lag of the group:

    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group alerts-notifier

## Spark: streaming job

Four queries in one application (`spark_jobs/streaming_job.py`): `dlq`, `q1_events`, `q2_region_kpis`, `q3_sustained`.

    docker compose logs -f spark-streaming

Read the outputs:

    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vehicle_events_dlq --from-beginning --property print.key=true
    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic region_kpis_1min --from-beginning --property print.key=true
    docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alerts --from-beginning

Sustained speeding demo (the simulator alone almost never triggers it):

    docker compose run --rm --no-deps simulator python producers/scenario_sustained_speeding.py --vehicle V0007

Unknown vehicle demo: delete V0040 from the catalog with a tombstone (null value) while the job runs,
its next events get the region `UNKNOWN`:

    echo "V0040:NULL" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic fleet_catalog --property parse.key=true --property key.separator=: --property null.marker=NULL

## Next steps

Spark batch jobs (quality check, daily report), Airflow orchestration, failure experiments.

## Layout

    producers/      simulator (provided), catalog publisher, sustained speeding scenario
    consumers/      alerts notifier
    spark_jobs/     streaming job
    scripts/        topic creation (provided)
