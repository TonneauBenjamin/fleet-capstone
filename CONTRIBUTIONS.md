# Contribution statements

We split the project along the architecture: the Parquet hand-off files are the boundary between the
real-time path (Kafka + Spark Structured Streaming) and the batch / orchestration path (Spark batch +
Airflow). Each of us owned one side end to end, including the experiments that stress it. The
interface between the two sides (topic names and keys, Parquet layout `event_date/batch_id`, quality
verdict format) and the architecture choices were decided together, and we wrote the report together.

**Nathan CHEVAUCHE** was responsible for the real-time path: the Docker Compose infrastructure (KRaft
broker with the internal listener, shared image, init container), Part A (idempotent catalog
publisher, alerts notifier with manual commits and cooperative-sticky assignment, choice of keys and
partitions, delivery semantics of each hop) and Part B (streaming job: validation and dead-letter
queue, watermark deduplication, broadcast enrichment with UNKNOWN regions, the three queries, the
idempotent Parquet sink and the checkpoints). He ran experiments E1 (kill -9 of the streaming job)
and E3 (broker down), and the demos of Parts A and B.

**Benjamin TONNEAU** was responsible for the batch and orchestration path: Part C (quality job and
its thresholds, daily report, proof of partition pruning, skew study with salting versus AQE) and
Part D (the `fleet_daily_pipeline` and `kafka_lag_monitor` DAGs, sensor mode, branching and trigger
rule, logical date / catchup / backfill, idempotence proof), plus the measurement scripts of
`experiments/scripts/`. He ran experiments E2 (notifier down and lag monitor) and E4 (burst), and the
performance study.
