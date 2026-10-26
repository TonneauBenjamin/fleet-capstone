# E4 - burst at ten times the normal rate (hypothesis written before the run)

Procedure: simulator at 200 events/s instead of 20 for 5 minutes, then back to 20. Batch durations
from the checkpoints (`stream_stats.py`), alert latency (`latency.py`), Spark UI statistics page.

Hypothesis:
- Kafka (sequential appends, ~millions of msg/s possible) and the producer (batching) are far from
  their limits: not the bottleneck.
- The bottleneck is the Spark job: 4 queries share one local driver/executor, and each batch has a
  fixed cost (planning, state store commit, catalog re-read, Parquet write in q1). With 10x more
  records per second the batches get bigger and somewhat longer; on 20 threads we expect the
  processing rate to stay above 200/s, so no growing backlog, but higher latency.
- q1 (foreachBatch: Parquet + alerts) is the slowest query (it already had ~2.5x fewer batches than
  q2/q3 at normal load), so it is where the latency grows first.
- The notifiers get ~10x more alerts (~300/min), still easy.
- What to change first: bound the batch size and cost (maxOffsetsPerTrigger, fewer shuffle
  partitions), then give Spark more cores/executors (or split the queries in separate apps).
  Adding Kafka partitions only helps if reading Kafka is the limit, which we do not expect.
