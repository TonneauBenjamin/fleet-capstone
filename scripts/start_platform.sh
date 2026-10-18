#!/usr/bin/env bash
# Starts the Kafka broker, creates the six topics and publishes the vehicle catalog.
# Safe to run several times. Run it from anywhere, with the project's venv activated.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! python3 -c "import confluent_kafka" 2>/dev/null; then
  echo "The project's Python environment is not active. Run first:"
  echo "  conda deactivate; source .venv/bin/activate"
  exit 1
fi

echo "== 1/3 Kafka broker"
docker compose up -d
echo "== 2/3 Topics (the script waits for the broker if it is still starting)"
python3 scripts/create_topics.py
echo "== 3/3 Vehicle catalog (40 vehicles, compacted topic fleet_catalog)"
python3 producers/publish_catalog.py | tail -1

cat <<'EOF'

Platform ready. Open one terminal tab per component (see README.md, "Run the pipeline"):
  simulator : python3 producers/produce_vehicle_events.py --rate 20 --unknown-vehicles
  spark     : spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_jobs/streaming_app.py
  notifier A: python3 -u consumers/alerts_notifier.py --name A
  notifier B: python3 -u consumers/alerts_notifier.py --name B
  check     : ./scripts/check_pipeline.sh
EOF
