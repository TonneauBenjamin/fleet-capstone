import glob
import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
TOPICS = ["vehicle_events", "fleet_catalog", "alerts", "region_kpis_1min",
          "vehicle_events_dlq", "daily_reports"]


def day_files_exist(ds):
    return len(glob.glob(f"{DATA_DIR}/events/event_date={ds}/*/*.parquet")) > 0


def publish(topic, key, value):
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "enable.idempotence": True})
    producer.produce(topic, key=key, value=json.dumps(value))
    if producer.flush(30) > 0:
        raise Exception(f"message not delivered to {topic}")


@dag(
    dag_id="fleet_daily_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
)
def fleet_daily_pipeline():

    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=day_files_exist,
        op_kwargs={"ds": "{{ ds }}"},
        mode="reschedule",
        poke_interval=60,
        timeout=2 * 60 * 60,
    )

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def check_topics():
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
        existing = admin.list_topics(timeout=10).topics
        missing = [t for t in TOPICS if t not in existing]
        if missing:
            raise Exception(f"missing topics: {missing}")
        print("all six topics exist")

    run_quality = BashOperator(
        task_id="run_quality",
        bash_command="cd /app && spark-submit spark_jobs/quality_job.py --date {{ ds }}",
    )

    @task.branch
    def check_verdict(ds=None):
        with open(f"{DATA_DIR}/quality/{ds}.json") as f:
            verdict = json.load(f)
        print(verdict)
        return "run_report" if verdict["passed"] else "publish_quality_alert"

    run_report = BashOperator(
        task_id="run_report",
        bash_command="cd /app && spark-submit spark_jobs/daily_report_job.py --date {{ ds }}",
    )

    @task
    def publish_report(ds=None):
        with open(f"{DATA_DIR}/reports/{ds}.json") as f:
            report = json.load(f)
        publish("daily_reports", ds, report)

    @task
    def publish_quality_alert(ds=None):
        with open(f"{DATA_DIR}/quality/{ds}.json") as f:
            verdict = json.load(f)
        publish("alerts", ds, {"alert_type": "data_quality_failed", "date": ds, "reasons": verdict["reasons"]})

    # runs after either branch; the default all_success would skip it because one branch is always skipped
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    verdict = check_verdict()
    wait_for_files >> check_topics() >> run_quality >> verdict
    verdict >> run_report >> publish_report() >> end
    verdict >> publish_quality_alert() >> end


fleet_daily_pipeline()
