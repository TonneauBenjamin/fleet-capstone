"""
Task statistics of every stage of a running Spark application, read from the Spark UI REST API.
Used for the skew study: stage duration, and per task the duration and shuffle-read records (min / median / max).
Usage: python experiments/scripts/spark_stages.py http://skew:4040
"""
import json
import sys
import urllib.request
from datetime import datetime

base = sys.argv[1] + "/api/v1/applications"


def get(url):
    return json.load(urllib.request.urlopen(url))


app = get(base)[0]["id"]
def ms(t):
    return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fGMT").timestamp() * 1000


print("stage  description  stage_ms  tasks  task_ms(min/med/max)  shuffle_read_records(min/med/max)")
for s in reversed(get(f"{base}/{app}/stages")):
    tasks = get(f"{base}/{app}/stages/{s['stageId']}/{s['attemptId']}/taskList?length=1000")
    d = sorted(t["duration"] for t in tasks)
    r = sorted(t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in tasks)
    if not r or not d:
        continue
    mid = len(d) // 2
    stage_ms = ms(s["completionTime"]) - ms(s["submissionTime"])
    print(f"{s['stageId']:>5}  {s.get('description', ''):<11}  {stage_ms:>8.0f}  {len(d):>5}  "
          f"{d[0]}/{d[mid]}/{d[-1]:<16}  {r[0]}/{r[mid]}/{r[-1]}")
