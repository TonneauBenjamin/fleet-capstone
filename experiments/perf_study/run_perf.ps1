# performance study: 4 runs of the streaming job (shuffle partitions x trigger), see plan.md
$log = "experiments\perf_study\run.log"
$runs = @(
    @{ n = "p6_default";   p = 6;   t = "" },
    @{ n = "p6_10s";       p = 6;   t = "10" },
    @{ n = "p200_default"; p = 200; t = "" },
    @{ n = "p200_10s";     p = 200; t = "10" }
)
function Now { (Get-Date).ToUniversalTime().ToString('s') }
"== main spark-streaming stopped at $(Now)Z" | Out-File -Append -Encoding utf8 $log
docker stop spark-streaming | Out-Null
foreach ($r in $runs) {
    $name = $r.n
    "== $name (shuffle.partitions=$($r.p), trigger='$($r.t)', maxOffsetsPerTrigger=100000) started at $(Now)Z" | Out-File -Append -Encoding utf8 $log
    docker compose run -d --no-deps --name "perf-$name" -e DATA_DIR=/app/data/perf/$name -e MAX_OFFSETS_PER_TRIGGER=100000 -e TRIGGER_SECONDS=$($r.t) spark-streaming spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.sql.shuffle.partitions=$($r.p) spark_jobs/streaming_job.py 2>&1 | Out-Null
    $start = Get-Date
    $caught = $false
    while (-not $caught -and ((Get-Date) - $start).TotalMinutes -lt 25) {
        Start-Sleep 15
        $out = cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/stream_stats.py --checkpoint data/perf/$name/checkpoints/q1_events --last 1 2>&1"
        $row = $out | Select-String -Pattern '^\s*(\d+)\s+([\d.]+)\s+(\d+)\s*$' | Select-Object -Last 1
        if ($row -and [int]$row.Matches[0].Groups[1].Value -gt 1 -and [int]$row.Matches[0].Groups[3].Value -lt 2000) { $caught = $true }
    }
    $c = (Get-Date).ToUniversalTime()
    "   caught up at $($c.ToString('s'))Z" | Out-File -Append -Encoding utf8 $log
    Start-Sleep 200
    $e = (Get-Date).ToUniversalTime()
    cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/latency.py --since $($c.AddSeconds(20).ToString('s')) --until $($e.ToString('s')) 2>&1" | Select-Object -Last 1 | Out-File -Encoding utf8 "experiments\perf_study\${name}_latency.txt"
    cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/stream_stats.py --checkpoint data/perf/$name/checkpoints/q1_events 2>&1" | Select-Object -Skip 2 | Out-File -Encoding utf8 "experiments\perf_study\${name}_batches.txt"
    docker rm -f "perf-$name" | Out-Null
    "   finished at $(Now)Z" | Out-File -Append -Encoding utf8 $log
}
docker start spark-streaming | Out-Null
"== main spark-streaming restarted at $(Now)Z" | Out-File -Append -Encoding utf8 $log
"== done" | Out-File -Append -Encoding utf8 $log
