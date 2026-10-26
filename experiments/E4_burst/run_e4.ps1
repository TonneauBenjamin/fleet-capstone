$d="experiments\E4_burst"
$t=(Get-Date).ToUniversalTime()
"baseline window for latency: $($t.AddMinutes(-3).ToString('s')) -> $($t.ToString('s'))" | Out-File -Encoding utf8 "$d\run.log"
cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/latency.py --since $($t.AddMinutes(-3).ToString('s')) --until $($t.ToString('s')) 2>&1" | Select-Object -Last 1 | Out-File -Encoding utf8 "$d\latency_baseline_20eps.txt"
cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/stream_stats.py --last 10 2>&1" | Select-Object -Skip 2 | Out-File -Encoding utf8 "$d\q1_batches_baseline_20eps.txt"
docker compose stop simulator 2>&1 | Out-Null
"== burst 200 events/s started at $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Append -Encoding utf8 "$d\run.log"
docker compose run -d --no-deps --name simulator-burst simulator python producers/produce_vehicle_events.py --rate 200 2>&1 | Out-Null
Start-Sleep 300
"== burst stopped at $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Append -Encoding utf8 "$d\run.log"
docker rm -f simulator-burst 2>&1 | Out-Null
docker compose start simulator 2>&1 | Out-Null
"== done" | Out-File -Append -Encoding utf8 "$d\run.log"
