# global check after E1-E4 (before the perf study, which republishes alerts on purpose)
$o = "experiments\final_check.txt"
"== simulator stopped at $((Get-Date).ToUniversalTime().ToString('s'))Z, check after the streaming job drained" | Out-File -Encoding utf8 $o
docker compose stop simulator 2>&1 | Out-Null
Start-Sleep 45
cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/check_pipeline.py 2>&1" | Select-Object -Skip 2 | Out-File -Append -Encoding utf8 $o
docker compose start simulator 2>&1 | Out-Null
"== done $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Append -Encoding utf8 $o
