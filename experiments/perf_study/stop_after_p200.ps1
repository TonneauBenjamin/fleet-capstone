# stop the perf study after the 3rd run: with 200 shuffle partitions the job cannot even drain the backlog,
# a 4th run (200 partitions + 10 s trigger) would only confirm it and take another 30 min
$log = "experiments\perf_study\run.log"
while (-not (Select-String -Path $log -Pattern "p200_10s" -Quiet)) { Start-Sleep 5 }
Stop-Process -Id 14240 -Force
Start-Sleep 5
docker rm -f perf-p200_10s 2>&1 | Out-Null
docker start spark-streaming | Out-Null
"== run p200_10s cancelled (see results.md), main spark-streaming restarted at $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Append -Encoding utf8 $log
