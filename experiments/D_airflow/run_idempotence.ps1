# D.3 proof: run_quality twice for the same (frozen) date, compare the verdict files
$o = "experiments\D_airflow\idempotence.log"
function Now { (Get-Date).ToUniversalTime().ToString('s') }
function WaitQuality {
    Start-Sleep 20
    do {
        Start-Sleep 15
        $st = cmd /c "docker exec airflow airflow tasks state fleet_daily_pipeline run_quality 2026-10-13T00:00:00+00:00 2>nul" | Select-Object -Last 1
    } until ($st -match "success|failed")
    return $st
}
"== simulator stopped at $(Now)Z (data of 2026-10-13 frozen)" | Out-File -Encoding utf8 $o
docker compose stop simulator 2>&1 | Out-Null
Start-Sleep 40
foreach ($i in 1, 2) {
    "== clear run_quality (run $i) at $(Now)Z" | Out-File -Append -Encoding utf8 $o
    cmd /c "docker exec airflow airflow tasks clear fleet_daily_pipeline -t ^run_quality$ -s 2026-10-13 -e 2026-10-13 -y 2>&1" | Out-Null
    $st = WaitQuality
    "   state: $st at $(Now)Z" | Out-File -Append -Encoding utf8 $o
    "   sha256 of data/quality/2026-10-13.json: $((Get-FileHash data\quality\2026-10-13.json).Hash)" | Out-File -Append -Encoding utf8 $o
    Get-Content data\quality\2026-10-13.json | Out-File -Append -Encoding utf8 $o
}
docker compose start simulator 2>&1 | Out-Null
"== simulator restarted at $(Now)Z" | Out-File -Append -Encoding utf8 $o
"== done" | Out-File -Append -Encoding utf8 $o
