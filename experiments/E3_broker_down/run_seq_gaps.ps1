$f="experiments\E3_broker_down\seq_gaps.txt"
"## all messages of the simulator run that crossed the outage (12:23:00 -> now - 10 s)" | Out-File -Encoding utf8 $f
cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/seq_gaps.py --since 2026-10-13T12:23:00 2>&1" | Select-Object -Last 1 | Out-File -Append -Encoding utf8 $f
"## only messages created while the broker was down (12:24:31 -> 12:25:36)" | Out-File -Append -Encoding utf8 $f
cmd /c "docker compose run --rm --no-deps -T simulator python experiments/scripts/seq_gaps.py --since 2026-10-13T12:24:31 --until 2026-10-13T12:25:36 2>&1" | Select-Object -Last 1 | Out-File -Append -Encoding utf8 $f
"== done" | Out-File -Append -Encoding utf8 $f
