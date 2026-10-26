$o="experiments\C_batch\skew_data_generation.log"
"== simulator stopped, hot simulator 1000 events/s --hot-vehicle started at $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Encoding utf8 $o
docker compose stop simulator 2>&1 | Out-Null
docker compose run -d --no-deps --name simulator-hot simulator python producers/produce_vehicle_events.py --rate 1000 --hot-vehicle 2>&1 | Out-Null
Start-Sleep 600
cmd /c "docker run --rm --network capstone_default -v `"%CD%:/app`" -w /app capstone-screens python experiments/scripts/screenshots.py experiments/screenshots C_q1_statistics_at_1000eps=query:spark-streaming:4040:q1_events 2>&1" | Out-File -Append -Encoding utf8 $o
Start-Sleep 270
"== hot simulator stopped at $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Append -Encoding utf8 $o
docker rm -f simulator-hot 2>&1 | Out-Null
docker compose start simulator 2>&1 | Out-Null
"== done" | Out-File -Append -Encoding utf8 $o
