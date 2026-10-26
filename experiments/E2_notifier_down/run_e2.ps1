$o = "experiments\E2_notifier_down\lag_timeline.log"
function Lag { $t=(Get-Date).ToUniversalTime().ToString('s'); docker exec capstone-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group alerts-notifier 2>&1 | Select-String "alerts-notifier" | % { "$t`Z " + (($_.Line -split '\s+')[2..5] -join ' ') } | Out-File -Append -Encoding utf8 $o }
"# time partition committed end lag" | Out-File -Encoding utf8 $o
Lag
"== notifiers stopped at $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Append -Encoding utf8 $o
docker stop notifier1 notifier2 | Out-Null
for ($i=1; $i -le 10; $i++) { Start-Sleep 60; Lag }
"== notifiers started at $((Get-Date).ToUniversalTime().ToString('s'))Z" | Out-File -Append -Encoding utf8 $o
docker start notifier1 notifier2 | Out-Null
for ($i=1; $i -le 6; $i++) { Start-Sleep 10; Lag }
"== done" | Out-File -Append -Encoding utf8 $o
