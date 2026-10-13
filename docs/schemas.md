# Message formats (contract between all components)

## alerts (key = vehicle_id; system alerts: key = their subject, e.g. the date or the group name)
{
  "alert_id":    "V0001-198:speeding",     # deterministic: <event_id or window>:<alert_type>
  "alert_type":  "speeding" | "harsh_braking" | "sustained_speeding" | "data_quality_failed" | "consumer_lag_high",
  "vehicle_id":  "V0001" | null,
  "event_time":  "2026-10-13T07:10:30.106909+00:00",   # when it happened (UTC)
  "detected_at": "2026-10-13T07:10:32.000000+00:00",   # when the alert was produced (UTC)
  "details":     { ... }                                # e.g. {"speed_kmh": 127.3}
}
