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

## vehicle_events_dlq (key = original Kafka key of the rejected message)
{
  "reason":           "broken_json" | "missing_field:<name>" | "bad_timestamp" | "speed_out_of_range",
  "raw_value":        "<the original message, unchanged>",
  "source_partition": 3,                      # where it was in vehicle_events
  "source_offset":    51204,
  "source_timestamp": "2026-10-18T14:03:12.123Z",
  "detected_at":      "2026-10-18T14:03:13.456Z"
}
