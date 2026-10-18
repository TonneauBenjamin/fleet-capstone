# Our project, explained simply

Read this before 21 October. Every section is short on purpose: say it in your own words.

---

## 1. The story in 30 seconds

A delivery company has **40 vans** in **5 regions** (Paris, Lyon, Marseille, Lille, Toulouse).
Every van sends a small message every few seconds: speed, position, fuel, "did it brake hard?".

The managers want:
1. **an alert at once** when a van drives too fast (> 120 km/h) or brakes hard;
2. **live numbers per region** every minute;
3. **a daily report**, made automatically, only if the day's data is good.

The difficulty: the data is **dirty on purpose** — broken messages, messages sent twice, late
messages, and one van that talks much more than the others. Our system must survive this and we
must **prove** it with numbers.

---

## 2. The three tools — one image each

| Tool | Think of it as… | In our project |
|---|---|---|
| **Kafka** | A **post office** with mailboxes (*topics*). A mailbox has drawers (*partitions*). Letters stay after being read; each reader keeps a **bookmark** (*offset*). | carries every message in and out |
| **Spark** | A **factory** that takes letters, checks them, sorts them, computes. *Streaming* = it works non-stop in small rounds (*micro-batches*). | validation, duplicates, region, files, alerts |
| **Airflow** | A **manager with an alarm clock** that starts jobs every day and checks they worked. | daily report, lag check (after 21 Oct) |

---

## 3. The journey of one message (what runs today)

```
van V0007 says "134 km/h"
  │
  ▼  Kafka mailbox  vehicle_events   (6 drawers; V0007 always goes to the same drawer)
  │
  ▼  Spark
     1. CHECK      broken? missing field? bad date? speed < 0 or > 250?
                   → yes: goes to mailbox vehicle_events_dlq with the reason ("rejected" box)
     2. DOUBLES    same event_id seen in the last 2 minutes? → thrown away
     3. REGION     look up V0007 in the catalog → "Lyon" (unknown van → "UNKNOWN")
     4. SAVE       written to a Parquet file  data/events/event_date=…/batch_id=…
     5. ALERT      134 > 120 → alert sent to mailbox alerts
  │
  ▼  Notifier (2 copies running) prints:  SPEEDING vehicle=V0007 …
```

---

## 4. What we built — one sentence per file

| File | What it does |
|---|---|
| `producers/publish_catalog.py` | sends the "ID card" of the 40 vans (plate, type, region) to Kafka |
| `consumers/alerts_notifier.py` | reads the alerts and prints them; we run 2 copies |
| `spark_jobs/streaming_app.py` | the Spark factory: check → doubles → region → save → alerts |
| `scripts/start_platform.sh` | starts Kafka, creates the 6 mailboxes, sends the catalog |
| `scripts/check_pipeline.sh` | prints a summary: how many messages, rejects, alerts, files |
| `tests/test_spark_logic_local.py` | tests the Spark logic with fake data, without Kafka |
| `REPORT_NOTES.md`, `LOG.md` | our notes and diary for the final report |

---

## 5. What we did, part by part, and what we proved

### A1 — The catalog and "compaction"
* **Idea:** the catalog mailbox is *compacted*: Kafka is **allowed** to delete old versions of a van's
  card and keep only the newest.
* **What we proved:** after sending the catalog twice there were **80** cards, nothing deleted.
  Kafka only cleans *closed* files, and a file closes after **7 days** by default. When we forced
  files to close after 10 seconds, the old cards were cleaned: **120 written → 80 readable**.
* **In one sentence:** compaction guarantees we always keep the newest card, but **not when** the
  old ones disappear — so Spark always keeps the newest card per van itself.

### A2 — The notifier and the "consumer group"
* **Idea:** 2 copies of the notifier share the 3 drawers of the alerts mailbox (2 + 1).
  Each saves its **bookmark after** printing an alert → an alert can be printed twice after a crash,
  but is **never lost** (*at-least-once*).
* **What we proved:** normal stop (Ctrl+C) → the other copy takes over in **< 1 second**.
  Brutal kill (`kill -9`) → takeover after **48 seconds** (Kafka waits 45 s of silence), restart
  exactly at the bookmark, **no alert lost**.

### A3 — Why these keys and drawer counts
* Key = vehicle → all messages of a van stay **in order** in one drawer. Cost: drawers are
  **unequal** (we measured 578 vs 1219 messages), and with the "hot" van one drawer got **50 %** of
  all traffic.
* 6 drawers for the big flow (6 Spark workers in parallel), 3 for small flows (allows up to 3
  notifiers), 1 for the daily report (one per day, perfect order).

### A4 — "Can a message be lost or doubled?" per step
| Step | Answer | Why, simply |
|---|---|---|
| van → Kafka | not doubled by retries | *idempotent* producer: Kafka recognises a resent message |
| Kafka → Spark | not lost | Spark writes down which messages it took (*checkpoint*); after a crash it takes them again |
| Spark → files | not doubled | a replayed round **overwrites** its own folder (`batch_id`) |
| Spark → alerts | can be doubled | Kafka only adds; a replayed round sends its alerts again |
| alerts → notifier | can be doubled, never lost | bookmark saved after printing; copies are skipped by `alert_id` |

### B1 — Checking messages (dead-letter queue)
* Every message gets **one verdict**: valid, or a reason (`broken_json`, `missing_field:…`,
  `bad_timestamp`, `speed_out_of_range`). Rejected ones go to `vehicle_events_dlq` with their text.
* **The NULL trap:** if the speed is missing, "speed > 250" is neither true nor false. With two
  separate filters the message would land **nowhere** and vanish. We compute one `reason` column
  first, then split on "reason empty / not empty" → every message lands somewhere.
* **Proof (test):** 2 111 messages in = rejected + valid, exactly; the reasons match the faults we
  injected.

### B2 — Removing doubles
* Spark remembers each `event_id` for 2 minutes of event time (*watermark*), so its memory stays
  **small** (old ids are forgotten).
* Side effect we measured: events arriving **more than 2 minutes late** are **dropped**
  (the simulator sends some 3–6 minutes late). Exception: in the very first round there is no
  watermark yet, so those are kept.

### B3 — Adding the region
* Spark reads the catalog **once at start**, keeps the **newest card per van**, and joins it to
  every event. Unknown vans (V0041–V0045) get region **UNKNOWN** instead of disappearing.
* **Limit:** if the catalog changes while Spark runs, Spark sees it only after a restart.

### B4 — Query 1: files + alerts
* Every round (10 s), Spark writes the events to Parquet files, in a folder per day and per round
  (`event_date=…/batch_id=…`), and sends alerts for speed > 120 or hard braking.
* **Proof (test):** replaying the same round **does not add rows** (it overwrites its own folder);
  53 dangerous events → exactly 53 alerts, each with a unique id like `V0009-…-302:speeding`.

---

## 6. Words to know (one line each)

* **Topic** — a mailbox for one kind of message. **Partition** — a drawer inside it.
* **Offset** — a message's number in its drawer. **Commit** — saving the bookmark.
* **Consumer group** — several copies sharing the drawers. **Rebalance** — redistributing drawers.
* **Idempotent** — doing it twice gives the same result as once.
* **Micro-batch** — one small round of Spark streaming. **Checkpoint** — Spark's notebook to restart.
* **Watermark** — "I accept events up to 2 minutes late"; it lets Spark forget old things.
* **DLQ (dead-letter queue)** — the box of rejected messages, with the reason.
* **Parquet** — a compact file format for tables. **Broadcast join** — send the small catalog to every worker.

---

## 7. The demo on 21 October (≈ 3 minutes)

Before the teacher arrives: everything already running (README, "Run the pipeline").
1. **Show the diagram** in `README.md` and say the story (section 1).
2. **Tab 3 (Spark):** "every 30 s Spark prints how many messages each query handled".
   Open <http://localhost:4040> → *Structured Streaming*.
3. **Tabs 4–5 (notifiers):** real alerts arriving; press **Ctrl+C in tab 5** → tab 4 takes over at once.
4. **Tab 6:** run `./scripts/check_pipeline.sh` → rejected messages per reason, alerts per type,
   rows per region (with UNKNOWN), notifier lag.
5. **Say what's next:** live regional numbers (Query 2), "speeding for minutes" alert (Query 3),
   daily quality check + report (Spark batch), Airflow, failure experiments, report.

## 8. Questions the teacher may ask — short answers

* **Why key by vehicle?** Order per van. Cost: unequal drawers (measured).
* **What does compaction guarantee?** The newest value per key is kept — not when old ones go.
* **Why commit after handling?** Never lose an alert; a crash can only cause a duplicate, which we skip by `alert_id`.
* **What happens to a message with no speed?** It goes to the DLQ with `missing_field:speed_kmh` — never lost (NULL trap).
* **Why is the dedup memory bounded?** The watermark lets Spark forget ids older than 2 minutes.
* **A duplicate arriving very late?** It is older than the watermark, so it is dropped as late data.
* **Why is the Parquet write safe on replay but not the alerts?** Files are overwritten per `batch_id`; Kafka only appends.
* **What if the catalog changes while Spark runs?** Not seen until restart (we read it once and keep it in memory).
* **Who did what?** Say it honestly — and that the code was written with an AI assistant, and you tested and can explain it.
