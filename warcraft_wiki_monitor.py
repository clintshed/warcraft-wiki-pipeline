#!/usr/bin/env python3
"""
Warcraft Wiki Forever sync monitor.

Checks the ACTIVE ingest state file every 10 minutes and reports whether the
pipeline is progressing. Fixed to stop the stale-monitor false alarm that was
flooding Discord (see research t_27371440 / findings t_b7f7f850):

  * Watches the ACTIVE state file (warcraft_wiki_ingest_state.json) that the
    rewritten ingest writes, not the frozen legacy file whose mtime never
    changes -> which made `stalled` perpetually true.
  * Off-hours guard: the ingest is scheduled daily (~01:00). Normal off-hours
    inactivity is NOT misreported as "stalled" -- a page is only "stalled" when
    the state is older than (run_interval + grace).
  * Notification throttle: a genuinely stalled state alerts at most once per
    WMS_ALERT_THROTTLE_S (default 3600s) instead of every tick, so a persistent
    failure can't flood Discord.

The cron harness reads the JSON `stalled`/`throttled`/`last_ingest_run` fields to
decide whether to post.
"""
import json
import os
import time
from datetime import datetime, timezone

STATE_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_ingest_state.json')
EMBEDDINGS_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_embeddings.jsonl')
SNAPSHOT_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_monitor_snapshot.json')
INGEST_REPORT = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_ingest_report.json')

# --- stale-false-alarm + notification-throttle tuning -------------------------
INGEST_INTERVAL_H = float(os.getenv("WMS_INGEST_INTERVAL_H", "24"))  # daily schedule
STALL_GRACE_H = float(os.getenv("WMS_STALL_GRACE_H", "6"))           # grace beyond interval
STALLED_AFTER_S = (INGEST_INTERVAL_H + STALL_GRACE_H) * 3600.0       # only stale past this
ALERT_THROTTLE_S = int(os.getenv("WMS_ALERT_THROTTLE_S", "3600"))     # alert max once / this many s

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def load_snapshot():
    if not os.path.exists(SNAPSHOT_FILE):
        return {"count": 0, "last_modified": 0, "last_pages": [], "last_alert": 0}
    with open(SNAPSHOT_FILE) as f:
        return json.load(f)

def save_snapshot(snap):
    with open(SNAPSHOT_FILE, 'w') as f:
        json.dump(snap, f, indent=2)

def load_ingest_report():
    if not os.path.exists(INGEST_REPORT):
        return {}
    try:
        with open(INGEST_REPORT) as f:
            return json.load(f)
    except Exception:
        return {}

def count_embeddings():
    embed_count = 0
    if os.path.exists(EMBEDDINGS_FILE):
        with open(EMBEDDINGS_FILE) as f:
            for line in f:
                if line.strip():
                    embed_count += 1
    return embed_count

def main():
    state = load_state()
    count = len(state)
    mtime = os.path.getmtime(STATE_FILE) if os.path.exists(STATE_FILE) else 0
    last_modified_str = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat() if mtime else None

    # Get recent pages (last 10)
    pages = list(state.items())[-10:]
    recent_pages = [{"page": p, "ts": t} for p, t in reversed(pages)]

    report = load_ingest_report()
    last_run_reported = report.get("generated")
    last_embedded = report.get("embedded")
    last_failed = report.get("failed")

    snap = load_snapshot()
    prev_count = snap.get("count", 0)
    added_since = count - prev_count

    age_seconds = max(0.0, time.time() - mtime) if mtime else None

    # Off-hours guard: only "stalled" when the state is older than the allowed
    # window (run interval + grace). Normal off-hours inactivity is NOT a stall
    # (this was the false-alarm root cause: frozen legacy file -> perpetual stall).
    if age_seconds is None:
        stalled = False
    else:
        stalled = age_seconds > STALLED_AFTER_S

    # Defensive: if a fresh ingest report exists, the pipeline is alive even if the
    # state mtime is a touch stale -> don't double-report.
    if stalled and last_run_reported:
        try:
            run_ts = datetime.fromisoformat(last_run_reported).replace(tzinfo=timezone.utc).timestamp()
            if (time.time() - run_ts) < STALLED_AFTER_S:
                stalled = False
        except Exception:
            pass

    prev_alert = snap.get("last_alert", 0)
    should_alert = stalled and (prev_alert == 0 or (time.time() - prev_alert) >= ALERT_THROTTLE_S)
    throttled = stalled and not should_alert

    out = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "state_file": STATE_FILE,
        "state_mtime": last_modified_str,
        "pages_processed": count,
        "embeddings_recorded": count_embeddings(),
        "added_since_last_check": added_since,
        "state_age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
        "stalled": stalled,
        "throttled": throttled,
        "last_ingest_run": last_run_reported,
        "last_ingest_embedded": last_embedded,
        "last_ingest_failed": last_failed,
        "recent_pages": recent_pages,
        "prev_count": prev_count,
    }
    save_snapshot({**snap, "count": count, "last_modified": mtime,
                   "last_pages": [p for p, _ in pages],
                   "last_alert": time.time() if should_alert else prev_alert})

    print(json.dumps(out, indent=2))

    if should_alert:
        print("ALERT: Sync stalled. State unchanged for %.1fh (>%.0fh window) at %s"
              % (age_seconds / 3600, STALLED_AFTER_S / 3600, last_modified_str))
    elif throttled:
        print("OK: Progress nominal. Alert throttled this tick (>=%.0h window); "
              "%d new pages since last check. Total %d"
              % (ALERT_THROTTLE_S / 3600, added_since, count))
    else:
        print("OK: Progress updated. %d new pages since last check. Total %d"
              % (added_since, count))

if __name__ == "__main__":
    main()
