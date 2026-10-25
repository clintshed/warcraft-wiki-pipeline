#!/usr/bin/env python3
"""
Warcraft Wiki Forever sync monitor.
Checks state file progress every 10 minutes.
"""
import json
import os
import time
from datetime import datetime, timezone

STATE_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_state.json')
EMBEDDINGS_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_embeddings.jsonl')
SNAPSHOT_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_monitor_snapshot.json')

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE) as f:
        return json.load(f)

def load_snapshot():
    if not os.path.exists(SNAPSHOT_FILE):
        return {"count":0, "last_modified":0, "last_pages":[]}
    with open(SNAPSHOT_FILE) as f:
        return json.load(f)

def save_snapshot(snap):
    with open(SNAPSHOT_FILE,'w') as f:
        json.dump(snap,f,indent=2)

def main():
    state = load_state()
    count = len(state)
    mtime = os.path.getmtime(STATE_FILE) if os.path.exists(STATE_FILE) else 0
    last_modified_str = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()

    # Get recent pages (last 10)
    pages = list(state.items())[-10:]
    recent_pages = [{"page":p,"ts":t} for p,t in reversed(pages)]

    # Count embeddings
    embed_count = 0
    if os.path.exists(EMBEDDINGS_FILE):
        with open(EMBEDDINGS_FILE) as f:
            for line in f:
                embed_count += 1

    snap = load_snapshot()
    prev_count = snap.get("count",0)
    prev_mtime = snap.get("last_modified",0)

    added_since = count - prev_count
    stalled = (mtime == prev_mtime) and (time.time() - mtime > 600)  # 10 min no change

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "state_file": STATE_FILE,
        "state_mtime": last_modified_str,
        "pages_processed": count,
        "embeddings_recorded": embed_count,
        "added_since_last_check": added_since,
        "stalled": stalled,
        "recent_pages": recent_pages,
        "prev_count": prev_count
    }

    # Save snapshot
    save_snapshot({
        "count": count,
        "last_modified": mtime,
        "last_pages": [p for p,_ in pages]
    })

    print(json.dumps(report, indent=2))

    # Alert if stalled
    if stalled:
        print(f"ALERT: Sync appears stalled. State file unchanged for >10 min at {last_modified_str}")
    else:
        print(f"OK: Progress updated. {added_since} new pages since last check. Total {count}")

if __name__ == "__main__":
    main()
