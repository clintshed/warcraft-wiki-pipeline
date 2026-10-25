# Fix: warcraft.wiki.gg download stalling

Resilience hardening for the `warcraft_wiki_ingest.py` download pipeline,
addressing the weaknesses found in the two research tasks for this board:

* Research `t_27371440` — "Diagnose why Discord alerts are stuck in alert-loop
  / false-alarm state"
* Findings `t_b7f7f850` — "Diagnose why warcraft wiki ingestion keeps stalling"

The diagnosis (both tasks) is that a single slow / blocked / challenge-page on
`warcraft.wiki.gg` costs up to ~19s (4x exponential backoff) + up to 300s
(subprocess timeout) per failure, and swallows per-page fetch failures by
writing empty content. The daily cron never ran and the monitor watched a
frozen state file, so a transient block read as a permanent "stalled" alarm
that flooded Discord.

## Changes (this branch)

### 1. Adjusted timeouts  — `warcraft_wiki_ingest.py::donsetch_get`
* Per-fetch hard budget passed to `donsetch fetch --deadline-ms` (never a silent
  hang; donsetch returns an honest deadline error). `subprocess.run` `timeout`
  is now a safety net *above* the deadline.
* New env knobs: `WWS_DONSETCH_TIMEOUT` (subprocess cap, default 300s),
  `WWS_DONSETCH_DEADLINE_MS` (hard budget, default 120000ms).

### 2. Retry / backoff logic — `donsetch_get`, `_backoff_wait`, `_parse_retry_after`
* Exponential backoff now carries **jitter** (`WWS_RETRY_JITTER`, default 0.5s)
  so a transient rate-limit does not re-collide at the same instant (thundering
  herd guard).
* Explicit `Retry-After` hint from donsetch stderr meta is honored when present.
* Per-fetch timing is instrumented; a slow tail is logged (`SLOW tail detected`)
  rather than reading as a permanent stall.

### 3. Backpressure / bounded concurrency — `_fetch_pages_concurrent`, `run_incremental`
* Page content for the work set is fetched with a **bounded worker pool**
  (`WWS_MAX_WORKERS`, default 3) so one slow/blocked page can't stall the whole
  run; egress fan-out stays under the ~1 req / 3s Cloudflare guidance.
* All OpenViking writes remain sequential + crash-safe.

### 4. Notification throttling — `warcraft_wiki_monitor.py`
* Watches the **active** ingest state file (`warcraft_wiki_ingest_state.json`)
  that the rewritten ingest writes, not the frozen legacy file (the source of
  the perpetual "stalled" false alarm).
* Off-hours guard: the ingest is scheduled daily, so state older than
  (interval + grace) is "stalled" — normal off-hours inactivity is **not**
  reported.
* Alert throttling: a genuine stall alerts at most once per
  `WMS_ALERT_THROTTLE_S` (default 3600s) instead of every tick.
* The emitted JSON now carries `stalled` / `throttled` / `last_ingest_run` for
  the cron harness to act on.

### 5. No silent data loss — `ingest_one`
* A page whose fetch failed **and** has no cached ZIM body is now marked
  `fetch-failed` and skipped (retried next run) instead of writing an empty page
  to memory.

## Verification
* `python3 -m py_compile warcraft_wiking_ingest.py warcraft_wiki_monitor.py` pass.
* Standalone checks (scratch/ww_test_stall_fixes.py): backoff+Retry-After honored,
  fetch-failed guard writes no empty content (state untouched), bounded-concurrent
  fetch populates all titles.
* `python3 warcraft_wiki_monitor.py` on the live state file reports
  `stalled: false` during off-hours (no false alarm).

## Files
* `warcraft_wiki_ingest.py` (adjusted timeouts, retry/backoff, backpressure,
  no-empty-write)
* `warcraft_wiki_monitor.py` (active state file, off-hours guard, throttle)

## Follow-ups (not in this branch)
* The scheduled daily cron was previously not running (identify the root cause
  — likely a stale script-name typo in the job config) and must be confirmed.
* Consider wiring the monitor's `stalled`/`throttled` output into the Discord
  alert channel so alerts are throttled end-to-end.
