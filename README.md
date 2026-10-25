# warcraft-wiki-pipeline

Ingestion pipeline for **WoW:Forever** scope pages from `warcraft.wiki.gg` into
OpenViking.

## Layout
* `warcraft_wiki_ingest.py` — incremental ingest. Enumerates scope pages,
  fetches content via the Cloudflare-safe DonSeTch egress, classifies
  Source/>Version, embeds (Nomic), and pushes to OpenViking.
* `warcraft_wiki_monitor.py` — watches the ingest state file and reports whether
  the pipeline is progressing (with off-hours + notification-throttling guards).
* `warcraft_wiki_sync.py` / `warcraft_wiki_sync_html_fallback.py` — legacy
  ingestion paths retained for compatibility.
* `warcraft_wiki_migrate.py`, `warcraft_wiki_zim_cache_build.py` — tooling for
  state migration and ZIM body caching.
* `warcraft_wiki_hogger_regression_test.py`, `warcraft_wiki_sync_test.py` — tests.
* `enumerate_forever_union.py` — builds the Forever scope title set.

## Configuration (env)
See the `# --- config ---` block at the top of `warcraft_wiking_ingest.py` and
the tuning block in `warcraft_wiki_monitor.py`. Key knobs:
`WWS_DONSETCH_TIMEOUT`, `WWS_DONSETCH_DEADLINE_MS`, `WWS_RETRY_JITTER`,
`WWS_MAX_WORKERS`, `WMS_INGEST_INTERVAL_H`, `WMS_STALL_GRACE_H`,
`WMS_ALERT_THROTTLE_S`.

## Stalling resilience
See `CHANGES_STALL_FIX.md` for the recent hardening: adjusted per-fetch
timeouts, jittered backoff + Retry-After, bounded-concurrency egress
backpressure, off-hours/throttled monitoring, and no-silent-empty-write.

## Runtime artifacts (git-ignored)
`warcraft_wiki_ingest_state.json`, `warcraft_wiki_embeddings.jsonl`,
`warcraft_wiki_zim_cache.json`, reports, and the monitor snapshot.
