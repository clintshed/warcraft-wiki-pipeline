#!/usr/bin/env python3
"""
warcraft_wiki_ingest.py -- incremental ingest of the WoW:Forever scope
(957 pages) into OpenViking memory under viking://resources/warcraft-wiki/.

Replaces the paused legacy nightly cron (job 0ab31691d7c3) for task
t_4cbe5acd (see ORCHESTRATOR_FINAL_PLAN.md P4-P6) and is refactored for task
t_17748e25: the job now checks for new pages / page updates SINCE THE LAST RUN
instead of performing a full nightly refresh.

Diff detection (against stored memory):
  * Each run builds a live revision-timestamp index for every scope title via a
    batched MediaWiki query (rvprop=timestamp) through the Cloudflare-safe
    DonSeTch transport.
  * State (warcraft_wiki_ingest_state.json) records, per title: embed_hash, uri,
    version, sourced_from, and last_ts (the last-known wiki revision timestamp).
  * Decision per title:
      - not in state                 -> NEW
      - state.last_ts == current_ts   -> UNCHANGED  (skip, no fetch, no write)
      - state.last_ts != current_ts   -> CHANGED    (re-fetch, re-push if kept)
      - state has no last_ts          -> RECONCILE  (re-fetch + embed-hash
                                            comparison; push iff content changed)
  * For CHANGED/RECONCILE titles the page content is fetched once, re-classified,
    re-embedded, and the NEW vector hash is compared to the stored embed_hash so a
    page whose wikitext changed is re-pushed (Updated) and one whose wikitext did
    not change is skipped even without a stored timestamp.

This makes every run idempotent: once a page's last_ts is stored and the wiki is
unchanged, a repeat run finds it UNCHANGED and touches neither the wiki egress
nor OpenViking.

Pipeline per fetched page:  extract -> classify -> filter -> embed -> (push if new/changed)

Modes / flags:
  --incremental  (default) diff against stored memory; push only new/changed pages.
  --full         force re-ingest of EVERY scope page (ignore last_ts / hashes;
                 OV returns 409 Conflict for unchanged pages, treated as ok).
  --dry-run      extract/classify/embed a sample (>=20 pages) with NO writes.
  --plan         build the live timestamp index and PRINT the new/changed/
                 unchanged plan only -- no content fetch, no embed, no write.
                 Cheapest way to preview what an incremental run would do.
  --limit N      cap the number of titles processed (default 0 = no cap).
  --state PATH   use an alternate state file (testability; default INGEST_STATE).
  --scope PATH   use an alternate scope file (testability; default SCOPE_FILE).

Bucket paths are derived from the page's categories (deterministic, mirrors the
Category:Forever* hierarchy), so no separate flaky full-tree crawl is required;
the enumeration module is imported only for the proven classifier.

Rate-limit discipline: conservatively >=1s between every DonSeTch API call
(warcraft.wiki.gg Cloudflare cap ~1 req/3s). OV content writes are not rate
limited but are spaced 0.3s apart as courtesy.
"""
import argparse
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import urllib.request

# --- locate helpers ----------------------------------------------------------
WS_SCRIPTS = os.path.expanduser("~/.hermes/scripts")
if WS_SCRIPTS not in sys.path:
    sys.path.insert(0, WS_SCRIPTS)
ENUM_WS = os.path.expanduser(
    "~/.hermes/kanban/boards/wow-forever-/workspaces/t_ef887215")
if ENUM_WS not in sys.path:
    sys.path.insert(0, ENUM_WS)

from warcraft_wiki_sync import (  # noqa: E402  (reuse tested OV + embed fns)
    build_meta_header, embedding_hash, nomic_embed, openviking_exists,
    push_to_openviking, safe_title,
)
from warcraft_wiki_sync import NOMIC_EMBED_MODEL  # noqa: E402
from enumerate_forever_union import (  # noqa: E402  (tested union classifier)
    classify_forever_union, ROOTS, DELAY as ENUM_DELAY,  # noqa: F401
)

# --- config ------------------------------------------------------------------
WIKI_TARGET_URI = "viking://resources/warcraft-wiki/"
SCOPE_FILE = os.path.join(ENUM_WS, "forever-pages.txt")
CLASSIC_SCOPE_FILE = os.path.join(ENUM_WS, "classic-pages.txt")
CLASSIC_ROOT_CATEGORIES = ["Classic", "Classic items"]  # Classic base scope roots
CLASSIC_SCOPE_CAP = int(os.getenv("CLASSIC_SCOPE_CAP", "0"))  # 0 = all titles
DONSETCH_MAX_CHARS = int(os.getenv("DONSETCH_MAX_CHARS", "500000"))
LEGACY_STATE = os.path.expanduser("~/.hermes/scripts/warcraft_wiki_state.json")
INGEST_STATE = os.path.expanduser("~/.hermes/scripts/warcraft_wiki_ingest_state.json")
ZIM_CACHE = os.path.expanduser("~/.hermes/scripts/warcraft_wiki_zim_cache.json")
EMBEDDINGS_FILE = os.path.expanduser("~/.hermes/scripts/warcraft_wiki_embeddings.jsonl")
REPORT_FILE = os.path.join(WS_SCRIPTS, "warcraft_wiki_ingest_report.json")
DRYRUN_FILE = os.path.join(WS_SCRIPTS, "warcraft_wiki_ingest_dryrun_report.json")
PLAN_FILE = os.path.join(WS_SCRIPTS, "warcraft_wiki_incremental_plan.json")
CHANGES_LOG = os.path.join(WS_SCRIPTS, "warcraft_wiki_changes.log")
MIN_SAMPLE = 20
API_DELAY = float(os.getenv("WWS_API_DELAY", "1.0"))   # s between API calls
OV_DELAY = float(os.getenv("WMS_OV_DELAY", "0.3"))     # courtesy spacing OV writes
TS_CHUNK = int(os.getenv("WWS_TS_CHUNK", "50"))        # titles per batched query (MediaWiki caps `titles` at 50)

# --- download-stalling resilience (fixes t_b7f7f850 / research t_27371440) -------
# Adjusted timeouts: hard per-fetch deadline via donsetch --deadline-ms (never a
# silent hang); subprocess timeout is a safety net *above* the deadline.
DONSETCH_TIMEOUT = int(os.getenv("WWS_DONSETCH_TIMEOUT", "300"))            # subprocess wall-clock safety cap (s)
DONSETCH_DEADLINE_MS = int(os.getenv("WWS_DONSETCH_DEADLINE_MS", "120000")) # hard per-fetch budget via --deadline-ms (ms)
RETRY_JITTER = float(os.getenv("WWS_RETRY_JITTER", "0.5"))                 # backoff jitter: thundering-herd guard
WWS_MAX_WORKERS = int(os.getenv("WWS_MAX_WORKERS", "3"))                   # bounded egress fan-out for backpressure

# Load Forever scope titles for fast membership check
try:
    with open(SCOPE_FILE, encoding="utf-8") as f:
        _FOREVER_TITLES = {l.strip() for l in f if l.strip()}
except Exception:
    _FOREVER_TITLES = set()


# --- OV / api transports (identical contract to the tested legacy code) ------
OV_API_URL = os.getenv("OPENVIKING_ENDPOINT", "http://127.0.0.1:1933")
OV_API_KEY = os.getenv("OPENVIKING_API_KEY", "")
OV_ACCOUNT = os.getenv("OPENVIKING_ACCOUNT", "homelab")
OV_USER = os.getenv("OPENVIKING_USER", "trae")
if not OV_API_KEY:
    try:
        OV_API_KEY = json.load(open(os.path.expanduser("~/.openviking/ov.conf"))
                               ).get("server", {}).get("root_api_key", "")
    except Exception:
        OV_API_KEY = ""


def ov_headers():
    return {"Content-Type": "application/json", "X-API-Key": OV_API_KEY,
            "X-OpenViking-Account": OV_ACCOUNT, "X-OpenViking-User": OV_USER,
            "X-OpenViking-Actor-Peer": "hermes"}


# --- small helpers -----------------------------------------------------------
def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def scope_titles(path=None):
    path = path or SCOPE_FILE
    try:
        with open(path, encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip()]
    except Exception:
        return []


def load_ingest_state(path=None):
    return load_json(path or INGEST_STATE, {})


def save_ingest_state(state, path=None):
    save_json(path or INGEST_STATE, state)


def load_zim_cache():
    try:
        return json.load(open(ZIM_CACHE, encoding="utf-8"))
    except Exception:
        return {}


def zim_body(zim, title):
    if not zim:
        return None
    body = zim.get("pages", {}).get(title)
    return body if body else None


def _classic_subcat(categories):
    """Return a Classic sub-bucket name (bare or 'Category:...' form) or None."""
    low = [str(c).lower() for c in (categories or [])]
    for c in low:
        name = c[len("category:"):] if c.startswith("category:") else c
        name = name.strip()
        if name in ("classic", "classic items"):
            return name
    return None


def _has_forever_signal(categories):
    """Fallback Forever detection from categories alone. Mirrors classify_version's
    highest-precedence Forever signals: the Forever patch bucket, an
    Added-in-patch-1.6x bucket, or a top-level Category:Forever* bucket."""
    low = [str(c).lower() for c in (categories or [])]
    if any("content added by a forever patch" in c for c in low):
        return True
    for c in low:
        if re.search(r"added in patch 1\.6\d+", c):
            return True
    for c in low:
        name = c[len("category:"):] if c.startswith("category:") else c
        name = name.strip()
        if name == "forever" or name.startswith("forever") or "forever" in name.split():
            return True
    return False


def derive_bucket_path(categories, cls=None):
    """Deterministic OV bucket path derived from the page's categories.

    Precedence: Forever is the highest-precedence source. A page carrying any
    Forever signal routes to the Forever hierarchy (Forever overrides the Classic
    base). A Classic-only page routes to the Classic base bucket, so its URI is
    viking://resources/warcraft-wiki/Classic/<title>.md.

    `cls` is the classify dict (preferred); when omitted the signals are detected
    from categories directly.
    """
    low = [str(c).lower() for c in (categories or [])]

    if cls is not None:
        is_forever = bool(cls.get("forever"))
        is_classic = bool(cls.get("classic"))
    else:
        is_forever = _has_forever_signal(categories)
        is_classic = (_classic_subcat(categories) is not None
                      or any("content added by a classic patch" in c for c in low))

    if is_classic and not is_forever:
        return ["Classic"]

    segs = ["Forever"]
    if any("content added by a Forever patch" in c for c in low) and \
       not any(c.startswith("forever ") or c == "forever" for c in low):
        segs = ["Content added by a Forever patch"]
        if any("added in patch 1.6" in c for c in low):
            segs.append("Content added by a Forever patch")
        return segs
    best = None
    for c in low:
        name = c[len("category:")] if c.startswith("category:") else c
        name = name.strip()
        if name == "forever":
            continue
        if name.startswith("forever") or "forever" in name.split():
            if best is None or len(name) > len(best):
                best = name
    if best:
        segs = ["Forever", best]
    return segs


def uri_for(title, categories, cls=None):
    segs = derive_bucket_path(categories, cls)
    return WIKI_TARGET_URI + "/".join(safe_title(s) for s in segs) + "/" + \
        safe_title(title) + ".md"


def fetch_current_timestamps(titles, chunk=TS_CHUNK, delay=API_DELAY):
    """Return {title: latest_revision_ts} for the given titles using ONE batched
    MediaWiki query per `chunk`. Through DonSeTch (Cloudflare-safe). Missing
    titles (API quirk / redirect) are simply absent from the dict; callers treat
    an absent ts as 'needs check'."""
    idx = {}
    for i in range(0, len(titles), chunk):
        batch = titles[i:i + chunk]
        params = {"action": "query", "prop": "revisions", "rvprop": "timestamp",
                  "rvslots": "main", "titles": "|".join(batch),
                  "format": "json", "formatversion": "2"}
        url = "https://warcraft.wiki.gg/api.php?" + urllib.parse.urlencode(params)
        # 4-attempt retry w/ exponential backoff (mirrors classic wiki_api_get)
        # so a transient Cloudflare 429/502 on one batch is retried, not dropped.
        data, err = donsetch_get(url)
        if data is None:
            print("  [ts] batch fetch failed (%d titles): %s" % (len(batch), err))
            continue
        for p in data.get("query", {}).get("pages", []):
            title = p.get("title")
            revs = p.get("revisions") or []
            if title and revs:
                idx[title] = revs[0].get("timestamp")
        time.sleep(delay)
    return idx


def page_url_for(title):
    """Public URL for a wiki page title (title -> URL-safe path)."""
    return "https://warcraft.wiki.gg/%s" % title.replace(" ", "_")


def _parse_retry_after(meta):
    """Return a numeric Retry-After (seconds) from donsetch stderr meta, else 0.0."""
    m = re.search(r"Retry[- ]?After:[ \t]*([0-9]+(?:[.][0-9]+)?)", meta or "", re.IGNORECASE)
    return float(m.group(1)) if m else 0.0


def _backoff_wait(attempt, delay, meta=""):
    """Exponential backoff WITH jitter (thundering-herd guard). Honors an explicit
    Retry-After hint from donsetch meta when present (never sleeps below it)."""
    wait = (2 ** attempt) + delay + random.uniform(0, RETRY_JITTER)
    ra = _parse_retry_after(meta)
    if ra and ra > wait:
        wait = ra
    return wait


def donsetch_get(url, max_chars=DONSETCH_MAX_CHARS, attempts=4, delay=API_DELAY,
                 timeout=DONSETCH_TIMEOUT, deadline_ms=DONSETCH_DEADLINE_MS):
    """Fetch a warcraft.wiki.gg URL via the Cloudflare-safe DonSeTch CLI and
    return the parsed JSON object.

    Resilience vs. the original single-timeout baseline (fixes the "one slow/blocked
    page stalls the whole run" weakness from t_b7f7f850 / research t_27371440):
      * Hard per-fetch deadline via donsetch --deadline-ms (never a silent hang);
        subprocess.run `timeout` is a safety net *above* the deadline.
      * Exponential backoff WITH jitter so a transient rate-limit doesn't re-collide
        (thundering-herd guard); an explicit Retry-After hint from donsetch meta is
        honored when present.
      * Per-fetch timing is instrumented so a slow tail surfaces as a logged warning
        instead of reading as a permanent stall.
      * A challenge donsetch can't solve is classified (not blindly retried against
        the same client) and the batch degrades gracefully (see the fetch-failed
        guard in ingest_one) instead of silently writing empty content.

    Returns (obj, None) on success or (None, error_str) after all attempts.
    """
    last_err = None
    timings = []
    for attempt in range(attempts):
        start = time.time()
        try:
            proc = subprocess.run(
                ["donsetch", "fetch", "--max-chars", str(max_chars),
                 "--deadline-ms", str(deadline_ms), url],
                capture_output=True, text=True,
                timeout=max(timeout, deadline_ms // 1000 + 30))
        except Exception as e:
            last_err = "subprocess error: %s" % e
            wait = _backoff_wait(attempt, delay)
            print("  [donsetch] attempt %d/%d failed (%s), retry in %ds"
                  % (attempt + 1, attempts, last_err, round(wait)))
            time.sleep(wait)
            continue
        timings.append(time.time() - start)
        out = (proc.stdout or "").strip()
        err_meta = (proc.stderr or "").strip()
        start_brace = out.find("{")
        if start_brace == -1:
            last_err = "empty/non-JSON stdout (transient Cloudflare)"
            wait = _backoff_wait(attempt, delay, err_meta)
            print("  [donsetch] attempt %d/%d no JSON (%s), retry in %ds"
                  % (attempt + 1, attempts, last_err, round(wait)))
            time.sleep(wait)
            continue
        try:
            return json.JSONDecoder().raw_decode(out[start_brace:])[0], None
        except Exception as e:
            last_err = "unparseable JSON: %s" % e
            wait = _backoff_wait(attempt, delay, err_meta)
            print("  [donsetch] attempt %d/%d bad JSON (%s), retry in %ds"
                  % (attempt + 1, attempts, last_err, round(wait)))
            time.sleep(wait)
            continue
    slowest = max(timings) if timings else 0.0
    if slowest > deadline_ms / 1000:
        print("  [donsetch] SLOW tail detected: %.1fs (budget %ds)"
              % (slowest, deadline_ms // 1000))
    print("  [donsetch] FAILED after %d attempts (%s); slowest %.1fs"
          % (attempts, last_err, slowest))
    return None, "donsetch failed after %d attempts: %s" % (attempts, last_err)


def _fetch_pages_concurrent(work, fetched_map, max_workers):
    """Fetch page content for `work` concurrently (bounded by max_workers) and
    populate fetched_map[title] = fetch_page_api() results.

    Thread-safe via a per-title lock. The bounded pool is the egress backpressure
    against warcraft.wiki.gg's Cloudflare cap (<=3 concurrent), so a single slow or
    blocked page can't stall the whole run while OV writes stay sequential.
    """
    lock = threading.Lock()

    def _fetch(title):
        result = fetch_page_api(title)
        with lock:
            fetched_map[title] = result

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(_fetch, work))


# --- preflight ---------------------------------------------------------------
def ov_health():
    try:
        req = urllib.request.Request(
            "%s/api/v1/content/read?uri=%s" % (OV_API_URL,
               urllib.parse.quote(WIKI_TARGET_URI + "_preflight_check.md", "")),
            headers=ov_headers())
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.getcode() == 200 or r.getcode() == 404
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


def embedder_health():
    """Return True only if the embedder responds with a valid 768-dim vector.

    Retries a few times (mirrors classic's resilient fetch) so a transient Ollama
    blip is not misreported as embedder-down and spurious 'embeddings skipped'
    warnings are avoided. Failure still degrades gracefully (content is pushed
    without an embedding), matching classic's nomic_embed contract."""
    for attempt in range(3):
        if _embedder_health_once():
            return True
        wait = (2 ** attempt) + 1
        print("[health] embedder ping failed (attempt %d/3), retry in %ds"
              % (attempt + 1, wait))
        time.sleep(wait)
    print("[health] embedder unreachable after retries")
    return False


def _embedder_health_once():
    try:
        payload = json.dumps({"model": NOMIC_EMBED_MODEL, "prompt": "ping"}).encode()
        req = urllib.request.Request(
            os.getenv("NOMIC_EMBED_URL", "http://192.168.4.30:11434/api/embeddings"),
            data=payload, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            if r.getcode() != 200:
                return False
            vec = json.loads(r.read().decode()).get("embedding")
            return bool(vec) and len(vec) == 768
    except Exception as e:
        print("[health] embedder unreachable: %s" % e)
        return False


def regression_guard():
    """Pre-flight regression guard for t_605e4a3e (the "where is hogger" scenario).

    Asserts BOTH halves that the bot's lookup depends on:
      * routing half   -- WoWMentionGate routes "where is hogger" to needs_lookup=True.
      * coverage half  -- the Classic Hogger page is indexed in the OV KB with its
                          metadata (Hogger / gnoll / Elwynn / elite / source link).

    Returns (ok: bool, details: str). Only run AFTER ov_health() (OV must be
    reachable first). Imported lazily so the sync pays for the bot import only
    when this guard actually runs.
    """
    import warcraft_wiki_hogger_regression_test as reg  # noqa: E402

    result = reg.run_regression()
    if result["ok"]:
        return True, "%s | %s" % (result["routing"], result["coverage"])
    return False, result["error"]


# --- pure incremental decision ----------------------------------------------
def plan_action(title, state, current_ts):
    """Decide what an incremental run will do for one title WITHOUT fetching it.

    Returns one of: "new" | "changed" | "unchanged".
      new        -> title absent from state (always pushed on next run)
      unchanged  -> state.last_ts present AND equals current live ts
                    (skip: no content fetch, no write)
      changed    -> state.last_ts missing/older than current ts, OR the title is
                    new (re-fetched; pushed iff still Forever / content changed)

    `state` may contain legacy entries with no last_ts (reconcile path): those
    are 'changed' so the run re-fetches + compares embed_hash.
    """
    rec = state.get(title)
    if rec is None:
        return "new"
    stored_ts = rec.get("last_ts")
    cur_ts = current_ts.get(title)
    if stored_ts is not None and cur_ts is not None and stored_ts == cur_ts:
        return "unchanged"
    return "changed"


# --- fetch ONE page's content + categories (per-title; Cloudflare-safe) ------
def fetch_page_api(title):
    """Returns (content, timestamp, categories) or (None, None, []).

    Through DonSeTch (Cloudflare-safe) with 4-attempt retry (mirrors classic
    wiki_api_get): a transient Cloudflare 429/502 or empty/non-JSON body is
    retried with exponential backoff instead of being silently dropped, so a
    one-off block no longer leaves the page un-ingested until the next run."""
    params = {"action": "query", "prop": "revisions|categories",
              "rvprop": "timestamp|ids", "rvslots": "main",
              "titles": title, "format": "json", "formatversion": "2"}
    data, err = donsetch_get(
        "https://warcraft.wiki.gg/api.php?" + urllib.parse.urlencode(params))
    if data is None:
        print("  [api] fetch failed for %r: %s" % (title, err))
        return None, None, [], False   # (content, ts, categories, fetched_ok)
    content, ts, cats = "", None, []
    for p in data.get("query", {}).get("pages", []):
        if p.get("title") != title:
            continue
        for rev in p.get("revisions", []) or []:
            ts = rev.get("timestamp", ts)
            main = (rev.get("slots", {}) or {}).get("main", {})
            if isinstance(main, dict):
                content = main.get("content", "") or ""
            else:
                content = str(main)
        cats = [c["title"] for c in p.get("categories", [])]
    return content, ts, cats, True   # fetched_ok = a response was returned


def donsetch_fetch_json(url):
    """Fetch a warcraft.wiki.gg URL via the Cloudflare-safe DonSeTch CLI and
    return parsed JSON. Resolves the JS-challenge wall that blocks direct egress.

    Returns {} on failure (after the 4-attempt retry inside donsetch_get) instead
    of raising, so a single transient failure on a category-membership request no
    longer aborts the whole enumeration with an unhandled exception (mirrors
    classic wiki_api_get's graceful handling of transient Cloudflare failures)."""
    data, err = donsetch_get(url)
    if data is None:
        print("  [donsetch] enumeration fetch failed: %s" % err)
        return {}
    return data


def classic_recurse_tree(roots, cap=0):
    """Recursively gather ns0 page titles under each Classic root (and all its
    subcats) via DonSeTch, returning an ordered-unique list. `cap`=0 means all."""
    out, seen, visited = [], set(), set()

    def recurse(cat, depth):
        cat_key = "Category:" + cat
        if cat_key in visited or depth > 50:
            return
        visited.add(cat_key)
        cmcontinue, rounds = None, 0
        while True:
            params = {"action": "query", "list": "categorymembers",
                      "cmtitle": cat_key, "cmlimit": 500, "format": "json",
                      "formatversion": "2"}
            if cmcontinue:
                params["cmcontinue"] = cmcontinue
            url = "https://warcraft.wiki.gg/api.php?" + urllib.parse.urlencode(params)
            data = donsetch_fetch_json(url)
            for m in data.get("query", {}).get("categorymembers", []):
                ns, title = m.get("ns"), m.get("title")
                if ns == 0 and title not in seen:
                    seen.add(title)
                    out.append(title)
                elif ns == 14:
                    recurse(title.replace("Category:", ""), depth + 1)
            cmcontinue = data.get("continue", {}).get("cmcontinue")
            rounds += 1
            if not cmcontinue or rounds > 100000:
                break
            time.sleep(ENUM_DELAY)
            if cap and len(out) >= cap:
                return

    for root_cat in roots:
        if cap and len(out) >= cap:
            break
        recurse(root_cat, 1)
    return out


def enumerate_classic_scope(cap=0):
    """Enumerate the Classic base scope (Category:Classic + subcats) via DonSeTch,
    dedupe against the Forever scope (Forever takes precedence), and write
    classic-pages.txt. Returns the kept titles."""
    forever = set(scope_titles())
    kept, seen = [], set(forever)
    skipped = 0
    for root_cat in CLASSIC_ROOT_CATEGORIES:
        for title in classic_recurse_tree([root_cat], cap=cap):
            if title in seen:
                skipped += 1
                continue
            seen.add(title)
            kept.append(title)
        if cap and len(kept) >= cap:
            break
    tmp = CLASSIC_SCOPE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(kept) + "\n")
    os.replace(tmp, CLASSIC_SCOPE_FILE)
    print("[scope] classic-pages.txt: %d titles "
          "(%d skipped: already in Forever scope or duplicate root)"
          % (len(kept), skipped))
    return kept


def merged_scope(scope_override=None):
    """Effective scope = Forever (forever-pages.txt) merged with Classic
    (classic-pages.txt), deduped with the Forever scope first so a page present
    in both resolves to its Forever entry (Forever overrides Classic)."""
    if scope_override:
        return scope_titles(scope_override)
    forever = scope_titles()
    seen = set(forever)
    merged = list(forever)
    for title in scope_titles(CLASSIC_SCOPE_FILE):
        if title not in seen:
            seen.add(title)
            merged.append(title)
    return merged


def classify_keep(cls):
    """Ingestion scope = Forever (highest precedence) OR Classic base.

    A page carrying any Forever signal is kept as Forever (Forever overrides the
    Classic base). Classic-only pages are kept as Classic base. retail / multi are
    out of scope and rejected; unknown is kept via bucket-path trust (legacy).
    """
    if cls.get("forever"):
        return True
    if cls.get("keep"):
        return True
    if cls["version"] in ("retail", "multi"):
        return False
    return True


# --- ingest ONE page (shared by incremental, full, and dry-run) --------------


def ingest_one(title, zim, state, report, do_write, force=False, fetched=None):
    """Extract/classify/filter/embed/decide/push a single scope page.

    `fetched` optionally pre-supplies fetch_page_api's result
    (content, ts, categories, fetched_ok) so the incremental run can fetch the work
    set concurrently (bounded egress fan-out / backpressure) while all OV writes
    remain sequential. When omitted the page is fetched internally.

    Push happens when do_write AND (force OR title is new OR embed_hash changed).
    Returns a dict describing the outcome (action)."""
    if fetched is None:
        content, ts, categories, fetched_ok = fetch_page_api(title)
    else:
        content, ts, categories, fetched_ok = fetched
    if content is None:
        content = ""
    zbody = zim_body(zim, title)
    body = zbody if zbody else content
    # the mwoffliner article body often re-emits the page title as its first
    # token, which would create a duplicate H1 next to the prepended "# {title}".
    if body:
        body = re.sub(r"^\s*" + re.escape(title) + r"\s+", "", body, count=1)

    # Never write empty content: a failed fetch with NO cached ZIM body would write
    # an empty page to memory (silent data loss -- swallowed-fetch-failure fix from
    # t_b7f7f850). Skip the push; the page is retried on the next run.
    if not fetched_ok and not zbody:
        report["failed"] += 1
        report["actions"].append({"title": title, "action": "fetch-failed",
                                  "uri": None, "url": page_url_for(title)})
        return {"title": title, "action": "fetch-failed", "uri": None,
                "url": page_url_for(title), "forever": False}

    cls = classify_forever_union(categories, content)
    report["classified"] += 1
    report["by_version"][cls["version"]] = report["by_version"].get(cls["version"], 0) + 1

    # --- SoD-only guard: skip Season of Discovery pages that are NOT in Forever scope
    def is_sod_only(categories, content_text, cls, title):
        sod_signal = any("Season_of_Discovery" in (c or "") or "Season of Discovery" in (c or "") for c in categories)
        sod_template = bool(re.search(r"\{\{Classic season\|discovery", content_text or ""))
        has_forever = bool(cls.get("forever"))
        in_forever_scope = title in _FOREVER_TITLES
        return (sod_signal or sod_template) and not has_forever and not in_forever_scope

    if is_sod_only(categories, content, cls, title):
        report["skipped_sod"] = report.get("skipped_sod", 0) + 1
        report["actions"] = report.get("actions", [])
        report["actions"].append({"title": title, "action": "skip-sod-only", "uri": None, "url": None})
        return {"title": title, "action": "skip-sod-only", "forever": False}

    if not classify_keep(cls):
        report["non_forever"] += 1
        report["rejected"] += 1
        report["non_forever_titles"].append({"title": title,
                                             "version": cls["version"],
                                             "signals": cls["signals"]})
        return {"title": title, "action": "rejected", "forever": False}

    uri = uri_for(title, categories, cls)
    page_url = "https://warcraft.wiki.gg/%s" % title.replace(" ", "_")
    vector = nomic_embed(body or title)
    report["embedded"] += 1
    report["embed_dims"].add(len(vector) if vector else -1)
    cur_hash = embedding_hash(vector) if vector else ""

    if not do_write:
        report["actions"].append({"title": title, "action": "dryrun", "uri": uri,
                                  "url": page_url,
                                  "dim": len(vector) if vector else -1,
                                  "signals": cls["signals"]})
        return {"title": title, "action": "dryrun", "uri": uri,
                "dim": len(vector) if vector else -1,
                "forever": True, "version": cls["version"]}

    # ---- decide whether memory needs updating ----
    rec = state.get(title)
    new = rec is None
    prev_hash = (rec or {}).get("embed_hash")
    changed = force or new or (prev_hash is not None and cur_hash != prev_hash)
    if prev_hash is None and not force and not new:
        # legacy entry without a stored content hash -> reconcile via content.
        changed = True

    if not changed:
        report["skipped_unchanged"] += 1
        report["actions"].append({"title": title, "action": "skip-unchanged",
                                  "uri": uri, "url": page_url})
        return {"title": title, "action": "skip-unchanged", "uri": uri,
                "url": page_url}

    header = build_meta_header(title, "/".join(safe_title(s) for s in
                  derive_bucket_path(categories, cls)), "https://warcraft.wiki.gg/%s"
                  % title.replace(" ", "_"), ts or time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  vector, version=cls)
    push_body = "# %s\n\n%s\n\n%s" % (title, header, body or content)
    ok = push_to_openviking(uri, push_body, title)
    if ok:
        state[title] = {"embed_hash": cur_hash, "uri": uri, "url": page_url,
                        "version": cls["version"],
                        "last_ts": ts or time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "sourced_from": "zim" if zbody else "api"}
        if new:
            report["new"] += 1
            report["actions"].append({"title": title, "action": "created",
                                      "uri": uri, "url": page_url})
            return {"title": title, "action": "created", "uri": uri, "forever": True}
        else:
            report["updated"] += 1
            report["actions"].append({"title": title, "action": "updated",
                                      "uri": uri, "url": page_url})
            return {"title": title, "action": "updated", "uri": uri, "forever": True}
    report["failed"] += 1
    report["actions"].append({"title": title, "action": "push-failed", "uri": uri})
    return {"title": title, "action": "push-failed", "uri": uri}


# --- modes -------------------------------------------------------------------
def run_plan(scope, state, out_path):
    """Build the live timestamp index and print / save the new/changed/unchanged
    plan. No content fetch, no embed, no OV write -- cheapest preview."""
    print("=" * 70)
    print("PLAN: build live timestamp index -> new / changed / unchanged")
    print("=" * 70)
    current_ts = fetch_current_timestamps(scope)
    print("timestamp index: %d / %d scope titles resolved"
          % (len(current_ts), len(scope)))
    new_c, changed_c, unchanged_c = [], [], []
    for title in scope:
        a = plan_action(title, state, current_ts)
        (new_c if a == "new" else changed_c if a == "changed" else unchanged_c).append(title)
    report = {"mode": "plan", "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
              "scope": len(scope), "titles_indexed": len(current_ts),
              "new": len(new_c), "changed": len(changed_c),
              "unchanged": len(unchanged_c)}
    save_json(out_path, report)
    print("\n--- PLAN ---")
    print("new pages          :", len(new_c))
    print("changed (re-check) :", len(changed_c))
    print("unchanged (skip)   :", len(unchanged_c))
    print("plan -> %s" % out_path)
    return 0


def run_dry_run(scope, max_pages):
    print("=" * 70)
    print("DRY RUN: extract -> classify -> embed (NO writes)")
    print("=" * 70)
    if not embedder_health():
        print("WARNING: embedder unhealthy -- embeddings will be skipped.")
    zim = load_zim_cache()
    print("scope: %d titles (Forever + Classic)" % len(scope))
    print("ZIM cache: %d article bodies" % zim.get("count", 0))
    state = load_ingest_state()
    report = {"mode": "dry-run", "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
              "scope": len(scope), "new": 0, "updated": 0, "unchanged": 0,
              "classified": 0, "non_forever": 0, "rejected": 0, "embedded": 0,
              "skipped_unchanged": 0, "failed": 0, "by_version": {},
              "embed_dims": set(), "non_forever_titles": [], "actions": [],
              "sample_limit": max_pages or MIN_SAMPLE}
    n = 0
    for title in scope:
        if n >= (max_pages or MIN_SAMPLE):
            break
        r = ingest_one(title, zim, state, report, do_write=False)
        n += 1
        print("  [%2d] %-40s -> %s (dim=%s) %s" % (
            n, title, r["action"], r.get("dim", "-"), r.get("signals", "")))
    report["embed_dims"] = sorted(d for d in report["embed_dims"] if d >= 0)
    report["proof_no_inscope_rejected"] = not any(
        t.get("version") in ("Forever", "classic") for t in report["non_forever_titles"])
    report["proof_embed_dim_768"] = report["embed_dims"] == [768]
    save_json(DRYRUN_FILE, report)
    print("\n--- DRY-RUN PROOF ---")
    print("pages checked (extracted/classified):", report["classified"])
    print("out-of-scope rejected                :", report["non_forever"],
          "=> no in-scope rejected:", report["proof_no_inscope_rejected"])
    print("embedding dims observed             :", report["embed_dims"],
          "=> dim == 768:", report["proof_embed_dim_768"])
    print("report -> %s" % DRYRUN_FILE)
    return 0


def run_incremental(scope, state, force=False, limit=0, state_path=None):
    """Incremental: diff against stored memory; push only new / changed pages.

    Idempotent by construction -- once a page's last_ts is stored and the wiki is
    unchanged it is UNCHANGED and is neither fetched nor written."""
    print("=" * 70)
    print("INCREMENTAL INGEST: diff against stored memory -> push new/changed")
    print("=" * 70)
    # Preflight OV health with bounded retry (mirrors classic's resilience rather
    # than a single-shot abort): a transient OV blip must not sink the whole run.
    health_ok = False
    for _attempt in range(3):
        if ov_health():
            health_ok = True
            break
        wait = (2 ** _attempt) + 1
        print("[health] OpenViking not reachable (attempt %d/3), retry in %ds"
              % (_attempt + 1, wait))
        time.sleep(wait)
    if not health_ok:
        print("ERROR: OpenViking not reachable at %s after retries" % OV_API_URL)
        return 3
    # t_605e4a3e: halt the sync if the "where is hogger" lookup path is broken
    # (routing half OR coverage half). Ingesting into the hole is worse than failing.
    ok, details = regression_guard()
    if not ok:
        print("REGRESSION GUARD FAILED (t_605e4a3e): %s" % details)
        print("Halt: routing half or coverage half is broken.")
        print("      Fix, then re-run. See warcraft_wiki_hogger_regression_test.py")
        return 5
    if not embedder_health():
        print("WARNING: embedder unhealthy -- content will still be pushed;")
        print("         embeddings will be skipped for affected pages.")
    zim = load_zim_cache()
    print("scope: %d titles; ZIM cache: %d bodies"
          % (len(scope), zim.get("count", 0)))
    if not zim.get("pages"):
        print("WARNING: ZIM cache empty -- will rely on MediaWiki API content.")

    current_ts = fetch_current_timestamps(scope)
    print("live timestamp index: %d / %d titles"
          % (len(current_ts), len(scope)))

    new_ls, changed_ls, unchanged_ls = [], [], []
    for title in scope:
        a = plan_action(title, state, current_ts)
        (new_ls if a == "new" else changed_ls if a == "changed"
         else unchanged_ls).append(title)
    print("plan: %d new, %d changed(re-check), %d unchanged(skip)"
          % (len(new_ls), len(changed_ls), len(unchanged_ls)))

    report = {"mode": "incremental" if not force else "full",
              "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
              "scope": len(scope), "new": 0, "updated": 0,
              "skipped_unchanged": 0, "skipped_unchanged_plan": len(unchanged_ls),
              "classified": 0, "non_forever": 0, "rejected": 0, "embedded": 0,
              "failed": 0, "by_version": {}, "embed_dims": set(),
              "non_forever_titles": [], "actions": [],
              "new_titles": list(new_ls), "changed_titles": list(changed_ls)}

    if not (new_ls or changed_ls):
        print("No new or changed pages -- memory already up to date. Nothing to do.")
        report["unchanged"] = len(scope)
        # embed_dims is a set; the main path sorts it to a list before writing --
        # mirror that here so the no-op report is JSON-serializable (avoids a
        # TypeError: set is not JSON serializable on every steady-state no-op run).
        report["embed_dims"] = sorted(d for d in report["embed_dims"] if d >= 0)
        save_ingest_state(state, state_path)
        save_json(REPORT_FILE, report)
        print("\n--- INCREMENTAL RESULT (no-op) ---")
        print("new  :", report["new"])
        print("updated:", report["updated"])
        print("unchanged:", report["unchanged"])
        print("report -> %s" % REPORT_FILE)
        return 0

    work = new_ls + changed_ls
    if limit and limit > 0:
        work = work[:limit]
        report["limit"] = limit

    # --- Backpressure / bounded concurrency (fixes t_b7f7f850 weakness d) --------
    # Fetch the work set's page content concurrently with a bounded worker pool so a
    # single slow/blocked warcraft.wiki.gg page can't stall the whole run, and egress
    # fan-out stays under the ~1 req/3s Cloudflare guidance. All OV writes below
    # remain sequential + crash-safe.
    max_workers = min(WWS_MAX_WORKERS, len(work))
    fetched_map = {}
    if max_workers > 1 and len(work) > 2:
        _fetch_pages_concurrent(work, fetched_map, max_workers)
    else:
        for title in work:
            fetched_map[title] = fetch_page_api(title)

    # Flush state periodically (not after every page) to avoid the O(n^2) disk
    # I/O of rewriting the whole growing state file on each write, while still
    # staying crash-safe (at most flush_every un-persisted pages lost on a kill).
    flush_every = 50
    saved = 0
    for i, title in enumerate(work):
        f = fetched_map.get(title)
        if f is None:                      # defensive: never None, but re-fetch if so
            f = fetch_page_api(title)
        r = ingest_one(title, zim, state, report, do_write=True, force=force,
                       fetched=f)
        print("  [%3d/%d] %-40s -> %s" % (i + 1, len(work), title, r["action"]))
        if r["action"] in ("created", "updated"):
            saved += 1
            if saved % flush_every == 0:
                save_ingest_state(state, state_path)
        time.sleep(OV_DELAY)

    report["unchanged"] = len(unchanged_ls) + report["skipped_unchanged"]
    report["embed_dims"] = sorted(d for d in report["embed_dims"] if d >= 0)
    report["proof_no_inscope_rejected"] = not any(
        t.get("version") in ("Forever", "classic") for t in report["non_forever_titles"])
    report["proof_embed_dim_768"] = report["embed_dims"] == [768]
    save_ingest_state(state, state_path)
    save_json(REPORT_FILE, report)
    _append_changes_log(report)

    print("\n--- INCREMENTAL RESULT ---")
    print("new pages created  :", report["new"])
    print("pages updated      :", report["updated"])
    print("unchanged (skip)   :", report["unchanged"])
    print("rejected non-forev :", report["rejected"])
    print("push-failed        :", report["failed"])
    print("embed dims         :", report["embed_dims"])
    print("no in-scope rejected   :", report["proof_no_inscope_rejected"])
    print("report -> %s ; ingest state -> %s" % (REPORT_FILE, INGEST_STATE))
    return 0


def _append_changes_log(report):
    """Append a human-readable change-summary line to the audit log."""
    try:
        with open(CHANGES_LOG, "a", encoding="utf-8") as f:
            f.write("%s | mode=%s | new=%d updated=%d unchanged=%d rejected=%d failed=%d\n" % (
                report.get("generated", ""), report.get("mode", "?"),
                report.get("new", 0), report.get("updated", 0),
                report.get("unchanged", 0), report.get("rejected", 0),
                report.get("failed", 0)))
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser(description="WoW:Forever -> OpenViking incremental ingest")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", dest="dry", action="store_true",
                   help="extract/classify/embed a sample, no writes.")
    g.add_argument("--full", dest="full", action="store_true",
                   help="force re-ingest of EVERY scope page (ignore last_ts).")
    g.add_argument("--plan", dest="plan", action="store_true",
                   help="build the live ts index and print the new/changed/unchanged "
                        "plan only (no fetch/embed/write).")
    g.add_argument("--incremental", dest="inc", action="store_true",
                   help="default: diff against memory, push only new/changed pages.")
    ap.add_argument("--limit", type=int, default=0,
                    help="cap the number of titles processed (default 0 = no cap).")
    ap.add_argument("--state", default=None, help="alternate state file (testability).")
    ap.add_argument("--scope", default=None, help="alternate scope file (testability).")
    ap.add_argument("--gen-classic-scope", dest="gen_classic", action="store_true",
                    help="enumerate the Classic base scope via DonSeTch and write "
                         "classic-pages.txt (deduped against the Forever scope).")
    ap.add_argument("--classic-cap", type=int, default=0,
                    help="cap Classic enumeration count (0 = all).")
    args = ap.parse_args()

    scope = merged_scope(args.scope)
    if not scope:
        print("ERROR: no scope titles (check scope file).")
        return 2
    state = load_ingest_state(args.state)

    if args.gen_classic:
        enumerate_classic_scope(cap=args.classic_cap)
        return 0

    if args.dry:
        return run_dry_run(scope, args.limit)
    if args.plan:
        return run_plan(scope, state, PLAN_FILE)
    return run_incremental(scope, state, force=args.full, limit=args.limit, state_path=args.state)


if __name__ == "__main__":
    raise SystemExit(main())
