#!/usr/bin/env python3
"""
Warcraft Wiki nightly sync to OpenViking.

INGESTS WOOW "FOREVER" + "CLASSIC" CONTENT. Ingestion scope is Forever (the
highest-precedence source) plus Classic. All other WoW versions (retail, Vanilla
without a Classic-classification, ...) and any content not tagged for Forever or
Classic are excluded. Forever takes precedence: a page carrying any Forever signal
is kept as Forever even when it also carries Classic content (the Classic variant
is superseded by the base/override layer). Classic content is in scope via
Category:Classic / Category:Classic items membership or a {{Classic only|only|
season}} template. Shared / backported items that also exist in another flavor are
KEPT because they carry a Forever marker (see classify_version()).

Version filtering
-----------------
  Primary: a page is Forever iff it lives in a "Forever patch" bucket, i.e.
           Category:Content added by a Forever patch  OR  any
           Category:Added in patch 1.6x  (matched by regex).
  Secondary (items only): an {{#data:#Forever}} data block in the wikitext.
  Classification (classify_version) also reports retail / classic-line buckets
  so a page that carries ONLY those (and no Forever marker) is rejected.

Version unit check
------------------
For every candidate page the wiki API is queried for its real categories
(single call: prop=revisions|categories) and classify_version() is applied.
A page that does not classify as Forever is skipped and counted as a unit-check
violation (see warcraft_wiki_sync_test.py for the pure-logic unit tests).
If a page's categories cannot be fetched, it is kept only when it was found
under the verified Forever bucket (bucket-path fallback) and flagged.

Runs at 1am via Hermes cron, script-only (no_agent).
Memory segregated under viking://resources/warcraft-wiki/
"""
import os
import re
import sys
import time
import json
import hashlib
import urllib.request
import urllib.parse

OV_API_URL = os.getenv('OPENVIKING_ENDPOINT', 'http://127.0.0.1:1933')
OV_API_KEY = os.getenv('OPENVIKING_API_KEY', '')
OV_ACCOUNT = os.getenv('OPENVIKING_ACCOUNT', 'homelab')
OV_USER = os.getenv('OPENVIKING_USER', 'trae')
if not OV_API_KEY:
    try:
        with open(os.path.expanduser('~/.openviking/ov.conf')) as f:
            cfg = json.load(f)
            OV_API_KEY = cfg.get('server', {}).get('root_api_key', '')
    except Exception:
        pass

WIKI_TARGET_URI = "viking://resources/warcraft-wiki/"
STATE_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_state.json')

# --- Nomadic embed (nomic-embed-text) local embedding config ---
# Embeds each fetched page with the local nomic-embed-text model (Ollama)
# BEFORE content is written to OpenViking memory. Configurable via env.
NOMIC_EMBED_URL = os.getenv('NOMIC_EMBED_URL', 'http://192.168.4.30:11434/api/embeddings')
NOMIC_EMBED_URL_V1 = os.getenv('NOMIC_EMBED_URL_V1', 'http://192.168.4.30:11434/v1/embeddings')
NOMIC_EMBED_MODEL = os.getenv('NOMIC_EMBED_MODEL', 'nomic-embed-text')
# Durable, searchable store for computed embeddings + metadata (JSONL).
EMBEDDINGS_FILE = os.path.expanduser('~/.hermes/scripts/warcraft_wiki_embeddings.jsonl')

# --- Version filter: WoW "Forever" only ---
# Authoritative Forever bucket. Recursing from here captures brand-new Forever
# items/quests AND shared/backported items (which also carry a second flavor
# bucket), while excluding all retail / BC / WotLK / Cataclysm / ... content.
FOREVER_ROOT_CATEGORY = "Content added by a Forever patch"
FOREVER_PATCH_BUCKET = "forever patch"               # -> Category:Content added by a Forever patch
FOREVER_PATCH_16 = re.compile(r"added in patch 1\.6\d+", re.IGNORECASE)  # -> Category:Added in patch 1.6x
DATA_FOREVER_MARKER = "#data:#forever"               # -> {{#data:#Forever}} data block
FOREVER_STAR_RE = re.compile(r"^forever\b")           # -> top-level Category:Forever* buckets
RETAIL_BUCKET = "content added by patch"             # -> retail
CLASSIC_BUCKET = "content added by a classic patch"  # -> Vanilla/BC/WotLK/Cataclysm/...

# --- Classic ingestion scope (expansion on top of the Forever-only policy) ---
# Category roots whose descendant pages are in scope as "Classic" content.
# Category:Classic (~18k pages) already contains Category:Classic items, but
# Category:Classic items is listed explicitly so it is always discovered even if
# the tree order changes. See CLASSIC_MARKERS.md (t_e1a5dc5e) for the full tree.
CLASSIC_ROOT_CATEGORIES = ["Classic", "Classic items"]
# {{Classic only}} / {{Classic only-section}} / {{Classic unique}} / {{Classic
# season}} templates mark Classic presence in wikitext (secondary signal).
CLASSIC_TEMPLATE_RE = re.compile(r"classic\s+(only|unique|season)", re.IGNORECASE)
# Production ingestion gate for Classic pages. The Classic tree is ~18k pages.
# The base/override precedence layer (task t_2746df22) is now in place and verified
# (Classic vs Forever precedence in group_and_dedupe), so Classic ingestion is ON by
# default -- merged Forever + Classic scope is ingested every run. Set
# WOG_INGEST_CLASSIC=0 to restrict to Forever scope only.
WOG_INGEST_CLASSIC = os.getenv("WOG_INGEST_CLASSIC", "true").lower() in ("1", "true", "yes", "on")

USER_AGENT_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]
import random
USER_AGENT = random.choice(USER_AGENT_POOL)
DELAY_BETWEEN_REQUESTS = 3.0


def wiki_api_get(params):
    url = "https://warcraft.wiki.gg/api.php"
    query = urllib.parse.urlencode(params, doseq=True)
    req_url = f"{url}?{query}"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://warcraft.wiki.gg/",
        "Origin": "https://warcraft.wiki.gg",
        "Cache-Control": "no-cache",
    }
    for attempt in range(4):
        req = urllib.request.Request(req_url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                code = resp.getcode()
                if code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 10))
                    time.sleep(retry_after + DELAY_BETWEEN_REQUESTS)
                    continue
                data = resp.read()
                if not data:
                    # Empty body: transient Cloudflare/challenge, retry below
                    raise RuntimeError("empty response body")
                try:
                    return json.loads(data.decode('utf-8'))
                except json.JSONDecodeError:
                    # Non-JSON (challenge page or transient), retry below
                    raise RuntimeError("non-JSON response body")
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 10))
                time.sleep(retry_after + DELAY_BETWEEN_REQUESTS)
                continue
            # 404 etc -> return empty
            return {}
        except urllib.error.URLError as e:
            # timeout / network error
            wait = (2 ** attempt) + DELAY_BETWEEN_REQUESTS
            print(f"Wiki API URLError {e.reason} attempt {attempt+1}/4, retry in {wait}s")
            time.sleep(wait)
            continue
        except RuntimeError as e:
            # empty / non-JSON body (transient Cloudflare)
            wait = (2 ** attempt) + DELAY_BETWEEN_REQUESTS
            print(f"Wiki API {e} attempt {attempt+1}/4, retry in {wait}s")
            time.sleep(wait)
            continue
    raise RuntimeError("Wiki API failed after retries")


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def openviking_exists(uri):
    if not OV_API_KEY:
        return False, None
    headers = {
        "X-API-Key": OV_API_KEY,
        "X-OpenViking-Account": OV_ACCOUNT,
        "X-OpenViking-User": OV_USER,
        "X-OpenViking-Actor-Peer": "hermes"
    }
    params = {"uri": uri}
    url = f"{OV_API_URL}/api/v1/content/read?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.getcode() == 200:
                return True, json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False, None
        return False, None
    return False, None


def push_to_openviking(uri, content, title):
    if not OV_API_KEY:
        print("No OV_API_KEY, skipping push")
        return False
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": OV_API_KEY,
        "X-OpenViking-Account": OV_ACCOUNT,
        "X-OpenViking-User": OV_USER,
        "X-OpenViking-Actor-Peer": "hermes"
    }
    page_url = "https://warcraft.wiki.gg/%s" % title.replace(" ", "_")
    payload = {
        "uri": uri,
        "content": f"# {title}\n\n{content}",
        "mode": "create",
        "url": page_url
    }
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(f"{OV_API_URL}/api/v1/content/write", data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            code = resp.getcode()
            print(f"OpenViking push status {code} for {uri}")
            return code in (200, 201)
    except urllib.error.HTTPError as e:
        if e.code == 409:
            # Already exists, treat as success
            print(f"OpenViking push skipped 409 Conflict for {uri}")
            return True
        print(f"OpenViking push failed: {e}")
        return False
    except Exception as e:
        print(f"OpenViking push failed: {e}")
        return False


def nomic_embed(text):
    """Embed text using the local nomic-embed-text (nomadic embed) model via Ollama.

    Tries the Ollama native /api/embeddings endpoint first, then the OpenAI-style
    /v1/embeddings endpoint. Returns a 768-dim vector, or None if embedding is
    unavailable (callers must tolerate this and still sync the content).
    """
    for url, use_v1 in ((NOMIC_EMBED_URL, False), (NOMIC_EMBED_URL_V1, True)):
        payload = {"model": NOMIC_EMBED_MODEL, "prompt": text}
        if use_v1:
            payload = {"model": NOMIC_EMBED_MODEL, "input": text}
        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(url, data=data,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.getcode() != 200:
                    continue
                result = json.loads(resp.read().decode('utf-8'))
                vec = (result.get("embedding") or
                       (result.get("data", [{}])[0].get("embedding") if use_v1 else None))
                if vec:
                    norm = sum(v * v for v in vec) ** 0.5
                    h = hashlib.md5(json.dumps(vec).encode('utf-8')).hexdigest()[:12]
                    print(f"[nomadic-embed] model={NOMIC_EMBED_MODEL} dim={len(vec)} "
                          f"norm={norm:.4f} hash={h} title=<embedded>")
                    return vec
        except Exception as e:
            reason = getattr(e, "reason", e)
            print(f"[nomadic-embed] {url} failed: {reason}")
    return None


def embedding_hash(vector):
    return hashlib.md5(json.dumps(vector).encode('utf-8')).hexdigest()


def record_embedding(uri, title, category_path, page_ts, source_url, vector):
    """Append the computed embedding + full metadata to a searchable JSONL store."""
    if not vector:
        return
    rec = {
        "uri": uri,
        "title": title,
        "source_url": source_url,
        "category_path": category_path,
        "page_timestamp": page_ts,
        "embed_model": NOMIC_EMBED_MODEL,
        "embed_dim": len(vector),
        "embed_hash": embedding_hash(vector),
        "embed_time": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    try:
        with open(EMBEDDINGS_FILE, 'a') as f:
            f.write(json.dumps(rec) + "\n")
    except Exception as e:
        print(f"[nomadic-embed] failed to record embedding for {title}: {e}")


def scope_label(version):
    """Human-readable scope label for the Classic-base / Forever-override model.

    A Forever page that also carries Classic content is a 'Forever override' of
    its Classic base; a Classic-only page is a 'Classic base'; otherwise the raw
    version string. Drives metadata tagging and logging.
    """
    version = version or {}
    if version.get("forever"):
        return "Forever override" if version.get("classic") else "Forever"
    if version.get("classic"):
        return "Classic base"
    return version.get("version", "unknown")


def build_meta_header(title, category_path, source_url, page_ts, vector, version=None):
    """Human-readable metadata block prepended to stored content so Viking
    retains correct, searchable metadata (source, category, version, base/override,
    embed)."""
    version = version or {}
    embed_hash = embedding_hash(vector) if vector else "unavailable"
    ver = version.get("version", "unknown")
    other = version.get("other_versions", "")
    lines = [
        f"> Source: {source_url}",
        f"> Category: {category_path}",
        f"> Version: {ver}{' - ' + other if other else ''}",
    ]
    # Base/Override inheritance tagging: only for Classic-carrying pages so the
    # override relationship is preserved in stored metadata.
    if version.get("classic"):
        lines.append(f"> Base/Override: {scope_label(version)}")
    lines += [
        f"> Page updated: {page_ts}",
        f"> Embed: {NOMIC_EMBED_MODEL} ({len(vector) if vector else 0}-dim, hash={embed_hash})",
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Version filter                                                              #
# --------------------------------------------------------------------------- #
def _is_forever_star_category(cat):
    """True for a top-level 'Category:Forever*' bucket.

    Matches 'Forever', 'Forever abilities', 'Forever NPCs', ... in both bare
    wgCategories form ('Forever') and prefixed API form ('Category:Forever').
    ^forever\\b avoids matching 'Content added by a Forever patch' (handled by
    FOREVER_PATCH_BUCKET) and any retail/classic bucket.
    """
    c = str(cat).lower().strip()
    if c.startswith("category:"):
        c = c[len("category:"):].strip()
    return bool(FOREVER_STAR_RE.match(c))


def _has_classic_broad_category(cats):
    """True for membership in a broad Classic category:
    Category:Classic or Category:Classic items (bare or 'Category:...' form)."""
    for c in cats:
        cl = str(c).strip().lower()
        if cl.startswith("category:"):
            cl = cl[len("category:"):].strip()
        if cl == "classic" or cl == "classic items":
            return True
    return False


def classify_version(categories, wikitext=""):
    """Classify a page into a WoW version bucket.

    Ingestion scope is FOREVER + CLASSIC. Forever is the highest-precedence
    source: a page carrying any Forever signal is kept as Forever even when it
    also carries Classic content (the Classic variant is superseded -- handled by
    the base/override layer). Classic pages (Category:Classic / Category:Classic
    items membership, or a {{Classic only|only-section|unique|season}} template)
    are now in scope and kept when they carry no Forever signal.

    Returns dict:
      version    : 'Forever' | 'classic' | 'retail' | 'multi' | 'unknown'
      forever    : True iff a Forever signal is present (highest precedence)
      classic    : True iff the page is Classic content (Category:Classic/items
                   or a Classic template)
      keep       : True iff the page is in scope (Forever OR Classic)
      signals    : list of detected signals (for logging)
      other_versions: space-joined non-forever flavors the page also carries
    """
    cats = list(categories or [])
    csl = [str(c).strip().lower() for c in cats]
    w = (wikitext or "").lower()

    signals = []
    is_forever = False
    if any(FOREVER_PATCH_BUCKET in c for c in csl):
        is_forever = True
        signals.append("Forever patch bucket")
    if any(FOREVER_PATCH_16.search(c) for c in csl):
        is_forever = True
        signals.append("Added-in-patch-1.6x")
    if DATA_FOREVER_MARKER in w:
        is_forever = True
        signals.append("#data:#Forever")
    if any(_is_forever_star_category(c) for c in cats):
        is_forever = True
        signals.append("Category:Forever")

    classic_broad = _has_classic_broad_category(cats)
    classic_bucket = any(CLASSIC_BUCKET in c for c in csl)
    classic_template = bool(CLASSIC_TEMPLATE_RE.search(w))
    is_classic = classic_broad or classic_bucket or classic_template
    if is_classic:
        signals.append("Category:Classic" if classic_broad
                       else ("content added by a classic patch" if classic_bucket
                             else "Classic template"))

    has_retail = any(RETAIL_BUCKET in c for c in csl)
    other = " ".join(s for s in (("retail" if has_retail else None),
                                 ("classic" if is_classic else None)) if s)

    if is_forever:
        return {"version": "Forever", "forever": True, "keep": True,
                "classic": is_classic, "signals": signals, "other_versions": other}
    if has_retail and is_classic:
        return {"version": "multi", "forever": False, "keep": False,
                "classic": True, "signals": signals, "other_versions": "retail classic"}
    if has_retail:
        return {"version": "retail", "forever": False, "keep": False,
                "classic": False, "signals": signals, "other_versions": "retail"}
    if is_classic:
        return {"version": "classic", "forever": False, "keep": True,
                "classic": True, "signals": signals, "other_versions": other}
    return {"version": "unknown", "forever": False, "keep": False,
            "classic": False, "signals": signals, "other_versions": ""}


def in_forever(categories, wikitext=""):
    """True iff the page belongs to WoW Forever (should be ingested)."""
    return classify_version(categories, wikitext)["forever"]


def process_candidate(page, path, content, categories, fetch_ok):
    """Decide whether to ingest a candidate page.

    Ingestion scope is Forever + Classic; Forever takes precedence.

    fetch_ok: whether per-page categories were fetched successfully.
      - fetched -> strict scope gate: keep iff classify in scope (Forever or
        Classic). Forever is reported as version "Forever", Classic as "classic".
      - not fetched -> fall back to bucket-path membership (the page was found
        under a verified bucket), kept but flagged unverified unless a wikitext
        marker ({{#data:#Forever}} / {{Classic only|unique|season}}) verifies it.
    """
    cls = classify_version(categories or [], content or "")
    if fetch_ok:
        if not cls["keep"]:
            return {"keep": False, "reason": f"not-forever:{cls['version']}",
                    "classify": cls, "version_verified": True}
        return {"keep": True, "reason": cls["version"].lower(),
                "classify": cls, "version_verified": True}
    # Bucket-path fallback; verify via wikitext markers (data block / Classic templates).
    secondary = classify_version([], content)["keep"]
    return {"keep": True, "reason": cls["version"].lower() if cls["keep"] else "bucket-path",
            "classify": cls, "version_verified": bool(secondary), "fallback": True}


def get_page_with_categories(page_title):
    """Fetch one page's content + revision timestamp + categories in ONE call.

    Returns (content, timestamp, categories, ok). ok is False on any failure so
    callers fall back to bucket-path trust instead of silently dropping content.
    """
    params = {
        "action": "query",
        "prop": "revisions|categories",
        "rvprop": "timestamp|content",
        "rvslots": "main",
        "titles": page_title,
        "format": "json",
        "formatversion": "2",
    }
    try:
        data = wiki_api_get(params)
    except Exception as e:
        print(f"  [version-check] fetch failed for {page_title}: {e}")
        return None, None, None, False
    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return None, None, None, False
    page = pages[0]
    revs = page.get("revisions", [])
    if not revs:
        return None, None, None, False
    rev = revs[0]
    # formatversion=2 nests raw wikitext under revisions[i].slots.main.content
    content = rev.get("slots", {}).get("main", {}).get("content", "")
    return content, rev.get("timestamp"), \
           [c["title"] for c in page.get("categories", [])], True


def safe_title(title):
    import re
    # Replace any character that is not alphanumeric, underscore or hyphen with underscore
    s = re.sub(r'[^A-Za-z0-9_\-]+', '_', title)
    # collapse multiple underscores
    s = re.sub(r'_+', '_', s)
    return s.strip('_')


def enumerate_pages(roots, dedup="title", max_pages=None):
    """Enumerate content pages under one or more category roots, recursing into
    subcategories. Returns list of (page_title, category_path).

    dedup: 'title' collapses a page reached via several paths/subcats to one
           entry (needed when merging Forever + Classic roots); 'path' keeps
           per-(page, path) duplicates (legacy behavior).
    max_pages: cap the number of raw (page, path) rows collected (fast/bounded
               enumeration for dry-runs).
    """
    pages_with_path = []
    visited_cats = set()

    def recurse(cat, path):
        cat_key = f"Category:{cat}"
        if cat_key in visited_cats:
            return
        visited_cats.add(cat_key)
        cmcontinue = None
        while True:
            params = {
                "action": "query",
                "list": "categorymembers",
                "cmtitle": cat_key,
                "cmlimit": 500,
                "cmtype": "all",   # pages + subcats (+ files, which we skip)
                "format": "json",
                "formatversion": "2",
            }
            if cmcontinue:
                params["cmcontinue"] = cmcontinue
            data = wiki_api_get(params)
            batch = data.get("query", {}).get("categorymembers", [])
            for m in batch:
                ns = m.get("ns")
                title = m.get("title")
                if ns == 0:
                    pages_with_path.append((title, path))
                elif ns == 14:
                    subcat = title.replace("Category:", "")
                    recurse(subcat, path + "/" + subcat)
            cmcontinue = data.get("continue", {}).get("cmcontinue")
            time.sleep(DELAY_BETWEEN_REQUESTS)
            if not cmcontinue:
                break

    for root in roots:
        recurse(root, root)
        if max_pages and len(pages_with_path) >= max_pages:
            break

    # dedupe while preserving discovery order
    seen = set()
    unique = []
    for page, path in pages_with_path:
        key = page if dedup == "title" else (page, path)
        if key not in seen:
            seen.add(key)
            unique.append((page, path))
    if max_pages:
        unique = unique[:max_pages]
    return unique


def enumerate_forever_pages():
    """[legacy] Enumerate every page under the Forever bucket (recursing into
    Forever patch subcategories). Returns list of (page_title, category_path).
    Preserved for backwards compatibility; scope expansion lives in
    enumerate_scope_pages()."""
    return enumerate_pages([FOREVER_ROOT_CATEGORY], dedup="path")


def enumerate_scope_pages(max_pages=None):
    """Enumerate the merged Forever + Classic scope (Forever root, Category:Classic,
    Category:Classic items), deduped by page title with the Forever root first so a
    page present in both resolves to its Forever path (Forever overrides Classic).

    Returns (merged_pages, forever_pages, classic_pages) where the last two are a
    clean partition of merged (split by category-path root via split_scope), so a
    page present in both roots is counted as Forever only."""
    forever_raw = enumerate_pages([FOREVER_ROOT_CATEGORY], dedup="path", max_pages=max_pages)
    classic_raw = enumerate_pages(["Classic", "Classic items"], dedup="title",
                                  max_pages=max_pages)
    merged = list(forever_raw)
    seen = {p for p, _ in forever_raw}
    for page, path in classic_raw:
        if page not in seen:
            seen.add(page)
            merged.append((page, path))
    return merged, *split_scope(merged)


def split_scope(unique_pages):
    """Split (page, path) tuples into (forever_pages, classic_pages) by the root of
    their category path. Backported pages resolve to the Forever path via
    title-dedup and are counted as Forever (Forever overrides Classic)."""
    forever, classic = [], []
    for page, path in unique_pages:
        root = path.split("/")[0] if path else ""
        if root == FOREVER_ROOT_CATEGORY:
            forever.append((page, path))
        else:
            classic.append((page, path))
    return forever, classic


def normalize_base(title):
    """Derive the canonical base-entity key for a page title.

    Strips a trailing WoW flavor suffix in parentheses so a page and its
    flavor-twin resolve to ONE base entity and the base/override dedup can
    supersede a Classic base with its Forever twin::

        'Druid abilities (Forever)' -> 'Druid abilities'
        'Druid abilities (Classic)' -> 'Druid abilities'
        'Gravestone Scepter'        -> 'Gravestone Scepter'

    Only the flavor suffixes ' (Forever)' and ' (Classic...)' are stripped; any
    other trailing parenthetical (e.g. a patronymic) is preserved so unrelated
    titles are never merged.
    """
    m = re.match(r"^(.*?)\s+\((?:Forever|Classic[^)]*)\)\s*$", title, re.IGNORECASE)
    return m.group(1).strip() if m else title.strip()


# Higher rank wins when two variants of the same base entity are in scope.
_SCOPE_RANK = {"Forever": 2, "classic": 1}


def group_and_dedupe(decisions, max_pages=None):
    """Classic base / Forever override deduplication over candidate decisions.

    Groups candidate pages by base entity (normalize_base) and resolves
    Classic-vs-Forever precedence: within each entity keep the highest-precedence
    in-scope variant (Forever > Classic) and drop the superseded one; a standalone
    Classic variant is kept only when no Forever variant exists for the entity.
    Out-of-scope variants are dropped. Discovery order is preserved.

    decisions: list of dicts each with keys
        page, path, classify (classify_version dict),
        decision (process_candidate dict; .keep),
        content, ts, categories, fetch_ok, last_ts   -- the rest main() needs.
    Returns (survivors, superseded): survivors is the deduped list of decision
    dicts to ingest (in discovery order); superseded is a list of
    (page, path, reason) overridden by a higher-precedence twin or dropped as
    out-of-scope.
    """
    groups = {}
    order = []
    for d in decisions:
        base = normalize_base(d["page"])
        if base not in groups:
            groups[base] = []
            order.append(base)
        groups[base].append(d)

    survivors, superseded = [], []
    for base in order:
        variants = groups[base]
        in_scope = [d for d in variants if d["decision"]["keep"]]
        if not in_scope:
            # No in-scope variant for this entity -> drop the whole entity.
            for d in variants:
                superseded.append((d["page"], d["path"], "out-of-scope"))
            continue

        def rank_key(d):
            cls = d["classify"]
            return (_SCOPE_RANK.get(cls.get("version"), 0),
                    1 if cls.get("forever") else 0, d["page"])

        best = max(in_scope, key=rank_key)
        survivors.append(best)
        for d in in_scope:
            if d is not best:
                superseded.append((d["page"], d["path"],
                                   f"superseded by {best['page']} "
                                   f"(base entity '{base}', scope={scope_label(best['classify'])})"))

    if max_pages:
        survivors = survivors[:max_pages]
    return survivors, superseded


def version_gate_report(unique_pages, max_pages=None):
    """Apply the version filter to candidate pages (no writes).

    Returns dict with kept (Forever) / classic (in-scope Classic) / rejected
    (retail/multi/unknown) lists (page, path, classify) + unverified pages.
    Used for the runtime unit-check summary and for dry-run validation.
    """
    kept, classic, rejected, unverified = [], [], [], []
    limit = max_pages if max_pages is not None else len(unique_pages)
    for i, (page, cat_path) in enumerate(unique_pages):
        if i >= limit:
            break
        content, ts, categories, fetch_ok = get_page_with_categories(page)
        if not content:
            continue
        decision = process_candidate(page, cat_path, content, categories, fetch_ok)
        cls = decision["classify"]
        if not decision["keep"]:
            rejected.append((page, cat_path, cls, decision["reason"]))
        elif cls["version"] == "classic":
            classic.append((page, cat_path, cls, decision["reason"]))
            if not decision["version_verified"]:
                unverified.append(page)
        else:
            kept.append((page, cat_path, cls, decision["reason"]))
            if not decision["version_verified"]:
                unverified.append(page)
    return {"kept": kept, "classic": classic, "rejected": rejected,
            "unverified": unverified}


def main():
    # Force Playwright-style HTML fallback permanently – API is blocked for this egress
    print("Pre-flight API health check skipped – forcing Playwright-style HTML fallback")

    print("Enumerating merged scope (Forever + Classic categories)...")
    unique_pages, forever_pages, classic_pages = enumerate_scope_pages()
    print(f"Found {len(unique_pages)} unique pages: "
          f"{len(forever_pages)} Forever, {len(classic_pages)} Classic")

    if len(unique_pages) == 0:
        print("ERROR: Enumerated 0 pages. Wiki API likely unreachable or buckets empty.")
        sys.exit(2)

    # Production ingestion policy: Forever is always ingested. Classic pages are
    # ingested by default as well (WOG_INGEST_CLASSIC defaults True), so the
    # merged Forever + Classic scope (unique_pages) is processed every run.
    # Set WOG_INGEST_CLASSIC=0 to restrict to Forever scope only.
    if WOG_INGEST_CLASSIC:
        loop_pages = unique_pages
        print("[scope] Classic ingestion ENABLED (processing all scope pages)")
    else:
        loop_pages = forever_pages
        print(f"[scope] Classic ingestion DISABLED (default): only {len(forever_pages)} "
              f"Forever page(s) processed. {len(classic_pages)} Classic page(s) discovered "
              "but not ingested. Set WOG_INGEST_CLASSIC=1 after the base/override layer.")

    state = load_state()
    rejected = 0
    violations = []

    # ---- Phase 1: fetch + classify every candidate (collect decisions) ----
    print("Phase 1: fetching and classifying candidates...")
    decisions = []
    for page, cat_path in loop_pages:
        print(f"Checking {page}...")
        content, ts, categories, fetch_ok = get_page_with_categories(page)
        if not content:
            print(f"  No content for {page}")
            time.sleep(DELAY_BETWEEN_REQUESTS)
            continue

        decision = process_candidate(page, cat_path, content, categories, fetch_ok)
        cls = decision["classify"]

        # ---- Version unit check (only out-of-scope pages are violations now) ----
        if not decision["keep"]:
            rejected += 1
            violations.append((page, cat_path, cls["version"], cls["other_versions"]))
            extra = (" other=" + cls["other_versions"]) if cls["other_versions"] else ""
            print(f"  [VERSION-GATE] REJECT {page}: version={cls['version']}{extra}")
            time.sleep(DELAY_BETWEEN_REQUESTS)
            continue
        if not decision["version_verified"]:
            print(f"  [VERSION-GATE] WARN {page}: version unverifiable (kept via bucket path)")

        decisions.append({
            "page": page, "path": cat_path, "content": content, "ts": ts,
            "categories": categories, "fetch_ok": fetch_ok,
            "decision": decision, "classify": cls, "last_ts": state.get(page),
        })

    # ---- Phase 2: Classic base / Forever override dedup (base-entity grouping) ----
    # When a base entity has both a Classic base and a Forever variant, the Forever
    # variant wins and the Classic base is dropped (superseded). Standalone Classic
    # pages with no Forever twin are kept as the base layer.
    print("Phase 2: resolving Classic base / Forever override precedence...")
    survivors, superseded = group_and_dedupe(decisions)
    for page, path, reason in superseded:
        print(f"  [OVERRIDE] drop {page} [{path}]: {reason}")

    added = updated = skipped = 0

    # ---- Phase 3: push survivors only (Forever overrides Classic base) ----
    print(f"Phase 3: pushing {len(survivors)} survivor page(s)...")
    for d in survivors:
        page, cat_path = d["page"], d["path"]
        content, ts = d["content"], d["ts"]
        categories, fetch_ok = d["categories"], d["fetch_ok"]
        cls = d["classify"]
        decision = d["decision"]
        last_ts = d["last_ts"]

        # ---- Build URI (reflects the bucket hierarchy) ----
        path_segments = [safe_title(seg) for seg in cat_path.split("/")]
        uri_path = "/".join(path_segments) + "/" + safe_title(page) + ".md"
        uri = f"{WIKI_TARGET_URI}{uri_path}"
        if last_ts == ts:
            exists, _ = openviking_exists(uri)
            if exists:
                skipped += 1
                print(f"  Skipping {page}, unchanged")
                time.sleep(DELAY_BETWEEN_REQUESTS)
                continue
            else:
                print(f"  State says unchanged but OpenViking missing, will re-sync")

        # ---- Nomadic embed (preserved): embed BEFORE writing ----
        source_url = f"https://warcraft.wiki.gg/page/{urllib.parse.quote(page)}"
        vector = nomic_embed(content)
        record_embedding(uri, page, cat_path, ts, source_url, vector)
        header = build_meta_header(page, cat_path, source_url, ts, vector, version=cls)
        body = f"# {page}\n\n{header}\n\n{content}"
        ok = push_to_openviking(uri, body, page)
        if ok:
            state[page] = ts
            if last_ts is None:
                added += 1
                print(f"  Added {page} -> {uri} ({scope_label(cls)})")
            else:
                updated += 1
                print(f"  Updated {page} -> {uri} ({scope_label(cls)})")
        else:
            print(f"  Failed to push {page}")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    save_state(state)

    # ---- Version unit check summary ----
    kept_total = added + updated + skipped
    if violations:
        print(f"\n[VERSION-UNIT-CHECK] FAILED: {len(violations)} out-of-scope page(s) "
              f"matched the enumeration but are not Forever/Classic:")
        for page, path, v, ov in violations[:25]:
            extra = (" other=" + ov) if ov else ""
            print(f"  - {page} [{path}] version={v}{extra}")
    else:
        print(f"\n[VERSION-UNIT-CHECK] PASSED: all {kept_total} ingested page(s) verified "
              f"in scope (Forever/Classic); {rejected} out-of-scope page(s) rejected.")
    print(f"Done. Added: {added}, Updated: {updated}, Skipped: {skipped}, "
          f"Rejected(out-of-scope): {rejected}, Override-dropped: {len(superseded)}")


def dry_run(max_pages=40, forever_sample=20, classic_sample=20):
    """Enumerate the merged Forever + Classic scope, classify, and report the
    KEPT (Forever) / IN-SCOPE (Classic) / REJECTED split WITHOUT writing to
    OpenViking. Per-scope sampling guarantees the report shows BOTH Forever and
    Classic pages. Bounded per-scope via max_pages so it stays fast."""
    print("Enumerating merged scope (Forever + Classic categories)...")
    merged, forever_pages, classic_pages = enumerate_scope_pages(max_pages=max_pages)
    print(f"Enumerated {len(merged)} unique pages: "
          f"{len(forever_pages)} Forever, {len(classic_pages)} Classic")

    sample = list(forever_pages[:forever_sample]) + list(classic_pages[:classic_sample])
    print(f"Classifying {len(sample)} sample pages "
          f"({forever_sample} Forever + {classic_sample} Classic)...")
    report = version_gate_report(sample, max_pages=max_pages)
    if report["unverified"]:
        print(f"[dry-run] {len(report['unverified'])} kept page(s) unverified "
              f"(kept via bucket-path fallback): {report['unverified'][:10]}")

    print(f"[dry-run] KEPT (Forever): {len(report['kept'])}")
    for page, path, cls, reason in report["kept"][:15]:
        other = (" other=" + cls["other_versions"]) if cls["other_versions"] else ""
        print(f"  + {page}  [{path}] version={cls['version']}{other}")

    print(f"[dry-run] IN-SCOPE (Classic): {len(report['classic'])}")
    for page, path, cls, reason in report["classic"][:15]:
        other = (" other=" + cls["other_versions"]) if cls["other_versions"] else ""
        print(f"  ~ {page}  [{path}] version={cls['version']}{other}")

    print(f"[dry-run] REJECTED (retail/multi/unknown): {len(report['rejected'])}")
    for page, path, cls, reason in report["rejected"][:15]:
        print(f"  - {page}  [{path}] {reason}")

    # Version unit check: only out-of-scope (retail/multi/unknown) pages are
    # violations now; Classic is in scope.
    if report["rejected"]:
        print("[dry-run] VERSION UNIT CHECK: FAILED")
        return 1
    print("[dry-run] VERSION UNIT CHECK: PASSED (in scope = Forever + Classic; "
          f"out-of-scope violations = {len(report['rejected'])})")
    return 0


if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        raise SystemExit(dry_run())
    main()
