#!/usr/bin/env python3
"""
P1 scope enumeration for World of Warcraft: Forever.

Builds the SCOPE = UNION of two overlapping "Forever" wiki hierarchies and writes:
  - forever-pages.txt   : one KEPT title per line (the --articleList scope)
  - forever-enum-report.json : full dry-run breakdown (kept / rejected / unverified /
                               backports w/ other_versions / cats visited per root)

SCOPE
  1. Category:Forever (ns14) + recurse all subcats            (primary root)
  2. Category:Content added by a Forever patch (ns14) + recurse
     (captures Category:Added in patch 1.6x -> 1.60.1 ...)
  Deduped by title across both roots.

CLASSIFICATION (reuse, do NOT re-derive)
  classify_version() is imported verbatim from ~/.hermes/scripts/warcraft_wiki_sync.py
  and provides the three keep signals:
    a) "forever patch" bucket   -> Category:Content added by a Forever patch
    b) "added in patch 1.6x"    -> Category:Added in patch 1.6x
    c) "#data:#forever" marker  -> {{#data:#Forever}} data block  (wikitext)
  NOTE: v1 fetches categories only (clean, reliable JSON through donsetch); the
  wikitext marker (c) is therefore NOT applied. That does not change the kept set:
  any page identifiable only by (c) is found under a verified Forever root and is kept
  via the bucket-path fallback below (never silently dropped).

  Decision per title:
    classify.forever == True         -> KEEP (record other_versions as a backport)
    version in {retail, classic}     -> REJECT (clearly non-Forever, logged by title)
    version in {unknown, multi}      -> KEEP via bucket-path, flag unverified
      (multi = retail + classic buckets AND a Forever signal; the signal wins)
  Fetch-failure resilience: a title whose real categories cannot be fetched is kept
  via bucket-path trust (it came from a verified Forever root) but flagged unverified.
  We never silently drop content on an API failure.

FETCH TRANSPORT (important)
  warcraft.wiki.gg sits behind a Cloudflare JS challenge that returns HTTP 403 to the
  Mac's direct egress (urllib / curl are WAF-blocked). Per spec section 6 that is the
  WAF-escalation scenario; Privado VPN is an optional per-run escalation, NOT wired in
  v1. Instead this script drives the SAME MediaWiki API through the DonSeTch CLI
  (`donsetch fetch`), which resolves the wall and returns the raw JSON on stdout. The
  URL contract and response schema are identical to wiki_api_get().

Rate-limit discipline: time.sleep between every fetch; donsetch also paces itself.
This script performs only read-only API queries -> it is inherently a dry run (no
API writes).
"""
import os
import sys
import time
import json
import subprocess
import argparse
from urllib.parse import urlencode

WS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.expanduser('~/.hermes/scripts'))
from warcraft_wiki_sync import classify_version  # reuse the 3 keep signals verbatim

# Category:Forever* subcategories (Forever abilities / NPCs / factions / quests /
# companions / ...). classify_version() does not recognize these, so a page carried
# only under such a bucket is mis-classified "unknown" even though it is genuinely
# Forever. We upgrade those to a verified Forever here (classify_version is kept
# verbatim -- this is a local enum refinement, not a change to the shared module).
FOREVER_BUCKET_SUBSTR = "category:forever"


def classify_forever_union(cats, wikitext=""):
    """classify_version + the Category:Forever* direct-marker signal.

    Returns the same dict shape as classify_version. Any page whose real categories
    include a Category:Forever<something> bucket is promoted to verified Forever so it
    is NOT needlessly kept as an "unverified" bucket-path entry.
    """
    cls = classify_version(cats, wikitext)
    if cls["forever"]:
        return cls
    low = [str(c).lower() for c in (cats or [])]
    if any(FOREVER_BUCKET_SUBSTR in c for c in low):
        return {
            "version": "Forever", "forever": True,
            "signals": list(cls["signals"]) + ["Category:Forever*"],
            "other_versions": cls["other_versions"],
        }
    return cls

API = "https://warcraft.wiki.gg/api.php"

# --- roots + tuning ------------------------------------------------------------
ROOTS = ["Forever", "Content added by a Forever patch"]
DELAY = float(os.getenv("ENUM_DELAY", "1.0"))                 # s between fetches
BATCH = int(os.getenv("ENUM_CLASSIFY_BATCH", "50"))           # report classify-batch group size (classification fetch is per-title)
MAX_PAGES = int(os.getenv("ENUM_MAX_PAGES", "0"))             # 0 = classify all
DONSETCH_MAX_CHARS = int(os.getenv("DONSETCH_MAX_CHARS", "500000"))


def build_url(params):
    return f"{API}?{urlencode(params, doseq=True)}"


def donsetch_fetch(url):
    """Fetch a URL via the DonSeTch CLI and return parsed JSON.

    The CLI resolves the Cloudflare challenge on the blocked direct egress. Raw JSON
    is emitted on stdout (status/meta goes to stderr), so we json.loads stdout.
    Raises RuntimeError on empty/non-JSON output so callers can bucket-path fallback.
    """
    proc = subprocess.run(
        ["donsetch", "fetch", "--max-chars", str(DONSETCH_MAX_CHARS), url],
        capture_output=True, text=True, timeout=300,
    )
    out = (proc.stdout or "").strip()
    if not out:
        raise RuntimeError(
            "donsetch empty stdout rc={} err={}".format(
                proc.returncode, (proc.stderr or "").strip()[:200]
            )
        )
    start = out.find("{")
    if start == -1:
        raise RuntimeError("donsetch no JSON in stdout: {!r}".format(out[:120]))
    # Prefer a strict parse; tolerate a stray trailing marker/meta after the JSON.
    try:
        return json.loads(out[start:].strip())
    except json.JSONDecodeError:
        pass
    try:
        obj, _end = json.JSONDecoder().raw_decode(out[start:])
        return obj
    except json.JSONDecodeError as ex:
        raise RuntimeError("donsetch non-JSON tail={!r} :: {}".format(
            out[start + 120:start + 220], ex))


MAX_RECURSE_DEPTH = int(os.getenv("ENUM_MAX_DEPTH", "50"))   # safety guard
MAX_PAGINATE_ROUNDS = int(os.getenv("ENUM_MAX_PAGINATE", "100000"))  # hard safety
CM_LIMIT = int(os.getenv("ENUM_CMLIMIT", "500"))             # MediaWiki cm page size


def recurse_tree(root):
    """Recursively gather every ns0 page under `root` and ALL of its subcats.

    Two invariants the operator requires:

    1. Recursion -- descends into every subcategory (ns==14) at ANY depth, so the
       whole Category:Forever* tree is crawled, not just one level. Guarded by
       MAX_RECURSE_DEPTH so a pathological cycle can never stack-overflow.

    2. Pagination -- every category request honors the cmcontinue token until the
       server stops returning one, so lists are never truncated at CM_LIMIT (500).
       Guarded by MAX_PAGINATE_ROUNDS against a stuck token.

    Returns ([(title, path)], visited-sets). ``pages_fetched`` counts total page
    refs pulled so the caller can see whether any bucket needed >1 page.
    """
    out = []
    visited = set()
    fetched = {"pages": 0, "buckets": 0}

    def recurse(cat, path, depth):
        cat_key = "Category:" + cat
        if cat_key in visited or depth > MAX_RECURSE_DEPTH:
            return
        visited.add(cat_key)
        cmcontinue = None
        rounds = 0
        while True:
            params = {
                "action": "query", "list": "categorymembers", "cmtitle": cat_key,
                "cmlimit": CM_LIMIT, "format": "json", "formatversion": "2",
            }
            if cmcontinue:
                # Resume exactly where the previous page left off; never re-fetch.
                params["cmcontinue"] = cmcontinue
            data = donsetch_fetch(build_url(params))
            members = data.get("query", {}).get("categorymembers", [])
            for m in members:
                fetched["pages"] += 1
                ns, title = m.get("ns"), m.get("title")
                if ns == 0:
                    out.append((title, path))
                elif ns == 14:
                    subcat = title.replace("Category:", "")
                    recurse(subcat, path + "/" + subcat, depth + 1)
            fetched["buckets"] += 1
            cmcontinue = data.get("continue", {}).get("cmcontinue")
            rounds += 1
            if not cmcontinue:
                break
            if rounds > MAX_PAGINATE_ROUNDS:
                raise RuntimeError("cmcontinue pagination exceeded safety cap")
            time.sleep(DELAY)

    recurse(root, root, 1)
    print(f"[union] {root}: fetched {fetched['pages']} page-refs across "
          f"{fetched['buckets']} buckets (depth<=MAX {MAX_RECURSE_DEPTH}, "
          f"cmcontinue honored)")
    return out, visited


def fetch_categories(titles):
    """Return {title: [category title, ...]} for the given titles.

    Fetches ONE title per request -- this is required for correctness.
    ``prop=categories`` under ``formatversion=2`` is unreliable for multi-title
    batches: it paginates the category enumeration via a ``clcontinue`` token and
    returns categories for only the first few requested pages, leaving the rest with
    empty arrays (the "922 unknown" artifact of the old BATCH=50 reader). Honoring
    ``clcontinue`` on a large batch paginates into *unrelated* pages and does not
    complete the batch. A single-title request always returns that page's full
    category set (verified: every ability/page the old batcher dropped is recovered,
    e.g. all re-dropped titles carry Category:Forever abilities).

    A title whose categories are empty/missing on fetch is recorded as [] (not None):
    the caller bucket-path-keeps it as unverified -- never dropped.
    """
    res = {}
    for t in titles:
        params = {
            "action": "query", "prop": "revisions|categories",
            "rvprop": "timestamp", "rvslots": "main",
            "titles": t, "format": "json", "formatversion": "2",
        }
        try:
            data = donsetch_fetch(build_url(params))
        except Exception:
            res[t] = []                      # fetch failure -> bucket-path fallback
            time.sleep(DELAY)
            continue
        cats = []
        for p in data.get("query", {}).get("pages", []):
            if p.get("title") == t:
                cats = [c["title"] for c in p.get("categories", [])]
        res[t] = cats
        time.sleep(DELAY)
    return res


def main():
    ap = argparse.ArgumentParser(description="Forever scope enumeration (dry run).")
    ap.add_argument("--max", dest="max_pages", type=int, default=MAX_PAGES,
                    help="classify at most this many union titles (0 = all)")
    args = ap.parse_args()
    cap = args.max_pages   # 0 => classify all

    # --- Phase A: union enumeration ---
    union = []          # ordered unique titles
    seen = set()
    per_root_counts = {}
    for root in ROOTS:
        pages, visited = recurse_tree(root)
        per_root_counts[root] = len(visited)   # categories visited
        for t, _p in pages:
            if t not in seen:
                seen.add(t)
                union.append(t)
        print("[enum] root '{}': {} ns0 pages, {} categories visited".format(
            root, len(pages), per_root_counts[root]), flush=True)
        time.sleep(DELAY)

    print("[enum] UNION deduped: {} unique titles".format(len(union)), flush=True)

    # --- zero-page guard: refuse to ship a stale/empty list ---
    if not union:
        err = os.path.join(WS, "enum-error.log")
        with open(err, "w") as f:
            f.write("ENUMERATION FAILED: 0 pages enumerated (enumeration API down "
                    "or unreachable). Refusing to write forever-pages.txt.\n")
        print("[enum] FATAL: 0 union pages -> aborting, wrote", err, flush=True)
        return 1

    # --- Phase B: classify gate (batched) ---
    cat_map = fetch_categories(union)         # title -> [cats] or None
    if all(v is None for v in cat_map.values()):
        err = os.path.join(WS, "enum-error.log")
        with open(err, "w") as f:
            f.write("ENUMERATION FAILED: category fetch failed for ALL {} titles "
                    "(enumeration API down). Refusing to ship a list.\n".format(
                        len(union)))
        print("[enum] FATAL: category fetch failed for all titles -> aborting, "
              "wrote", err, flush=True)
        return 1
    kept, rejected, unverified, backports = [], [], [], []
    # Project sense of "backport": a page kept because it carries a "Forever patch"
    # signal (Category:Content added by a Forever patch / Category:Added in patch 1.6x)
    # -> content added to Forever by a patch. Recorded separately from the strict
    # other_versions cross-flavor backports above so the report always shows the
    # patch-bucket backports that the operator's scope cares about.
    backports_patch = []

    for i, title in enumerate(union):
        if cap and i >= cap:   # cap=0 => classify all
            break
        cats = cat_map.get(title)
        if cats is None:
            # fetch failure -> bucket-path trust, flagged unverified; never drop
            unverified.append((title, "fetch-failed"))
            kept.append((title, None, True))
            continue
        cls = classify_forever_union(cats, "")
        other = cls.get("other_versions", "")
        ver = cls["version"]
        if cls["forever"]:
            kept.append((title, cls, False))
            if other:
                backports.append((title, other))
            backports_patch.append((title, " ".join(cls["signals"])))
        elif ver in ("retail", "classic"):
            # genuinely non-Forever (retail-only or classic-only buckets)
            rejected.append((title, cls))
        else:
            # unknown or multi: version indeterminate, but it was found under a
            # verified Forever root -> keep via bucket-path, flag unverified.
            unverified.append((title, "bucket-path:" + ver))
            kept.append((title, cls, True))
        print("  [{}] {} -> {} signals={} other={} cats={}".format(
            i, title, cls["version"], cls["signals"], other, cats[:6]), flush=True)

    kept_titles = [t for t, _, _ in kept]

    # --- outputs ---
    out_list = os.path.join(WS, "forever-pages.txt")
    with open(out_list, "w") as f:
        f.write("\n".join(kept_titles) + "\n")

    report = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "roots": ROOTS,
        "cats_visited_per_root": per_root_counts,
        "union_enumerated": len(union),
        "classified": len(union),          # distinct union pages processed
        "verified_forever": len(kept) - len(unverified),
        "kept_forever": len(kept),          # = verified + bucket-path (full scope)
        "rejected_non_forever": len(rejected),
        "unverified_bucket_path": len(unverified),
        "backports_with_other_versions": len(backports),
        "backports_patch_bucket": len(backports_patch),
        "kept_titles": kept_titles,
        "rejected": [{"title": t, "version": c["version"],
                      "other_versions": c.get("other_versions", "")}
                     for t, c in rejected],
        "unverified": [{"title": t, "reason": r} for t, r in unverified],
        "backports": [{"title": t, "other_versions": o} for t, o in backports],
        "backports_patch_bucket_list": [{"title": t, "signals": s}
                                        for t, s in backports_patch],
    }
    out_report = os.path.join(WS, "forever-enum-report.json")
    with open(out_report, "w") as f:
        json.dump(report, f, indent=2)

    # --- summary ---
    print("\n================ P1 ENUM REPORT (dry run) ================")
    print("union_enumerated         :", report["union_enumerated"])
    print("kept (Forever)           :", report["kept_forever"])
    print("rejected (non-Forever)   :", report["rejected_non_forever"])
    print("unverified (bucket-path) :", report["unverified_bucket_path"])
    print("backports (other_versions):", report["backports_with_other_versions"])
    print("backports (patch-bucket) :", report["backports_patch_bucket"],
          "[PRESENT]" if report["backports_patch_bucket"] > 0 else "[EMPTY]")
    print("0 non-Forever in list    :",
          "YES" if report["rejected_non_forever"] == 0 else "NO")
    print("output                   :", out_list, "({} lines)".format(len(kept_titles)))
    print("report                   :", out_report)
    if report["backports"]:
        t, o = report["backports"][0]
        print("backport sample          :", t, "->", o)
    print("==========================================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
