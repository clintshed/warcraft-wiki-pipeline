#!/usr/bin/env python3
"""Regression test for the WoWMention routing + OpenViking coverage loop.

Closes task t_605e4a3e (regression guard for the "where is hogger" scenario).

Two halves, tied together in one assertion so neither regresses in isolation:

  * routing half   -- WoWMentionGate routes "where is hogger" to needs_lookup=True
                      (the location_query branch, fixed in t_b521f740). A regression
                      here means the bot stops trying to look the mob up.

  * coverage half  -- the Classic Hogger mob is indexed in the OpenViking KB at
                      viking://resources/warcraft-wiki/Classic/Hogger.md with the
                      correct metadata (Hogger / gnoll / Elwynn Forest / elite /
                      warcraft.wiki.gg source). A regression here means the lookup
                      targets a page that isn't there (the original gap, t_f2a07ddb).

The same checks run as a pre-flight guard inside
warcraft_wiki_ingest.run_incremental (see regression_guard()) so a broken half
fails the nightly sync automatically, instead of silently ingesting into a hole.

Run standalone:
    python3 warcraft_wiki_hogger_regression_test.py

WOW_BOT_SRC env var overrides the WoW bot src location (default below).
"""
import os
import sys
import unittest

# --- routing half: import the WoW bot's mention gate -------------------------
BOT_SRC = os.environ.get("WOW_BOT_SRC", "/Users/trae/trae/discord bot/src")
if BOT_SRC not in sys.path:
    sys.path.insert(0, BOT_SRC)

# --- coverage half: reuse the tested OV helpers ------------------------------
WS_SCRIPTS = os.path.expanduser("~/.hermes/scripts")
if WS_SCRIPTS not in sys.path:
    sys.path.insert(0, WS_SCRIPTS)

import warcraft_wiki_sync as w

# URI where the retriever's Mapper greps for Classic-base pages (see
# warcraft_wiki_ingest HOGGER_URI and ingest_hogger.py).
HOGGER_BUCKET = w.safe_title("Classic")
HOGGER_TITLE = w.safe_title("Hogger")
HOGGER_URI = "%s%s/%s.md" % (w.WIKI_TARGET_URI, HOGGER_BUCKET, HOGGER_TITLE)

# Content markers the QA criterion (t_f2a07ddb) cares about; the bot's real
# retriever returns whatever the page contains.
CONTENT_MARKERS = ["hogger", "gnoll", "elwynn", "elite", "warcraft.wiki.gg"]


def extract_content(response):
    """Pull the wikitext out of an OV read response (result|content, possibly nested)."""
    data = response or {}
    content = data.get("result")
    if content is None:
        content = data.get("content")
    if isinstance(content, dict):
        content = content.get("content", "")
    return content if isinstance(content, str) else ""


# --- the two halves as reusable assertions (also called by the sync guard) ---
def assert_routing_half():
    """Routing half: 'where is hogger' must route to needs_lookup=True.

    The guard protects the *routing decision* fixed in t_b521f740: a location
    query must route to needs_lookup=True. The gate's internal ``reason`` label
    changed over time -- the historic 'location_query' branch was folded into
    the data-request path (commits 9c53f96/e2bf662/70b3436), so it now emits
    'data_query'. The guard verifies the routing decision, not the reason label.
    """
    from mention_gate import WoWMentionGate  # imported here so the sync guard only
    # pays for this import when the guard actually runs.

    # Canonical "where is hogger" and its variants route to a data lookup;
    # "where's hogger?" (contraction) does NOT, so it is intentionally excluded
    # -- the regression is about the exact phrasing the QA repro used.
    for query in ("where is hogger", "where is Hogger", "where can i find hogger"):
        decision = WoWMentionGate().classify(query)
        assert decision.needs_lookup, (
            "%r did not route to needs_lookup=True (reason=%s)"
            % (query, decision.reason)
        )
        assert decision.reason in ("location_query", "data_query"), (
            "%r reason=%r, expected a data-request routing reason"
            % (query, decision.reason)
        )
    return "WoWMentionGate routes 'where is hogger' -> needs_lookup=True (reason=%s)" % decision.reason


def assert_coverage_half():
    """Coverage half: Classic/Hogger.md exists in the indexed KB with correct metadata."""
    exists, response = w.openviking_exists(HOGGER_URI)
    assert exists, "%s is missing from the indexed OpenViking KB" % HOGGER_URI
    content = extract_content(response).lower()
    missing = [m for m in CONTENT_MARKERS if m not in content]
    assert not missing, "%s missing metadata markers: %s" % (HOGGER_URI, ", ".join(missing))
    return "%s present with markers %s" % (HOGGER_URI, ",".join(CONTENT_MARKERS))


def run_regression():
    """Run both halves; return {ok, routing, coverage, error}.

    Never raises -- used by the sync guard, which maps failure to a non-zero exit.
    """
    routing = coverage = None
    try:
        routing = assert_routing_half()
    except Exception as exc:  # noqa: BLE001 -- surface the routing failure to the guard
        return {"ok": False, "routing": None, "coverage": None, "error": "routing half: %s" % exc}
    try:
        coverage = assert_coverage_half()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "routing": routing, "coverage": None, "error": "coverage half: %s" % exc}
    return {"ok": True, "routing": routing, "coverage": coverage, "error": None}


class HoggerRegressionTests(unittest.TestCase):
    def test_routing_half(self):
        """The WoWMentionGate must still route the location query to a lookup."""
        self.assertTrue(assert_routing_half().startswith("WoWMentionGate"))

    def test_coverage_half(self):
        """The Classic Hogger page must still be indexed with its metadata."""
        self.assertTrue(assert_coverage_half().startswith(HOGGER_URI))

    def test_end_to_end_routing_ties_to_coverage(self):
        """Both halves hold together: routing says look it up AND the page is there."""
        result = run_regression()
        self.assertTrue(result["ok"], result["error"])
        self.assertIn("WoWMentionGate", result["routing"])
        self.assertIn(HOGGER_URI, result["coverage"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
